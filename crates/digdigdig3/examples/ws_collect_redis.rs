//! # ws_collect_redis — collect ticker / aggTrade / orderbook / 1m-kline from
//! Binance, OKX and Bitget into a local Redis (6379).
//!
//! Demonstrates the batch-subscribe path added for the 1000-symbol use case:
//! each exchange subscribes 10 symbols × 4 streams = **40 specs in one
//! `subscribe_batch` call**, which packs into a handful of wire frames
//! (Binance/OKX/Bitget each pack the whole batch into 1 frame).
//!
//! Every incoming WS event is serialized to JSON and appended to a Redis
//! stream (XADD) keyed as:
//!
//! ```text
//! dig3:{exchange}:{stream}:{symbol}
//! ```
//!
//! e.g. `dig3:binance:ticker:BTCUSDT`, `dig3:okx:aggTrade:BTC-USDT`,
//! `dig3:bitget:kline1m:BTCUSDT`. Streams are trimmed (`MAXLEN ~ 5000`) so the
//! buffer stays bounded.
//!
//! # Run
//!
//! Requires a local Redis on `127.0.0.1:6379` (default `redis://127.0.0.1`):
//!
//! ```text
//! cargo run --example ws_collect_redis --release            # default 60s
//! cargo run --example ws_collect_redis --release -- 30     # custom seconds
//! cargo run --example ws_collect_redis --release -- 30 127.0.0.1 6380
//! ```
//!
//! # Inspect
//!
//! ```text
//! redis-cli XRANGE dig3:binance:ticker:BTCUSDT - +
//! redis-cli XLEN dig3:binance:ticker:BTCUSDT
//! redis-cli --scan --pattern 'dig3:*'
//! ```
//!
//! No API keys required — public market data only.

use std::time::Duration;

use digdigdig3::connector_manager::ExchangeHub;
use digdigdig3::core::types::StreamEvent;
use digdigdig3::core::types::{AccountType, ExchangeId, StreamType, SubscriptionRequest, Symbol};
use futures_util::StreamExt;

/// 10 symbols per exchange; the per-venue native format is derived in
/// [`build_requests`] (OKX uses `BTC-USDT`, Binance/Bitget use `BTCUSDT`).
const SYMBOLS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    "LINKUSDT", "POLUSDT",
];

/// Four stream kinds per symbol → 40 subscription requests per exchange.
fn build_requests(exchange: ExchangeId) -> Vec<SubscriptionRequest> {
    let mut reqs = Vec::with_capacity(SYMBOLS.len() * 4);
    for sym in SYMBOLS {
        let native = match exchange {
            ExchangeId::OKX => okx_native(sym),
            _ => sym.to_string(),
        };
        let symbol = Symbol::with_raw("", "", native);
        reqs.push(SubscriptionRequest::ticker(symbol.clone()));
        reqs.push(SubscriptionRequest::new(symbol.clone(), StreamType::AggTrade));
        reqs.push(SubscriptionRequest::orderbook(symbol.clone()));
        reqs.push(SubscriptionRequest::kline(symbol, "1m"));
    }
    reqs
}

/// "BTCUSDT" → "BTC-USDT"
fn okx_native(s: &str) -> String {
    let idx = s.find("USDT").expect("symbol ends with USDT");
    format!("{}-{}", &s[..idx], &s[idx..])
}

/// Route a StreamEvent to its Redis key + JSON payload.
fn event_to_redis<'a>(
    exchange: &str,
    event: StreamEvent,
) -> Option<(String, String)> {
    use digdigdig3::core::types::StreamEvent::*;
    match event {
        Ticker { symbol, ticker } => Some((
            format!("dig3:{exchange}:ticker:{symbol}"),
            serde_json::to_string(&ticker).ok()?,
        )),
        AggTrade { symbol, agg } => Some((
            format!("dig3:{exchange}:aggTrade:{symbol}"),
            serde_json::to_string(&agg).ok()?,
        )),
        OrderbookSnapshot { symbol, book } => Some((
            format!("dig3:{exchange}:orderbook:{symbol}"),
            serde_json::to_string(&book).ok()?,
        )),
        // OKX books5 pushes only book *deltas* (no `action` field → no snapshot
        // frames) — record them under their own key so the data isn't lost.
        // A consumer rebuilds the L2 book by applying deltas to a cached side.
        OrderbookDelta { symbol, delta } => Some((
            format!("dig3:{exchange}:orderbookDelta:{symbol}"),
            serde_json::to_string(&delta).ok()?,
        )),
        Kline { symbol, interval, kline } => {
            let stream = format!("kline{}", interval.as_str().to_lowercase());
            Some((
                format!("dig3:{exchange}:{stream}:{symbol}"),
                serde_json::to_string(&kline).ok()?,
            ))
        }
        // Only the four streams above were subscribed; ignore everything else.
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_ansi(false).try_init().ok();

    // CLI: duration seconds (default 60), then optional redis host/port.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let duration = args
        .first()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    let redis_url = match (args.get(1), args.get(2)) {
        (Some(host), Some(port)) => format!("redis://{host}:{port}"),
        (Some(host), None) => format!("redis://{host}"),
        _ => "redis://127.0.0.1:6379".to_string(),
    };

    println!("═══ ws_collect_redis ═══");
    println!("  duration : {duration}s");
    println!("  redis    : {redis_url}");

    // ── 1. Redis connection ───────────────────────────────────────────────
    let redis_client = redis::Client::open(redis_url.as_str())?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;

    // ── 2. ExchangeHub + WS connect for the three venues ──────────────────
    let hub = ExchangeHub::new();
    let mut handles = Vec::new();

    for (exchange, label) in [
        (ExchangeId::Binance, "binance"),
        (ExchangeId::OKX, "okx"),
        (ExchangeId::Bitget, "bitget"),
    ] {
        hub.connect_websocket(exchange, AccountType::Spot, false).await?;
        let ws = hub
            .ws(exchange, AccountType::Spot)
            .expect("ws connector present for Spot");
        ws.connect(AccountType::Spot).await?;
        println!("  + {label} WS connected");

        // ── 3. Batch-subscribe 40 specs (10 symbols × 4 streams) ──────────
        let requests = build_requests(exchange);
        let ack = ws.subscribe_batch(requests).await?;
        println!(
            "  + {label}: batch subscribe {}/{} accepted, {} dropped",
            ack.submitted.len(),
            ack.submitted.len() + ack.dropped.len(),
            ack.dropped.len(),
        );
        for (req, err) in ack.dropped {
            println!("    ! {label} dropped: stream_type={:?} err={err}", req.stream_type);
        }

        // ── 4. Per-exchange collector task ────────────────────────────────
        let mut events = ws.event_stream();
        let label_owned = label.to_string();
        let redis_conn_owned = redis_conn.clone();
        handles.push(tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(duration);
            let mut count: u64 = 0;
            while tokio::time::Instant::now() < deadline {
                let Some(ev_res) = events.next().await else {
                    break;
                };
                let Ok(event) = ev_res else { continue };
                let Some((key, payload)) = event_to_redis(&label_owned, event) else {
                    continue;
                };
                // XADD key * data <json>
                let mut conn = redis_conn_owned.clone();
                let _: redis::RedisResult<Option<String>> = redis::cmd("XADD")
                    .arg(&key).arg("*").arg("data").arg(&payload)
                    .query_async(&mut conn).await;
                let _: redis::RedisResult<()> = redis::cmd("XTRIM")
                    .arg(&key).arg("MAXLEN").arg("~").arg(5000)
                    .query_async(&mut conn).await;
                count += 1;
            }
            println!("  = {label_owned}: {count} events written to redis");
        }));
    }

    // ── 5. Wait for collectors ───────────────────────────────────────────
    for h in handles {
        h.await?;
    }

    println!("\n═══ done — check with: redis-cli XRANGE dig3:binance:ticker:BTCUSDT - + ═══");
    Ok(())
}