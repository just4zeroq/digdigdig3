//! Batch subscribe/unsubscribe — grammar table + shared frame-build helpers.
//!
//! ## Design (locked in grill-me session)
//!
//! The transport sends frames through a shared outbound queue (paced by a
//! per-venue token bucket). A "batch" is a list of `StreamSpec`s folded into
//! one or more wire frames using a per-venue [`BatchGrammar`].
//!
//! The pump ([`build_packed`]) is venue-agnostic: it collects array elements,
//! chunks by `chunk_cap`, and flushes a frame whenever the chunk resets. All
//! per-venue facts live in the grammar the venue returns from
//! `batch_grammar()`:
//!
//! - `topic_fn` — builds the array element for one spec (topic string or
//!   object). `Err` = whole-message kind (OKX liquidation / block-trades,
//!   Bitget Liquidation wire-absent) → falls back to the per-spec frame;
//!   only if that also fails is the spec dropped.
//! - `envelope` — a fn-pointer that turns `(op, group, elements)` into the
//!   finished wire frame. The venue owns its envelope shape entirely (method
//!   key, array key, `id`/`time` presence, method-case); the core never
//!   special-cases a venue. A fifth exchange needs zero changes to this file.
//! - `group_key` — optional. Specs sharing one frame must also share the same
//!   group. Gate.io uses it for the per-kind `channel` (`spot.trades`); most
//!   venues return `None` (everything in one bucket).
//! - `chunk_cap` — max array elements per wire frame.
//!
//! Venues whose subscribe frame is NOT an array envelope (MEXC Futures
//! single-object `param`, dYdX/Lighter/Coinbase/...) return `None` from
//! `batch_grammar()` → [`build_looped`], the per-spec loop (unchanged legacy).

use serde_json::{json, Value};

use crate::core::rt::WsFrame;
use crate::core::types::WebSocketError;

use super::{
    protocol::WsProtocol,
    stream_spec::StreamSpec,
};

/// Build one packed wire frame from a chunk's elements.
///
/// - `op` — the subscribe/unsubscribe operation the frame carries
///   (`"SUBSCRIBE"`, `"subscribe"`, `"SUBSCRIPTION"`, … — venue decides).
/// - `group` — the current frame's group ([`BatchGrammar::group_key`]);
///   `None` when the grammar has no group.
/// - `elements` — the collected `topic_fn` results (never empty for a flshed
///   chunk; the pump skips empty chunks).
///
/// Returns the finished outbound frame. `'static` fn-pointer so a venue can
/// return its grammar from a static table.
pub type EnvelopeFn = fn(op: &str, group: Option<&str>, elements: &[Value]) -> WsFrame;

// ── Standard envelope constructors ──────────────────────────────────────────
// Shared JSON shapes for the common venue families. Each venue's
// `batch_grammar()` picks one (or writes its own) — the core pump never
// special-cases a venue. The `op` parameter is ALWAYS the session-neutral
// `"subscribe"` / `"unsubscribe"` (lowercase) that `build_packed` passes;
// the envelope maps it to the venue-native method case where required.

/// `{"method":<op uppercased>,"params":[...],"id":1}` — venues whose
/// subscribe verb is the op uppercased (Binance: `SUBSCRIBE`/`UNSUBSCRIBE`).
pub fn envelope_params_upper(op: &str, _group: Option<&str>, elements: &[Value]) -> WsFrame {
    WsFrame::Text(
        json!({
            "method": op.to_uppercase(),
            "params": elements,
            "id": 1u64,
        })
        .to_string(),
    )
}

/// `{"id":1,"method":"SUBSCRIPTION","params":[...]}` — MEXC Spot (the op verb
/// maps to `SUBSCRIPTION` / `UNSUBSCRIPTION`, not `SUBSCRIBE` / `UNSUBSCRIBE`).
pub fn envelope_subscription(op: &str, _group: Option<&str>, elements: &[Value]) -> WsFrame {
    let method = match op {
        "subscribe" => "SUBSCRIPTION",
        _ => "UNSUBSCRIPTION",
    };
    WsFrame::Text(
        json!({
            "id": 1u64,
            "method": method,
            "params": elements,
        })
        .to_string(),
    )
}

/// `{"op":op,"args":[...]}` — Bybit / OKX / Bitget (op verb is already
/// lowercase `"subscribe"` / `"unsubscribe"`, used verbatim).
pub fn envelope_args(op: &str, _group: Option<&str>, elements: &[Value]) -> WsFrame {
    WsFrame::Text(json!({ "op": op, "args": elements }).to_string())
}

/// `{"time":<secs>,"channel":<group>,"event":op,"payload":[...]}` — Gate.io.
/// The per-kind wire `channel` comes from the grammar's `group_key`
/// (`spot.tickers`...); `op` is the lowercase event verb.
pub fn envelope_gate(op: &str, group: Option<&str>, elements: &[Value]) -> WsFrame {
    WsFrame::Text(
        json!({
            "time": crate::core::utils::timestamp_seconds(),
            "channel": group.unwrap_or(""),
            "event": op,
            "payload": elements,
        })
        .to_string(),
    )
}

/// Static per-venue batch grammar. `'static` + `Copy` so a protocol can return
/// a `&'static BatchGrammar` from a table without allocating.
#[derive(Debug, Clone, Copy)]
pub struct BatchGrammar {
    /// Build the JSON array element for one spec.
    ///
    /// Returns `Err` when this kind cannot be packed — e.g. OKX liquidation /
    /// block-trade / settlement are whole-message channels, or a kind is
    /// wire-absent (Bitget Liquidation). The caller then falls back to the
    /// per-spec `subscribe_frame`.
    pub topic_fn: fn(&StreamSpec) -> Result<Value, WebSocketError>,
    /// Turn `(op, group, elements)` into the finished wire frame. All
    /// envelope shape (method key, array key, `id`/`time`, method-case) lives
    /// here — the core pump is venue-agnostic.
    pub envelope: EnvelopeFn,
    /// Max array elements per wire frame. The packer chunks the spec list
    /// into multiple frames of at most this many elements. `usize::MAX` =
    /// no cap.
    pub chunk_cap: usize,
    /// Optional per-spec group. When set, all specs sharing one frame must
    /// share the same group — the pump flushes whenever it changes. Gate.io
    /// uses it for its per-kind `channel` (`spot.tickers` / `futures.order_book`).
    pub group_key: Option<fn(&StreamSpec) -> String>,
}

/// Result of building a batch — the frames to send, plus which specs were
/// accepted vs dropped at build time.
#[derive(Debug, Clone)]
pub struct BatchBuild {
    /// Wire frames. Empty is a contract violation (a non-empty input with
    /// zero frames built) — callers must treat it as a protocol bug.
    pub frames: Vec<WsFrame>,
    /// Specs whose frames were built (every element of every frame,
    /// in order). These get inserted into `active_subs`.
    pub submitted: Vec<StreamSpec>,
    /// Specs that failed frame construction. `topic_fn` failed AND the
    /// per-spec `subscribe_frame` fallback also failed.
    pub dropped: Vec<(StreamSpec, WebSocketError)>,
}

/// Fold a non-empty spec slice through a protocol, using `grammar` if the
/// spec's kinds are packable and the trailing chunk-size cap permits it.
///
/// Shared helper used by `WsProtocol::subscribe_frame_batch` /
/// `unsubscribe_frame_batch` defaults.
pub fn build_packed<P: WsProtocol + ?Sized>(
    protocol: &P,
    grammar: &BatchGrammar,
    op: &str,
    is_subscribe: bool,
    specs: &[StreamSpec],
) -> Result<BatchBuild, WebSocketError> {
    if specs.is_empty() {
        return Ok(BatchBuild {
            frames: Vec::new(),
            submitted: Vec::new(),
            dropped: Vec::new(),
        });
    }

    let mut frames: Vec<WsFrame> = Vec::new();
    let mut submitted: Vec<StreamSpec> = Vec::new();
    let mut dropped: Vec<(StreamSpec, WebSocketError)> = Vec::new();

    // Current group for the frame being folded (from `grammar.group_key`).
    let mut group_cur: Option<String> = None;

    let mut elements: Vec<Value> = Vec::new();
    let mut frame_specs: Vec<StreamSpec> = Vec::new();
    let flush = |frames: &mut Vec<WsFrame>,
                 elements: &mut Vec<Value>,
                 frame_specs: &mut Vec<StreamSpec>,
                 submitted: &mut Vec<StreamSpec>,
                 group_cur: &mut Option<String>| {
        if elements.is_empty() {
            return;
        }
        let frame = (grammar.envelope)(op, group_cur.as_deref(), elements);
        frames.push(frame);
        submitted.extend(frame_specs.drain(..));
        elements.clear();
    };

    for spec in specs {
        // Group gate: if the new spec's group differs from the current frame's,
        // flush first (Gate.io channel changes force a new frame).
        if let Some(gk) = grammar.group_key {
            let g = gk(spec);
            if let Some(cur) = &group_cur {
                if *cur != g {
                    flush(
                        &mut frames,
                        &mut elements,
                        &mut frame_specs,
                        &mut submitted,
                        &mut group_cur,
                    );
                }
            }
            group_cur = Some(g);
        }

        match (grammar.topic_fn)(spec) {
            Ok(element) => {
                if elements.len() + 1 > grammar.chunk_cap {
                    flush(
                        &mut frames,
                        &mut elements,
                        &mut frame_specs,
                        &mut submitted,
                        &mut group_cur,
                    );
                }
                elements.push(element);
                frame_specs.push(spec.clone());
            }
            Err(_) => {
                // Whole-message kind — fall back to per-spec frame (the packed
                // frame for this spec's neighbors is flushed so ordering is
                // preserved).
                flush(
                    &mut frames,
                    &mut elements,
                    &mut frame_specs,
                    &mut submitted,
                    &mut group_cur,
                );
                match if is_subscribe {
                    protocol.subscribe_frame(spec)
                } else {
                    protocol.unsubscribe_frame(spec)
                } {
                    Ok(f) => {
                        frames.push(f);
                        submitted.push(spec.clone());
                    }
                    Err(e) => dropped.push((spec.clone(), e)),
                }
            }
        }
    }
    flush(
        &mut frames,
        &mut elements,
        &mut frame_specs,
        &mut submitted,
        &mut group_cur,
    );

    Ok(BatchBuild { frames, submitted, dropped })
}

/// The default fallback: build one frame per spec by calling
/// `subscribe_frame`/`unsubscribe_frame` (legacy behavior — unchanged).
pub fn build_looped<P: WsProtocol + ?Sized>(
    protocol: &P,
    specs: &[StreamSpec],
    is_subscribe: bool,
) -> Result<BatchBuild, WebSocketError> {
    if specs.is_empty() {
        return Ok(BatchBuild {
            frames: Vec::new(),
            submitted: Vec::new(),
            dropped: Vec::new(),
        });
    }

    let mut frames: Vec<WsFrame> = Vec::new();
    let mut submitted: Vec<StreamSpec> = Vec::new();
    let mut dropped: Vec<(StreamSpec, WebSocketError)> = Vec::new();

    for spec in specs {
        let r = if is_subscribe {
            protocol.subscribe_frame(spec)
        } else {
            protocol.unsubscribe_frame(spec)
        };
        match r {
            Ok(f) => {
                frames.push(f);
                submitted.push(spec.clone());
            }
            Err(e) => dropped.push((spec.clone(), e)),
        }
    }

    Ok(BatchBuild { frames, submitted, dropped })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rt::WsFrame;
    use crate::core::types::{AccountType, OwnedSymbolInput, WebSocketError};
    use crate::core::websocket::protocol::WsProtocol;
    use crate::core::websocket::stream_kind::StreamKind;
    use crate::core::websocket::stream_spec::StreamSpec;
    use crate::core::websocket::topic_registry::{TopicKey, TopicRegistry, TopicRegistryBuilder};
    use serde_json::json;
    use url::Url;

    // ── Mock grammar + protocol for packing tests ─────────────────────────

    fn topic_as_str(spec: &StreamSpec) -> Result<Value, WebSocketError> {
        Ok(Value::String(format!("topic.{}", spec.symbol.as_str())))
    }

    fn topic_err(_spec: &StreamSpec) -> Result<Value, WebSocketError> {
        Err(WebSocketError::NotImplemented("whole-message".into()))
    }

    #[test]
    fn no_grammar_loops() {
        let mock = MockProtocol::new();
        let spec = mock.spec("A");
        let batch = mock.subscribe_frame_batch(&[spec]).unwrap();
        assert_eq!(batch.submitted.len(), 1);
        assert_eq!(batch.frames.len(), 1);
    }

    #[test]
    fn pack_chunks_by_cap() {
        let grammar = BatchGrammar {
            topic_fn: topic_as_str,
            envelope: envelope_params_upper,
            chunk_cap: 2,
            group_key: None,
        };
        let mock = MockProtocol::new();
        // `op` is the session-neutral lowercase verb the pump passes; the
        // envelope maps it to the venue-native uppercase method.
        let batch = build_packed(&mock, &grammar, "subscribe", true, &mock.specs(3)).unwrap();
        // 3 specs, cap 2 → 2 wire frames, all 3 submitted
        assert_eq!(batch.frames.len(), 2);
        assert_eq!(batch.submitted.len(), 3);
        assert!(batch.dropped.is_empty());
        // First frame carries 2 params
        let first: Value = extract_json(&batch.frames[0]);
        assert_eq!(first["method"], "SUBSCRIBE");
        assert_eq!(first["params"].as_array().unwrap().len(), 2);
    }

    /// The pump passes `"subscribe"`/`"unsubscribe"`; venue envelopes map to
    /// native method case. These tests pin that mapping so no venue silently
    /// emits a lowercase method that the exchange rejects.
    #[test]
    fn envelope_maps_op_case_to_venue() {
        let els = [Value::String("btcusdt@ticker".into())];

        // Binance: "subscribe" → "SUBSCRIBE"
        let f = envelope_params_upper("subscribe", None, &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["method"], "SUBSCRIBE");
        let f = envelope_params_upper("unsubscribe", None, &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["method"], "UNSUBSCRIBE");

        // MEXC Spot: "subscribe" → "SUBSCRIPTION"
        let f = envelope_subscription("subscribe", None, &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["method"], "SUBSCRIPTION");
        let f = envelope_subscription("unsubscribe", None, &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["method"], "UNSUBSCRIPTION");

        // Bybit/OKX/Bitget: op verbatim, wrapped under "op".
        let f = envelope_args("subscribe", None, &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"].as_array().unwrap(), &els);

        // Gate: op verbatim under "event"; group becomes "channel".
        let f = envelope_gate("subscribe", Some("spot.tickers"), &els);
        let v: Value = extract_json(&f);
        assert_eq!(v["event"], "subscribe");
        assert_eq!(v["channel"], "spot.tickers");
        assert_eq!(v["payload"].as_array().unwrap(), &els);
    }

    #[test]
    fn fallback_on_topic_err() {
        // topic_err for every spec → all fall back per-spec.
        let grammar = BatchGrammar {
            topic_fn: topic_err,
            envelope: envelope_args,
            chunk_cap: usize::MAX,
            group_key: None,
        };
        let mock = MockProtocol::new();
        let batch = build_packed(&mock, &grammar, "subscribe", true, &mock.specs(3)).unwrap();
        assert_eq!(batch.frames.len(), 3);
        assert_eq!(batch.submitted.len(), 3);
    }

    #[test]
    fn drop_when_both_fail() {
        let grammar = BatchGrammar {
            topic_fn: topic_err,
            envelope: envelope_params_upper,
            chunk_cap: usize::MAX,
            group_key: None,
        };
        let mock = MockProtocol::new();
        let specs = [mock.spec("A"), mock.spec("FAIL")];
        let batch = build_packed(&mock, &grammar, "SUBSCRIBE", true, &specs).unwrap();
        assert_eq!(batch.frames.len(), 1); // A via fallback
        assert_eq!(batch.submitted.len(), 1);
        assert_eq!(batch.dropped.len(), 1); // FAIL
        assert_eq!(batch.dropped[0].0.symbol.as_str(), "FAIL");
    }

    #[test]
    fn group_key_flushes_on_change() {
        // Two groups of a different channel → one frame each (Gate.io).
        let grammar = BatchGrammar {
            topic_fn: topic_as_str, // payload element string
            envelope: envelope_gate,
            chunk_cap: 10,
            group_key: Some(|s| {
                if s.symbol.as_str().starts_with("A") {
                    "spot.tickers".to_string()
                } else {
                    "futures.tickers".to_string()
                }
            }),
        };
        let mock = MockProtocol::new();
        let specs = [mock.spec("A1"), mock.spec("A2"), mock.spec("B1")];
        let batch = build_packed(&mock, &grammar, "subscribe", true, &specs).unwrap();
        assert_eq!(batch.frames.len(), 2);
        let f0: Value = extract_json(&batch.frames[0]);
        let f1: Value = extract_json(&batch.frames[1]);
        assert_eq!(f0["channel"], "spot.tickers");
        assert_eq!(f0["event"], "subscribe");
        assert_eq!(f0["payload"].as_array().unwrap().len(), 2);
        assert_eq!(f1["channel"], "futures.tickers");
        assert_eq!(f1["payload"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn empty_specs_is_empty_build() {
        let grammar = BatchGrammar {
            topic_fn: topic_as_str,
            envelope: envelope_params_upper,
            chunk_cap: usize::MAX,
            group_key: None,
        };
        let mock = MockProtocol::new();
        let batch = build_packed(&mock, &grammar, "SUBSCRIBE", true, &[]).unwrap();
        assert!(batch.frames.is_empty());
        assert!(batch.submitted.is_empty());
        assert!(batch.dropped.is_empty());
    }

    fn extract_json(frame: &WsFrame) -> Value {
        match frame {
            WsFrame::Text(t) => serde_json::from_str(t).unwrap(),
            _ => panic!("expected text frame"),
        }
    }

    // ── Mock WsProtocol + helper specs ────────────────────────────────────

    struct MockProtocol;

    impl MockProtocol {
        fn new() -> Self { Self }

        fn spec(&self, sym: &str) -> StreamSpec {
            StreamSpec {
                kind: StreamKind::Ticker,
                symbol: OwnedSymbolInput::Raw(sym.into()),
                account_type: AccountType::Spot,
                depth: None,
                speed_ms: None,
            }
        }

        fn specs(&self, n: usize) -> Vec<StreamSpec> {
            (0..n).map(|i| self.spec(&format!("S{i}"))).collect()
        }
    }

    fn mock_registry() -> &'static TopicRegistry {
        static REG: std::sync::OnceLock<TopicRegistry> = std::sync::OnceLock::new();
        REG.get_or_init(|| TopicRegistryBuilder::default().build())
    }

    impl WsProtocol for MockProtocol {
        fn name(&self) -> &'static str { "mock" }
        fn endpoint(&self, _a: AccountType, _t: bool) -> Url {
            Url::parse("wss://mock.invalid").unwrap()
        }
        fn ping_frame(&self) -> Option<WsFrame> { None }
        fn auth_frame(&self, _creds: &crate::core::traits::Credentials) -> Option<Result<WsFrame, WebSocketError>> { None }
        fn subscribe_frame(&self, spec: &StreamSpec) -> Result<WsFrame, WebSocketError> {
            if spec.symbol.as_str() == "FAIL" {
                return Err(WebSocketError::NotImplemented("mock fail".into()));
            }
            Ok(WsFrame::Text(json!({"sub":"x"}).to_string()))
        }
        fn unsubscribe_frame(&self, _spec: &StreamSpec) -> Result<WsFrame, WebSocketError> {
            Ok(WsFrame::Text(json!({"unsub":"x"}).to_string()))
        }
        fn extract_topic(&self, _raw: &Value) -> Option<TopicKey> { None }
        fn topic_registry(&self, _a: AccountType) -> &TopicRegistry {
            mock_registry()
        }
    }
}