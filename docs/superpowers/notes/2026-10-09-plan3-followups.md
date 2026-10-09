# Plan 3 (Integrations) — follow-ups

Collected from per-task and final reviews of feat/integrations (cf7c6f1..8113b87).

## Rulings made during execution
- Task 4: Ruling: router reads/validates CloudEvent attributes and routes BEFORE decoding data (decode only on match) — plan text ordered decode first, which makes undecodable unmatched events bypass unmatched:'ack' on shared subjects — cost if wrong: none.
- Ruling: batcher captures AsyncLocalStorage.snapshot() per add() and publishes each item in its own snapshot — otherwise traces are cross-linked — cost if wrong: none.
- Ruling: CE data with a non-JSON datacontenttype different from the codec's contentType is handed to handlers as raw Uint8Array (no codec decode) — prevents lost/coerced foreign events — cost if wrong: handlers of such events must decode bytes themselves.
- Ruling: CloudEventInit gains optional datacontenttype; Uint8Array data without it omits datacontenttype; JetStream respects a user Nats-Msg-Id header; /otel no longer exports msgHdrsGetter/msgHdrsSetter (kept internal); tracing also falls back to ce-traceparent; idempotency reuses cached ctx.cloudEvent; README fixes — all small pre-publish items fixed in the same wave — cost if wrong: slightly larger diff.

## Deferred findings
- Task 1: implementer choices accepted — manual settlement via internal SETTLED_BY symbol; core success = none; core DLQ publish failure = drop with handler error; reply success = replied
- Task 1: minor (deferred): outcome records decision even when raw settle throws (no signal to consumers)
- Task 1: minor (deferred): manual ack then throw loses the error (ack variant has no error field) — note for OTel task
- Task 1: minor (deferred): manual nak outcome has error: undefined key; recordOutcome silent when map missing; test gaps (core store-error retry, manual batch, raw throw, dlq subject)
- Task 2: minor (deferred): structured read keeps both data/data_base64 (base64 wins), lenient base64; no ce-datacontenttype fallback; source|id key ambiguous if source has '|'; mixed-case duplicate headers order-dependent; string time not validated; KNOWN alias cosmetic
- Task 3: minor (deferred): datacontenttype fixed at creation even if plugin swaps codec; Uint8Array data labelled with codec content type (document)
- Task 3: minor (deferred): request() rejects cloudEvent only at type level; user Nats-Msg-Id header overwritten by ce.id when no msgId; test gaps (ce-extra on wire, codec swap, batcher cloudEvent)
- Task 4: minor (deferred): unmatched 'ack' on request recorded as outcome 'replied' though nothing sent; unparseable structured envelope still decode error even if type unmatched; 404 description echoes type without length cap; dlq unit cases don't assert publish call
- Task 5: minor (deferred): batch span named after first message subject; service endpoints lack consumer.group.name (HandlerInfo has no queue); invalid traceparent header blocks envelope fallback; String(err) for objects; per-call getTracer; no tracestate/baggage-layering test; guard regex single-quote only
- Task 6: minor (deferred): health total wait up to 2×rttTimeout (doc wording); rttTimeout not validated (Infinity/NaN); redaction only userinfo+pass/token; integration down test doesn't assert reason; no positive redacted-reason assertion; relies on NatsModule global
- Task 7: implementer choices accepted — protected hooks on NatsConnectionManager (documented internal); testing module provides mock clients under tokens + rejecting NatsStreamManager; default dispatch subject = handler subject; plugin failure outside settlement rethrown by dispatch
- Task 7: minor (deferred): protected hooks visible in public .d.ts (consider @internal + stripInternal); wildcard default dispatch subject (document); DLQ attribution under concurrent dispatches; no guard/interceptor harness test; silent handler+subject precedence / ignored inapplicable options; DispatchResult.result doc says never for batch; shared published stores (document)
- Task 8: implementer choice accepted — guard extended in public-api.spec instead of new entrypoints.spec
- Task 8: minor (deferred): /otel exports msgHdrsGetter/msgHdrsSetter undocumented (decide document vs internal); otel/health graph checks lack sanity assertion; README says message.cloudEvent (actual {meta,mode}); OTel example lacks npm install line for sdk-node/exporter; limitations not cross-listed in Known limitations
- Final: minor (deferred): lowercase nats-msg-id header from caller blocks ce.id default (server header case-sensitivity unverified); spec §9 text (datacontenttype from codec, Nats-Msg-Id rule) superseded by rulings — update spec; binary JSON codec + text/plain datacontenttype still JSON-encodes strings (documented); writeCloudEvent inlines data only for exact 'application/json'
