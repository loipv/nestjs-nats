# Plan 1 (Core) — follow-ups for Plans 2–4

Collected from per-task and final reviews of feat/core (f534a8a..cc17911). Items not fixed in Plan 1, grouped by owner.

## Rulings made during execution
- Ruling: Task 2 creates src/pipeline/types.ts with interim `NatsPlugin { name: string }` — plan Interfaces block says so; Files list omission is clerical — cost if wrong: none.
- Ruling: keep decorator names `Msg`/`Headers` per approved spec despite shadowing nats `Msg` type / global `Headers`; implementers alias imports internally (`type Msg as NatsMsg`) — cost if wrong: users alias imports; renaming later is a breaking change before 1.0 (acceptable in 0.x).
- Ruling: Task 8 "terms non-JSON" test — implementer records handler call count before publishing and asserts unchanged — cost if wrong: none.
- Task 6: Ruling: NatsParamType values changed to PAYLOAD=3, HEADERS=6, SUBJECT=7, MSG=8, CONTEXT=9, CLOUD_EVENT=10 (plan said 0..5) — Nest maps paramtype 3→'body', 4→'query', 5→'param', else 'custom'; ValidationPipe skips 'custom', so plan values would break the spec's "global ValidationPipe works" requirement; 3 matches @nestjs/microservices RpcParamtype.PAYLOAD — cost if wrong: none observable to users (internal enum values), Task 5 tests updated.
- Ruling: double logging of handler errors (Nest ExternalExceptionFilter + basicSettlement) kept for Plan 1; Nest's log follows app-level logger and is the only log of 500 replies; Plan 2 settlement redesign owns de-duplication and README documents that `logger: false` does not silence Nest's ExceptionsHandler — cost if wrong: noisy logs until Plan 2.
- Ruling: Task 7 lifecycle must close already-opened connections (stop adapters + closeAll) when onModuleInit sync or onApplicationBootstrap fails, then rethrow — spec §4 fail-fast implies a clean failure; with abortOnError:false an open socket would keep the process alive — cost if wrong: small extra code.
- Ruling: Task 7 deviation accepted — explorer returns orphan @ServiceEndpoint (no @NatsService) as service-endpoint with service: undefined so validateHandlers can reject it (spec §5 requires the error) — cost if wrong: Task 9 must narrow service !== undefined.
- Ruling: renumber NatsParamType to PAYLOAD=3, CONTEXT=6, SUBJECT=7, MSG=8, HEADERS=10, CLOUD_EVENT=11 so @Ctx from @nestjs/microservices (RpcParamtype.CONTEXT=6) resolves the context instead of headers — internal values, no user-visible cost; cost if wrong: none.
- Ruling: fix final-review minors 4-9 in the same wave (public API shape before Plan 3 depends on it) — cost if wrong: slightly larger fix diff.
- Ruling: keep deferred — publish ordering with async outbound plugins (document in README, Plan 4), logger context `NatsConnection:<name>` vs spec `NatsModule` (document) — cost if wrong: doc-only.
- Ruling: drainTimeout not bounding sub.drain()/nc.drain() must be fixed in Plan 2 before any npm publish (shutdown can hang during NATS outage) — carried to Plan 2.

## Deferred findings
- Task 1: minor (deferred): own `cause: undefined` property on subclasses when no cause given (errors.ts)
- Task 1: minor (deferred): jsonCodec silently encodes functions/symbols to empty payload
- Task 1: minor (deferred): exported HeadersInit shadows DOM type name
- Task 1: minor (deferred): test gaps — empty-array header, double-wrap NatsDecodeError, headers forwarded to codec
- Task 2: minor (deferred): status$ events forward raw `data` (server lists) unredacted to subscribers
- Task 2: minor (deferred): NatsConnectionError.cause is raw nats error; ERR_INVALID_URL `input` may carry creds if cause logged verbatim
- Task 2: minor (deferred): events$ Subject never completed; entries map not cleared after closeAll
- Task 2: minor (deferred): redundant .catch attached on each jsm() call
- Task 3: minor (deferred): publish() throws synchronously on invalid header input (toMsgHdrs) instead of logging
- Task 3: minor (deferred): serviceCode Number() may be NaN for non-numeric header
- Task 3: minor (deferred): forRootAsync without factory/class/existing throws bare TypeError, should be NatsConfigError
- Task 3: minor (deferred): no tests for useExisting, request short-circuit, NoResponders via err.cause
- Task 3: minor (deferred): bootstrap failure after a connection opened leaks that connection (shutdown hooks never run) — relevant to Task 7 lifecycle
- Task 3: minor (deferred): `reply` not visible on OutgoingMessage to plugins (by interface design)
- Task 4: minor (deferred): publish expect test uses bare rejects.toThrow()
- Task 4: minor (deferred, user-visible risk): diffConfig treats server-omitted falsy/default fields (no_ack:false, headers_only:false, nested placement/republish) as drift → spurious update each boot / false verify error
- Task 4: minor (deferred): no tests for create-only consumer, verify drifted consumer, unit mode table
- Task 4: minor (deferred): sync failure in onModuleInit leaves opened connections unclosed (same as Task 3 bootstrap-leak note)
- Task 4: minor (deferred): consumer defaults applied before diff → existing consumer with non-explicit ack_policy conflicts; document
- Task 5: minor (deferred): Payload(undefined, Pipe) treated as pipes list → pipes [undefined, Pipe]
- Task 5: minor (deferred): stale comment/cast in nats-context.ts about JsMsg.reply (JsMsg has reply getter)
- Task 5: minor (deferred): ack/nak/term set settled before calling raw — if raw throws, context says settled (carried to Task 6)
- Task 5: minor (deferred): param decorator on constructor param silently ignored
- Task 5: minor (deferred): coexistence test with createParamDecorator weak (Task 6 enhancers test covers end-to-end)
- Task 6: concern (for review): handler errors logged twice (Nest ExternalExceptionFilter + basicSettlement); Nest's log ignores logger:false
- Task 6: minor (deferred): EMPTY observable from handler → EmptyError via ECC → nak/500 (document)
- Task 6: minor (deferred): decode errors bypass Nest exception filters (decode before invoke; plan-mandated order)
- Task 6: minor (deferred): no TRANSIENT-scope or global-pipe (APP_PIPE) tests
- Task 6: minor (deferred): raw ack/nak/term throw leaves context settled; logged only (Plan 2 settlement replaces)
- Task 7: minor (deferred): drainTimeout does not bound adapter stop() (sub.drain) round-trip — stop runs before the timed race
- Task 7: minor (deferred): drainTimeout merge ignores unset connections ({50, unset} → 50 not 10000)
- Task 7: minor (deferred): request-path 5xx from plugin/codec not logged by library (only replied)
- Task 7: minor (deferred): 400-reply test ambiguous — orders.* wildcard handler also replies
- Task 7: minor (deferred): bootstrap-cleanup test never has started adapters to stop
- Task 7: minor (deferred): limiter release() at 0 silently ignored
- Task 7: minor (deferred, docs): onModuleDestroy runs before beforeApplicationShutdown — in-flight handlers may see torn-down user resources
- Task 7: minor (deferred): core adapter imports @nats-io/services only for header constants
- Task 8: minor (deferred, docs): nak-on-stop / close() drops already-delivered pull batch → messages ack-pending until ack_wait (30s); nak counts toward max_deliver
- Task 8: minor (deferred): dead consume loop is logged but not auto-restarted
- Task 8: minor (deferred): errorTrace helper duplicated (adapter + basic-settlement)
- Task 9: minor (deferred): service death via subscription error (service.stopped resolves with Error) not logged; endpoint loop never ends, idle blocks to drainTimeout
- Task 9: minor (deferred): adapter duplication — errorTrace x3, consume loop/respond/respondError near-copies across adapters
- Task 9: minor (deferred): dead guard + duplicate manager.get in lifecycle grouping
- Task 9: minor (deferred): unknown-connection service test fails in validator, not createAdapters
- Task 9: minor (deferred): tick()-based unit waits brittle; msg.respond boolean ignored
- Task 10: minor (deferred): stray section comments in src/index.ts after biome import sort
- Task 10: minor (deferred): optional-peer guard only regexes src/index.ts (not transitive); no type-level export test
- Final: deferred (docs) — subset diff never reports removed keys; server-normalized declared values (duplicate_window:0, max_deliver:0) still drift; null not treated as absent
