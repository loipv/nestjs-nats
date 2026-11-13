---
"@loipv/nestjs-nats": patch
---

Handler timeout polish:
- A timed-out run that rejects with an error caused by its abort, or with an axios `CanceledError`, is logged as stopped rather than failed.
- `ctx.working()` does nothing once the message is settled, so a run that outlived its timeout cannot override the nak.
- `NatsTestHarness.dispatch()` rejects an invalid module `handlerTimeout`, as bootstrap does.
- The shutdown warning counts the timed-out runs it gave up waiting for.
- Handlers without a timeout no longer pay for timeout bookkeeping.
