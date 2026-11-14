---
"@loipv/nestjs-nats": patch
---

`NatsClient.request()` reports `no_responders` when the `NoRespondersError` is deeper in the error's cause chain, or comes from another copy of the NATS library, as dead-lettering already did; it used to report such a request as a plain error. Internal clean-up otherwise: handler discovery is split into smaller modules, and duplicated helpers are shared.
