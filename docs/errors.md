# Errors

[Back to the README](../README.md)

| Class | Thrown when |
|---|---|
| `NatsModuleError` | Base class of all errors below |
| `NatsConfigError` | Invalid configuration found at startup. Examples: unknown connection name, duplicate durable, endpoint without `@NatsService`, request-scoped handler provider, missing consumer, an invalid `@CloudEventRouter`. Also thrown when publishing an invalid `cloudEvent` (no source, bad extension name) |
| `NatsConnectionError` | The first connection attempt failed, including authentication errors. Its `cause` is a sanitized copy of the client's error: the same `name` and `code`, with credentials masked in the message and stack, and no other properties |
| `StreamConfigConflictError` | A declared stream or consumer differs in an immutable field. The error carries `field`, `current` and `declared` |
| `NatsDecodeError` | A payload could not be decoded, or a message is not a valid CloudEvent |
| `NatsAtomicPublishError` | `JetStreamClient.publishAtomic()` failed or was aborted. None of the messages was stored, except after a timeout waiting for the commit's ack: the server may have stored the batch then (see [Atomic batches](./publishing.md#atomic-batches)). It has the `subject` of the first message and the `count` of messages, and its `cause`, when it has one, is a copy of the underlying error with credentials masked: that is the error `publishAtomic()` rejects with when sending or committing the batch fails. When the batch is aborted before it is sent, `publishAtomic()` rejects with the failing plugin's own error (unchanged) or, for the staging timeout or a plugin that did not pass a message on, with an abort error that has no `cause`; the abort error that plugins see through `next()` keeps the plugin's raw error as `cause`, so it is not masked. Invalid arguments throw a `NatsConfigError` instead |
| `NatsRequestError` | `NatsClient.request()` failed. See `code` |
| `NonRetryableError` | Throw it yourself to tell the library not to retry a message. It goes straight to the DLQ if there is one, else it is dropped |

A failing request handler or service endpoint replies with the `Nats-Service-Error` and `Nats-Service-Error-Code` headers:

| Error | Code | Description |
|---|---|---|
| `HttpException` | its status | its message |
| `NatsDecodeError` | `400` | `Bad Request` |
| anything else | `500` | `Internal Server Error`. Internal details are never sent |
