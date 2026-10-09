import type { MsgHdrs } from '@nats-io/transport-node';
import {
  type Attributes,
  type Context,
  context,
  type Exception,
  isSpanContextValid,
  type Link,
  propagation,
  ROOT_CONTEXT,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import { NatsJetStreamContext } from '../context/nats-context.js';
import { type Delivery, messageContexts } from '../pipeline/delivery.js';
import { isReplyDelivery } from '../pipeline/reply.js';
import type { NatsPlugin, Next, OutgoingMessage } from '../pipeline/types.js';
import { extractContext, headerValues, msgHdrsSetter } from './carrier.js';

export interface TracingOptions {
  /** Name of the tracer the spans are created with. Default: `'@loipv/nestjs-nats'`. */
  tracerName?: string;
  /**
   * Headers recorded as `messaging.header.<lowercased name>` span attributes (a string, or
   * a string array when the header has several values). Names match case-insensitively.
   */
  captureHeaders?: string[];
}

const DEFAULT_TRACER_NAME = '@loipv/nestjs-nats';
const MSG_ID_HEADER = 'Nats-Msg-Id';

const toException = (err: unknown): Exception => (err instanceof Error ? err : String(err));

/** Records `err` on `span` once (an error can show up both as an outcome and as a throw). */
function errorRecorder(span: Span): (err: unknown) => void {
  const seen = new Set<unknown>();
  return (err) => {
    if (seen.has(err)) return;
    seen.add(err);
    span.recordException(toException(err));
    span.setStatus({
      code: SpanStatusCode.ERROR,
      ...(err instanceof Error ? { message: err.message } : {}),
    });
  };
}

function captureHeaders(attributes: Attributes, h: MsgHdrs | undefined, names: string[]): void {
  for (const name of names) {
    const values = headerValues(h, name);
    if (values.length === 0) continue;
    attributes[`messaging.header.${name.toLowerCase()}`] =
      values.length === 1 ? (values[0] as string) : values;
  }
}

function consumerGroup(d: Delivery): string | undefined {
  switch (d.handler.kind) {
    case 'consume':
      return d.handler.durable;
    case 'subscribe':
      return d.handler.queue;
    default:
      return undefined;
  }
}

function recordOutcomes(span: Span, d: Delivery, recordError: (err: unknown) => void): void {
  for (const [message, outcome] of d.outcomes) {
    switch (outcome.action) {
      case 'retry':
        span.addEvent('nats.retry', {
          'nats.retry.delay_ms': outcome.delayMs,
          'nats.attempt': message.attempt,
        });
        break;
      case 'dlq':
        span.addEvent('nats.dlq', { 'nats.dlq.subject': outcome.subject });
        break;
      case 'skipped':
        span.addEvent('nats.skipped', { 'nats.skipped.reason': outcome.reason });
        break;
      default:
        break;
    }
    if ('error' in outcome && outcome.error !== undefined) recordError(outcome.error);
  }
}

/**
 * OpenTelemetry tracing plugin. Inbound deliveries get a `process <subject>` span
 * (CONSUMER, or SERVER when a reply is expected) that is a child of the context propagated
 * with the message, or, for a batch, a new trace linked to each message's context.
 * Outbound messages get a `publish <subject>` (PRODUCER) or `send <subject>` (CLIENT, for
 * requests) span whose context is injected into the message headers. Without an
 * OpenTelemetry SDK registered, it does nothing.
 */
export function tracing(options: TracingOptions = {}): NatsPlugin {
  const tracerName = options.tracerName ?? DEFAULT_TRACER_NAME;
  const captured = options.captureHeaders ?? [];
  // Resolved on each use, so a provider registered (or removed) later is honoured.
  const tracer = () => trace.getTracer(tracerName);

  return {
    name: 'otel-tracing',
    inbound: {
      stage: 'outer',
      async handle(d: Delivery, next: Next): Promise<void> {
        const contexts = messageContexts(d);
        const subject = d.messages[0]?.subject ?? '';
        const attributes: Attributes = {
          'messaging.system': 'nats',
          'messaging.destination.name': subject,
          'messaging.operation.type': 'process',
          'messaging.operation.name': 'process',
        };
        const group = consumerGroup(d);
        if (group !== undefined) attributes['messaging.consumer.group.name'] = group;

        let parent: Context = ROOT_CONTEXT;
        const links: Link[] = [];
        if (d.context.kind === 'batch') {
          attributes['messaging.batch.message_count'] = d.messages.length;
          d.messages.forEach((message, i) => {
            const ctx = contexts[i];
            if (!ctx) return;
            const linked = trace.getSpanContext(extractContext(message, ctx));
            if (linked && isSpanContextValid(linked)) links.push({ context: linked });
          });
        } else {
          const message = d.messages[0];
          const ctx = d.context;
          if (message) {
            parent = extractContext(message, ctx);
            const id = headerValues(message.headers, MSG_ID_HEADER)[0] ?? ctx.cloudEvent?.id;
            if (id) attributes['messaging.message.id'] = id;
            attributes['messaging.message.body.size'] = message.data.length;
            if (ctx instanceof NatsJetStreamContext) {
              attributes['messaging.nats.stream'] = ctx.stream;
              attributes['messaging.nats.seq'] = ctx.seq;
              attributes['messaging.nats.delivery_count'] = ctx.deliveryCount;
            }
            captureHeaders(attributes, message.headers, captured);
          }
        }

        const span = tracer().startSpan(
          `process ${subject}`,
          {
            kind: isReplyDelivery(d) ? SpanKind.SERVER : SpanKind.CONSUMER,
            attributes,
            links,
          },
          parent,
        );
        const recordError = errorRecorder(span);
        try {
          await context.with(trace.setSpan(parent, span), next);
        } catch (err) {
          recordError(err);
          throw err;
        } finally {
          recordOutcomes(span, d, recordError);
          span.end();
        }
      },
    },
    outbound: {
      async handle(m: OutgoingMessage, next: Next): Promise<void> {
        const request = m.kind === 'request';
        const attributes: Attributes = {
          'messaging.system': 'nats',
          'messaging.destination.name': m.subject,
          'messaging.operation.type': 'send',
          'messaging.operation.name': request ? 'request' : 'publish',
        };
        const id = headerValues(m.headers, MSG_ID_HEADER)[0] ?? m.cloudEvent?.meta.id;
        if (id) attributes['messaging.message.id'] = id;
        captureHeaders(attributes, m.headers, captured);

        const span = tracer().startSpan(`${request ? 'send' : 'publish'} ${m.subject}`, {
          kind: request ? SpanKind.CLIENT : SpanKind.PRODUCER,
          attributes,
        });
        const ctx = trace.setSpan(context.active(), span);
        try {
          await context.with(ctx, () => {
            propagation.inject(ctx, m.headers, msgHdrsSetter);
            return next();
          });
        } catch (err) {
          errorRecorder(span)(err);
          throw err;
        } finally {
          span.end();
        }
      },
    },
  };
}
