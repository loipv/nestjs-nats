import {
  type Attributes,
  type Counter,
  diag,
  type Histogram,
  type MeterProvider,
  metrics as metricsApi,
} from '@opentelemetry/api';
import type { Delivery } from '../pipeline/delivery.js';
import type { NatsPlugin, Next, OutgoingMessage } from '../pipeline/types.js';
import { consumerGroup } from './attributes.js';

export interface MetricsOptions {
  /** Name of the meter the instruments are created with. Default: '@loipv/nestjs-nats'. */
  meterName?: string;
  /**
   * Adds `messaging.destination.name`: `true` records the subject as is, a function records
   * what it returns for the subject (nothing when it returns `undefined`), for example to
   * turn `orders.123` into `orders.*`. Default: `false` (subjects can hold ids, and every
   * distinct value is a new series).
   */
  subjects?: boolean | ((subject: string) => string | undefined);
}

const DEFAULT_METER_NAME = '@loipv/nestjs-nats';
const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

interface Instruments {
  processDuration: Histogram;
  consumed: Counter;
  sent: Counter;
  operationDuration: Histogram;
}

/** `err.name` for an `Error` with a name, `_OTHER` for anything else. */
export function errorType(err: unknown): string {
  return err instanceof Error && err.name !== '' ? err.name : '_OTHER';
}

/**
 * Resolves the instruments on each record. The metrics API has no proxy provider (a meter
 * taken before the SDK registers stays a noop), so the instruments of one provider are
 * cached and rebuilt when `getMeterProvider()` returns another one.
 */
export function instrumentCache(meterName: string): () => Instruments {
  let slot: { provider: MeterProvider; instruments: Instruments } | undefined;
  return () => {
    const provider = metricsApi.getMeterProvider();
    if (slot?.provider === provider) return slot.instruments;
    const meter = provider.getMeter(meterName);
    const advice = { explicitBucketBoundaries: BUCKETS };
    const instruments: Instruments = {
      processDuration: meter.createHistogram('messaging.process.duration', {
        unit: 's',
        advice,
      }),
      consumed: meter.createCounter('messaging.client.consumed.messages', { unit: '{message}' }),
      sent: meter.createCounter('messaging.client.sent.messages', { unit: '{message}' }),
      operationDuration: meter.createHistogram('messaging.client.operation.duration', {
        unit: 's',
        advice,
      }),
    };
    slot = { provider, instruments };
    return instruments;
  };
}

/**
 * Returns the `messaging.destination.name` value for `subject`, or `undefined` when subjects
 * are off, the function returns nothing, or it throws (warned once per helper).
 */
export function subjectResolver(
  subjects: MetricsOptions['subjects'],
): (subject: string) => string | undefined {
  if (typeof subjects === 'function') {
    let warned = false;
    return (subject) => {
      try {
        return subjects(subject);
      } catch (err) {
        if (!warned) {
          warned = true;
          diag.warn('@loipv/nestjs-nats metrics: the `subjects` function threw', err);
        }
        return undefined;
      }
    };
  }
  return subjects === true ? (subject) => subject : () => undefined;
}

const seconds = (start: number): number => (performance.now() - start) / 1000;

/**
 * OpenTelemetry metrics plugin. Outbound sends record `messaging.client.sent.messages` and
 * `messaging.client.operation.duration`. Inbound deliveries record `messaging.process.duration` and
 * count each message by outcome in `messaging.client.consumed.messages`. Without an
 * OpenTelemetry SDK registered, it does nothing.
 */
export function metrics(options: MetricsOptions = {}): NatsPlugin {
  const instruments = instrumentCache(options.meterName ?? DEFAULT_METER_NAME);
  const subjectOf = subjectResolver(options.subjects);

  return {
    name: 'otel-metrics',
    inbound: {
      stage: 'outer',
      async handle(d: Delivery, next: Next): Promise<void> {
        const start = performance.now();
        let failure: { err: unknown } | undefined;
        try {
          await next();
        } catch (err) {
          failure = { err };
          throw err;
        } finally {
          const common: Attributes = {
            'messaging.system': 'nats',
            'messaging.operation.name': 'process',
            'nats.connection': d.handler.connection,
            'nats.handler': `${d.handler.className}.${d.handler.methodName}`,
            'nats.handler.kind': d.handler.kind,
          };
          const group = consumerGroup(d);
          if (group !== undefined) common['messaging.consumer.group.name'] = group;
          if (d.handler.kind === 'consume' || d.handler.kind === 'ordered') {
            common['nats.stream'] = d.handler.stream;
          }
          const { processDuration, consumed } = instruments();

          const first = d.messages[0];
          const firstSubject = first ? subjectOf(first.subject) : undefined;
          processDuration.record(seconds(start), {
            ...common,
            ...(firstSubject !== undefined ? { 'messaging.destination.name': firstSubject } : {}),
            ...(failure ? { 'error.type': errorType(failure.err) } : {}),
          });

          for (const message of d.messages) {
            const outcome = d.outcomes.get(message);
            const subject = subjectOf(message.subject);
            const error = outcome && 'error' in outcome ? outcome.error : undefined;
            consumed.add(1, {
              ...common,
              ...(subject !== undefined ? { 'messaging.destination.name': subject } : {}),
              'nats.outcome': outcome?.action ?? 'unsettled',
              ...(error !== undefined ? { 'error.type': errorType(error) } : {}),
            });
          }
        }
      },
    },
    outbound: {
      async handle(m: OutgoingMessage, next: Next): Promise<void> {
        const start = performance.now();
        let failure: { err: unknown } | undefined;
        try {
          await next();
        } catch (err) {
          failure = { err };
          throw err;
        } finally {
          const subject = subjectOf(m.subject);
          const attributes: Attributes = {
            'messaging.system': 'nats',
            'messaging.operation.name': m.kind === 'request' ? 'request' : 'publish',
            'nats.connection': m.connection,
            'nats.publish.kind': m.kind === 'publish' ? 'core' : m.kind,
            ...(subject !== undefined ? { 'messaging.destination.name': subject } : {}),
            ...(m.deadLetter ? { 'nats.dead_letter': true } : {}),
            ...(failure ? { 'error.type': errorType(failure.err) } : {}),
          };
          const { sent, operationDuration } = instruments();
          sent.add(1, attributes);
          operationDuration.record(seconds(start), attributes);
        }
      },
    },
  };
}
