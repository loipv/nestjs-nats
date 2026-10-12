import { type Attributes, context, metrics, propagation, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { MeterProvider, MetricReader } from '@opentelemetry/sdk-metrics';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';

export interface TestTracing {
  exporter: InMemorySpanExporter;
  provider: BasicTracerProvider;
  /** Drops the spans recorded so far. */
  reset(): void;
  /** Unregisters the global provider, context manager and propagator. */
  shutdown(): Promise<void>;
}

/** Registers an in-memory tracer provider, an async-hooks context manager and W3C propagation. */
export function setupTracing(): TestTracing {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  trace.setGlobalTracerProvider(provider);
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  return {
    exporter,
    provider,
    reset: () => exporter.reset(),
    shutdown: async () => {
      await provider.shutdown();
      trace.disable();
      context.disable();
      propagation.disable();
    },
  };
}

class TestReader extends MetricReader {
  protected async onForceFlush(): Promise<void> {}
  protected async onShutdown(): Promise<void> {}
}

export interface TestMetrics {
  provider: MeterProvider;
  /** Collects and returns the data points of `name`, each `{ attributes, value }`. */
  points(name: string): Promise<Array<{ attributes: Attributes; value: unknown }>>;
  /** Collects and returns the names of the meters (scopes) that recorded metrics. */
  scopes(): Promise<string[]>;
  /** Shuts the provider down and unregisters the global meter provider. */
  shutdown(): Promise<void>;
}

/** Registers an in-memory meter provider read by a manual `collect()`. */
export function setupMetrics(): TestMetrics {
  const reader = new TestReader();
  const provider = new MeterProvider({ readers: [reader] });
  metrics.setGlobalMeterProvider(provider);
  return {
    provider,
    async points(name) {
      const { resourceMetrics } = await reader.collect();
      return resourceMetrics.scopeMetrics
        .flatMap((s) => s.metrics)
        .filter((m) => m.descriptor.name === name)
        .flatMap((m) => m.dataPoints.map((p) => ({ attributes: p.attributes, value: p.value })));
    },
    async scopes() {
      const { resourceMetrics } = await reader.collect();
      return resourceMetrics.scopeMetrics.map((s) => s.scope.name);
    },
    async shutdown() {
      await provider.shutdown();
      metrics.disable();
    },
  };
}
