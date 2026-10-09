import type { JsMsg } from '@nats-io/jetstream';
import type { Msg, MsgHdrs } from '@nats-io/transport-node';
import type { LoggerService } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import type { NatsContext } from '../context/nats-context.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { compose } from './compose.js';
import type { InboundMiddleware, NatsPlugin, Next } from './types.js';

export interface InboundMessage {
  subject: string;
  data: Uint8Array;
  headers?: MsgHdrs;
  raw: Msg | JsMsg;
}

/** One unit of inbound work flowing through the inbound pipeline. */
export interface Delivery {
  readonly handler: HandlerDefinition;
  readonly messages: InboundMessage[];
  readonly context: NatsContext;
  readonly codec: Codec;
  readonly logger: LoggerService;
  result?: unknown;
}

type InboundHandler = (d: Delivery, next: Next) => Promise<void>;

const toHandler =
  (mw: InboundMiddleware): InboundHandler =>
  (d, next) =>
    mw.handle(d, next);

/**
 * Builds the inbound pipeline: plugins with `stage: 'outer'`, then the core
 * middlewares, then plugins with `stage: 'inner'` (or no stage), then the terminal.
 * Plugins keep their registration order within a stage.
 */
export function buildInboundPipeline(
  parts: { plugins: NatsPlugin[]; core: InboundMiddleware[] },
  terminal: (d: Delivery) => Promise<void>,
): (d: Delivery) => Promise<void> {
  const outer: InboundHandler[] = [];
  const inner: InboundHandler[] = [];
  for (const { inbound } of parts.plugins) {
    if (!inbound) continue;
    (inbound.stage === 'outer' ? outer : inner).push(toHandler(inbound));
  }
  return compose([...outer, ...parts.core.map(toHandler), ...inner], terminal);
}
