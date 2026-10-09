# `@loipv/nestjs-nats` — Plan 1: Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây phần lõi của thư viện: kết nối nhiều named connection, `NatsModule`, decorator `@Subscribe` / `@Consume` / `@NatsService` + `@ServiceEndpoint`, param decorators chạy qua pipes/guards/interceptors/filters của Nest, message pipeline có plugin, `NatsClient`, `JetStreamClient.publish`, đồng bộ stream/consumer khai báo.

**Architecture:** Dynamic module độc lập. `NatsConnectionManager` giữ các connection; `NatsLifecycle` đồng bộ stream (onModuleInit), khám phá handler bằng `DiscoveryService` và khởi động adapter (onApplicationBootstrap), dừng adapter + drain (beforeApplicationShutdown). Mỗi message đi qua pipeline onion `outer plugins → core middlewares → inner plugins → invoker`; invoker gọi handler đã bọc bằng `ExternalContextCreator` với `contextType = 'nats'`.

**Tech Stack:** TypeScript 6.0.x (ESM, NodeNext), NestJS 11/12, `@nats-io/transport-node` / `jetstream` / `services` 3.x, vitest 5 + `unplugin-swc`, Testcontainers (`nats:2.12-alpine`), Biome.

**Spec:** `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md`

**Phạm vi plan này** = spec §19 bước 1. Ngoài phạm vi (plan sau): retry/backoff/DLQ, heartbeat `working()`, idempotency, batch, `publishMany`/`Batcher`, ShutdownCoordinator đầy đủ (Plan 2); CloudEvents, `/otel`, `/health`, `/testing` (Plan 3); docs, examples, CI matrix, release (Plan 4). Plan 1 dùng settlement tối giản (ack / nak không delay) và shutdown tối giản; Plan 2 thay thế đúng hai điểm đó qua interface đã định ở đây.

## Global Constraints

- Package `@loipv/nestjs-nats`, `version: "0.1.0"`, `license: "MIT"`, `"type": "module"`, `sideEffects: false`, `engines.node: ">=22.12"`.
- ESM-only: mọi import tương đối trong `src/` và `test/` dùng đuôi `.js` (`moduleResolution: NodeNext`).
- peerDependencies: `@nestjs/common` & `@nestjs/core` `^11.0.0 || ^12.0.0`, `reflect-metadata` `^0.2.0`, `rxjs` `^7.1.0`, `@nats-io/transport-node` / `@nats-io/jetstream` / `@nats-io/services` `^3.0.0`. **Không có `dependencies`.**
- Entry chính không được import `@opentelemetry/api` hay `@nestjs/terminus`.
- Log, error message, health output không bao giờ chứa `pass`, `token`, seed, nội dung creds; userinfo trong URL server được thay bằng `***`.
- TypeScript `~6.0.3`, `strict`, `experimentalDecorators`, `emitDecoratorMetadata`, `isolatedModules`. Không dùng TS 7.
- Tên connection mặc định: `'default'`.
- Integration test cần Docker; image `nats:2.12-alpine` chạy với `-js`.
- Commit theo Conventional Commits; mỗi task kết thúc bằng một commit.

## Review Focus

1. **Payload không phải JSON** (vd. `nats pub orders.get hello` từ CLI) tới handler dùng `jsonCodec` → không crash process; request/reply nhận lỗi `400`; JetStream message bị `term()` thay vì nak lặp vô hạn. Test ở Task 6 (mapping) và Task 7 (end-to-end).
2. **Handler throw đồng bộ** (method không `async`) → xử lý y hệt promise bị reject. Test ở Task 6.
3. **Subscribe subject wildcard** (`orders.*`, `orders.>`) → `@Subject()` trả về subject thực tế của message, không phải pattern. Test ở Task 7.
4. **`concurrency: 1` phải giữ thứ tự** khi handler chậm và message tới dồn dập. Test ở Task 7 (core) và Task 8 (JetStream).
5. **Payload rỗng / handler trả `undefined`** → handler nhận `undefined`; `@ServiceEndpoint` trả `undefined` vẫn reply payload rỗng (requester không bị timeout); `@Subscribe` trả `undefined` thì không reply. Test ở Task 1, Task 7, Task 9.

---

## File structure

```
package.json  tsconfig.json  tsconfig.build.json  vitest.config.ts  biome.json  .gitignore  LICENSE
src/
  index.ts                               # public exports (Task 10 hoàn thiện)
  errors/errors.ts                       # Task 1
  codec/codec.ts  codec/headers.ts       # Task 1
  module/tokens.ts  module/options.ts    # Task 2
  module/logger.ts                       # Task 2
  connection/connection-manager.ts  connection/redact.ts   # Task 2
  pipeline/compose.ts  pipeline/types.ts # Task 3 (outbound), Task 6 (inbound)
  client/nats-client.ts                  # Task 3
  module/nats.module.ts  module/nats-core.module.ts  module/inject.decorators.ts  # Task 3
  jetstream/stream-sync.ts  jetstream/stream-manager.ts  jetstream/jetstream-client.ts  # Task 4
  module/nats-lifecycle.ts               # Task 4 (sync), Task 7 (start/stop)
  decorators/constants.ts  decorators/handler.decorators.ts  decorators/param.decorators.ts  # Task 5
  context/nats-context.ts                # Task 5
  pipeline/delivery.ts  pipeline/invoker.ts  pipeline/basic-settlement.ts  pipeline/reply.ts  # Task 6
  discovery/nats-params-factory.ts  discovery/handler-explorer.ts  discovery/handler-validator.ts  # Task 6/7
  adapters/limiter.ts  adapters/adapter.ts  adapters/core-subscription.adapter.ts  # Task 7
  adapters/jetstream-consumer.adapter.ts # Task 8
  adapters/service-endpoint.adapter.ts   # Task 9
test/
  unit/**.spec.ts
  integration/support/nats-server.ts  integration/support/app.ts
  integration/**.int.spec.ts
```

---

### Task 1: Scaffold, errors, codec, headers

**Files:**
- Modify: `package.json`
- Create: `tsconfig.json`, `tsconfig.build.json`, `vitest.config.ts`, `biome.json`, `.gitignore`, `LICENSE`
- Create: `src/errors/errors.ts`, `src/codec/codec.ts`, `src/codec/headers.ts`
- Test: `test/unit/errors.spec.ts`, `test/unit/codec.spec.ts`, `test/unit/headers.spec.ts`

**Interfaces:**
- Produces:
  - `class NatsModuleError extends Error` (`constructor(message: string, options?: { cause?: unknown })`, `name` = tên class)
  - `class NatsConfigError extends NatsModuleError`
  - `class NatsConnectionError extends NatsModuleError` — `readonly connection: string; readonly servers: string[]`
  - `class StreamConfigConflictError extends NatsModuleError` — `readonly stream: string; readonly consumer?: string; readonly field: string; readonly current: unknown; readonly declared: unknown`
  - `class NatsDecodeError extends NatsModuleError`
  - `type NatsRequestErrorCode = 'timeout' | 'no_responders' | 'service_error'`; `class NatsRequestError extends NatsModuleError` — `readonly code; readonly subject: string; readonly serviceCode?: number; readonly description?: string`
  - `class NonRetryableError extends NatsModuleError`
  - `interface Codec { readonly contentType: string; encode(data: unknown): Uint8Array; decode(bytes: Uint8Array, headers?: MsgHdrs): unknown }`
  - `jsonCodec(): Codec` (`contentType: 'application/json'`)
  - `encodePayload(codec: Codec, data: unknown): Uint8Array` — `Uint8Array` đi thẳng; `undefined` → `new Uint8Array(0)`
  - `decodePayload(codec: Codec, bytes: Uint8Array, headers?: MsgHdrs): unknown` — length 0 → `undefined`; codec throw → `NatsDecodeError` (cause = lỗi gốc)
  - `type HeadersInit = Record<string, string | string[]>`
  - `toMsgHdrs(init?: HeadersInit, base?: MsgHdrs): MsgHdrs` — luôn trả về `MsgHdrs` (tạo mới bằng `headers()` của `@nats-io/transport-node` nếu không có `base`); mảng → `append` từng giá trị
  - `fromMsgHdrs(h?: MsgHdrs): Record<string, string | string[]>` — 1 giá trị → `string`, nhiều → `string[]`, `undefined` → `{}`

- [ ] **Step 1: Scaffold**

`package.json`: giữ `name`, đặt `version 0.1.0`, `license MIT`, `type module`, `sideEffects false`, `engines`, `exports: { ".": { "types": "./dist/index.d.ts", "default": "./dist/index.js" } }`, `files: ["dist"]`, peerDependencies như Global Constraints. Scripts:
```json
{ "build": "tsc -p tsconfig.build.json", "test": "vitest run --project unit",
  "test:int": "vitest run --project integration", "lint": "biome check .",
  "check:pkg": "publint && attw --pack . --profile esm-only" }
```
devDependencies: `@nestjs/common@^12.1.2 @nestjs/core@^12.1.2 @nestjs/testing@^12.1.2 reflect-metadata rxjs @nats-io/transport-node@^3.4.0 @nats-io/jetstream@^3.4.0 @nats-io/services@^3.4.0 typescript@~6.0.3 vitest@^5 unplugin-swc @swc/core testcontainers @biomejs/biome @types/node@^22 publint @arethetypeswrong/cli class-validator class-transformer`.

`tsconfig.json`: `target ES2023`, `module/moduleResolution NodeNext`, `strict`, `experimentalDecorators`, `emitDecoratorMetadata`, `isolatedModules`, `declaration`, `sourceMap`, `outDir dist`, `skipLibCheck`; include `src`, `test`. `tsconfig.build.json` extends nó, `rootDir src`, include chỉ `src`.

`vitest.config.ts`: plugin `swc.vite({ jsc: { transform: { legacyDecorator: true, decoratorMetadata: true } } })`; `test.projects` gồm `unit` (`test/unit/**/*.spec.ts`) và `integration` (`test/integration/**/*.int.spec.ts`, `testTimeout 60_000`, `hookTimeout 120_000`, `fileParallelism: false`). `setupFiles` import `reflect-metadata`.

Run: `npm install && npx tsc --noEmit -p tsconfig.json`
Expected: cài đặt xong, tsc không lỗi (chưa có file nguồn).

- [ ] **Step 2: Write failing tests**

```ts
// test/unit/codec.spec.ts
it('roundtrips JSON', () => { const c = jsonCodec(); expect(decodePayload(c, encodePayload(c, { a: 1 }))).toEqual({ a: 1 }); });
it('passes Uint8Array through untouched', () => { const b = new Uint8Array([1, 2]); expect(encodePayload(jsonCodec(), b)).toBe(b); });
it('encodes undefined as empty and decodes empty as undefined', () => {
  expect(encodePayload(jsonCodec(), undefined).length).toBe(0);
  expect(decodePayload(jsonCodec(), new Uint8Array(0))).toBeUndefined();
});
it('wraps decode failures in NatsDecodeError', () => {
  const bad = new TextEncoder().encode('hello');
  expect(() => decodePayload(jsonCodec(), bad)).toThrow(NatsDecodeError);
});
// test/unit/headers.spec.ts
it('maps single and multi values', () => {
  const h = toMsgHdrs({ a: '1', b: ['x', 'y'] });
  expect(fromMsgHdrs(h)).toEqual({ a: '1', b: ['x', 'y'] });
});
it('returns {} for undefined', () => expect(fromMsgHdrs(undefined)).toEqual({}));
// test/unit/errors.spec.ts
it('sets name and keeps cause', () => {
  const e = new NatsDecodeError('x', { cause: 'c' });
  expect(e).toBeInstanceOf(NatsModuleError); expect(e.name).toBe('NatsDecodeError'); expect(e.cause).toBe('c');
});
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `npm test`
Expected: FAIL — không resolve được `../../src/codec/codec.js`.

- [ ] **Step 4: Implement `src/errors/errors.ts`, `src/codec/codec.ts`, `src/codec/headers.ts`** theo Interfaces ở trên.

- [ ] **Step 5: Run tests, lint, typecheck**

Run: `npm test && npm run lint && npx tsc --noEmit`
Expected: tất cả PASS, không lỗi.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: scaffold package; feat: errors, codec, headers"
```

---

### Task 2: Options, tokens, logger, connection manager

**Files:**
- Create: `src/module/options.ts`, `src/module/tokens.ts`, `src/module/logger.ts`, `src/connection/redact.ts`, `src/connection/connection-manager.ts`
- Create: `test/integration/support/nats-server.ts`
- Test: `test/unit/redact.spec.ts`, `test/integration/connection-manager.int.spec.ts`

**Interfaces:**
- Consumes: `NatsConfigError`, `NatsConnectionError`, `Codec` (Task 1)
- Produces:
  - `DEFAULT_CONNECTION = 'default'`
  - `getConnectionToken(name?) → 'NATS_CONNECTION:<name>'`, `getNatsClientToken(name?) → 'NATS_CLIENT:<name>'`, `getJetStreamClientToken(name?) → 'NATS_JETSTREAM:<name>'`, `getStreamManagerToken(name?) → 'NATS_STREAM_MANAGER:<name>'`, `getModuleOptionsToken(name?) → 'NATS_OPTIONS:<name>'`; `NATS_FEATURE_TOKEN_PREFIX = 'NATS_FEATURE:'`
  - `type StreamSyncMode = 'create-or-update' | 'create-only' | 'verify' | 'none'`
  - `type StreamDeclaration = WithRequired<Partial<StreamConfig>, 'name'>` (tự định nghĩa `WithRequired`)
  - `interface ConsumerDeclaration { stream: string; config: Partial<ConsumerConfig> & { durable_name: string } }`
  - `interface NatsModuleOptions { name?: string; connection: NodeConnectionOptions; jetstream?: { options?: JetStreamOptions; sync?: StreamSyncMode }; streams?: StreamDeclaration[]; consumers?: ConsumerDeclaration[]; codec?: Codec; plugins?: NatsPlugin[]; shutdown?: { drainTimeout?: number }; logger?: false | LoggerService }` (`NatsPlugin` khai báo trong `src/pipeline/types.ts` ở Task 3; Task 2 tạo file đó với `export interface NatsPlugin { name: string }` tạm thời, Task 3/6 mở rộng)
  - `interface NatsFeatureOptions { connection?: string; streams?: StreamDeclaration[]; consumers?: ConsumerDeclaration[] }`
  - `createNatsLogger(option: false | LoggerService | undefined, context = 'NatsModule'): LoggerService` — `false` → logger no-op; `undefined` → `new Logger(context)`
  - `redactServers(servers: string | string[] | undefined): string[]` — `nats://u:p@h:4222` → `nats://***@h:4222`; `undefined` → `['nats://127.0.0.1:4222']`
  - `interface ConnectionEntry { readonly name: string; readonly options: NatsModuleOptions; readonly nc: NatsConnection; readonly logger: LoggerService; js(): JetStreamClient; jsm(): Promise<JetStreamManager> }` (`js`/`jsm` lazy + cache, dùng `options.jetstream?.options`)
  - `type NatsConnectionState = 'connected' | 'reconnecting' | 'draining' | 'closed'`
  - `interface NatsStatusEvent { connection: string; type: Status['type']; data?: unknown }`
  - `@Injectable() class NatsConnectionManager`:
    - `connect(name: string, options: NatsModuleOptions): Promise<ConnectionEntry>` — tên trùng → `NatsConfigError`; connect lỗi → `NatsConnectionError` (message: `NATS connection "<name>" failed to connect to <redacted servers>: <err.message>`, `cause` = lỗi gốc)
    - `get(name = DEFAULT_CONNECTION): ConnectionEntry` — không có → `NatsConfigError('Unknown NATS connection "<name>"')`
    - `has(name): boolean`; `names(): string[]`
    - `status(name?): { name: string; state: NatsConnectionState; server?: string }`
    - `status$(name?): Observable<NatsStatusEvent>` — `Subject` nuôi từ `for await (const s of nc.status())`; log mỗi event (`disconnect`/`reconnect`/`ldm`/`error`/`staleConnection`) qua logger của entry
    - `closeAll(): Promise<void>` — `drain()` mọi connection song song, bỏ qua connection đã đóng

- [ ] **Step 1: Write the test support + failing tests**

`test/integration/support/nats-server.ts`:
```ts
export interface NatsTestServer { url: string; stop(): Promise<void> }
export function startNats(args?: string[]): Promise<NatsTestServer>
// GenericContainer('nats:2.12-alpine').withCommand(['-js', ...args]).withExposedPorts(4222)
//   .withWaitStrategy(Wait.forLogMessage(/Server is ready/)); url = `nats://${host}:${mappedPort}`
```

```ts
// test/unit/redact.spec.ts
it('redacts userinfo', () => expect(redactServers('nats://u:secret@h:4222')).toEqual(['nats://***@h:4222']));
it('keeps plain urls', () => expect(redactServers(['nats://h:1'])).toEqual(['nats://h:1']));

// test/integration/connection-manager.int.spec.ts
it('connects and reports connected status', async () => {
  const e = await mgr.connect('default', { connection: { servers: srv.url } });
  expect(mgr.status().state).toBe('connected'); expect(e.nc.isClosed()).toBe(false);
});
it('rejects duplicate names', async () => {
  await expect(mgr.connect('default', { connection: { servers: srv.url } })).rejects.toThrow(NatsConfigError);
});
it('fails fast on bad credentials without leaking the password', async () => {
  const auth = await startNats(['--user', 'app', '--pass', 's3cr3t']);
  const p = mgr.connect('bad', { connection: { servers: auth.url, user: 'app', pass: 'wrong-s3cr3t' } });
  await expect(p).rejects.toThrow(NatsConnectionError);
  await expect(p).rejects.not.toThrow(/s3cr3t/);
});
it('throws NatsConfigError for unknown connection', () => expect(() => mgr.get('nope')).toThrow(NatsConfigError));
it('closeAll drains every connection', async () => { await mgr.closeAll(); expect(mgr.status().state).toBe('closed'); });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- redact && npm run test:int -- connection-manager`
Expected: FAIL — module chưa tồn tại.

- [ ] **Step 3: Implement** `options.ts`, `tokens.ts`, `logger.ts`, `redact.ts`, `connection-manager.ts` theo Interfaces. `connect` dùng `connect()` của `@nats-io/transport-node` với `options.connection` nguyên vẹn (mặc định client không chờ lần connect đầu → lỗi ném ngay).

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int -- connection-manager`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: connection manager with fail-fast connect and redaction"
```

---

### Task 3: Outbound pipeline, `NatsClient`, `NatsModule`

**Files:**
- Create: `src/pipeline/types.ts` (mở rộng), `src/pipeline/compose.ts`, `src/client/nats-client.ts`, `src/module/nats-core.module.ts`, `src/module/nats.module.ts`, `src/module/inject.decorators.ts`
- Create: `test/integration/support/app.ts`
- Test: `test/unit/compose.spec.ts`, `test/integration/nats-client.int.spec.ts`, `test/integration/nats-module.int.spec.ts`

**Interfaces:**
- Consumes: Task 1, Task 2
- Produces:
  - `type Next = () => Promise<void>`
  - `compose<T>(middlewares: ReadonlyArray<(ctx: T, next: Next) => Promise<void>>, terminal: (ctx: T) => Promise<void>): (ctx: T) => Promise<void>` — gọi `next()` hai lần trong một middleware → reject `NatsModuleError('next() called multiple times')`
  - `interface OutgoingMessage { kind: 'publish' | 'request' | 'jetstream'; connection: string; subject: string; data: unknown; headers: MsgHdrs; codec: Codec }`
  - `interface OutboundMiddleware { handle(m: OutgoingMessage, next: Next): Promise<void> }`
  - `interface NatsPlugin { name: string; inbound?: InboundMiddleware; outbound?: OutboundMiddleware }` (`InboundMiddleware` khai báo ở Task 6; ở task này khai báo trước `interface InboundMiddleware { stage?: 'outer' | 'inner'; handle(d: Delivery, next: Next): Promise<void> }` với `Delivery` là `unknown` tạm, Task 6 thay type)
  - `class NatsClient` (không `@Injectable`, tạo bằng factory): `constructor(entry: ConnectionEntry)`; `publish(subject: string, data?: unknown, opts?: { headers?: HeadersInit; codec?: Codec; reply?: string }): void`; `request<T = unknown>(subject: string, data?: unknown, opts?: { timeout?: number; headers?: HeadersInit; codec?: Codec }): Promise<T>`; `flush(): Promise<void>`
    - Luồng: tạo `OutgoingMessage` (codec = `opts.codec ?? options.codec ?? jsonCodec()`, headers = `toMsgHdrs(opts.headers)`) → chạy `outbound` của các plugin theo thứ tự → terminal encode + gửi.
    - `publish` trả `void` nhưng pipeline là async: lỗi pipeline được log, không ném ra (fire-and-forget). `flush()` chờ các publish đang chạy qua pipeline rồi `nc.flush()`.
    - `request`: timeout mặc định `5000`; `TimeoutError` → `NatsRequestError('timeout')`; `NoRespondersError` (kiểm tra cả `err` và `err.cause`) → `'no_responders'`; reply có header `Nats-Service-Error-Code` → `'service_error'` với `serviceCode = Number(code)`, `description` = header `Nats-Service-Error`; ngược lại `decodePayload`.
  - `NatsModule.forRoot(options: NatsModuleOptions): DynamicModule`, `NatsModule.forRootAsync(options: NatsModuleAsyncOptions): DynamicModule`, `NatsModule.forFeature(options: NatsFeatureOptions): DynamicModule`
    - `interface NatsModuleAsyncOptions { name?: string; imports?: ModuleMetadata['imports']; inject?: InjectionToken[]; useFactory?: (...args: any[]) => Omit<NatsModuleOptions, 'name'> | Promise<Omit<NatsModuleOptions, 'name'>>; useClass?: Type<NatsOptionsFactory>; useExisting?: Type<NatsOptionsFactory> }`, `interface NatsOptionsFactory { createNatsOptions(): Omit<NatsModuleOptions, 'name'> | Promise<...> }`
    - `forRoot`/`forRootAsync` trả `{ module: NatsModule, global: true, imports: [NatsCoreModule], providers, exports }` với providers cho tên đó: options token; connection token (`useFactory: (mgr, opts) => mgr.connect(name, opts).then(e => e.nc)`); client token (`new NatsClient(mgr.get(name))`, inject connection token để đảm bảo thứ tự); JetStream client & stream manager token (Task 4 thêm).
    - `NatsCoreModule` (`@Global`, import `DiscoveryModule`) cung cấp và export `NatsConnectionManager`; Task 4/7 thêm `NatsLifecycle`.
    - `forFeature` trả module (không global) với một value provider token `NATS_FEATURE:<randomUUID()>` = `{ connection: DEFAULT_CONNECTION, ...options }`.
  - `InjectNatsClient(name?)`, `InjectNatsConnection(name?)`, `InjectJetStream(name?)`, `InjectStreamManager(name?)` = `Inject(get…Token(name))`
  - `test/integration/support/app.ts`: `createTestApp(metadata: ModuleMetadata): Promise<TestingModule>` — `Test.createTestingModule(metadata).compile()` rồi `init()`; gọi `close()` để chạy shutdown hooks.

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/compose.spec.ts
it('runs middlewares in onion order', async () => {
  const log: string[] = [];
  const mw = (n: string) => async (_: unknown, next: Next) => { log.push(`>${n}`); await next(); log.push(`<${n}`); };
  await compose([mw('a'), mw('b')], async () => { log.push('t'); })({});
  expect(log).toEqual(['>a', '>b', 't', '<b', '<a']);
});
it('rejects when next is called twice', async () => {
  const run = compose([async (_: unknown, n: Next) => { await n(); await n(); }], async () => {});
  await expect(run({})).rejects.toThrow('next() called multiple times');
});

// test/integration/nats-client.int.spec.ts — app: NatsModule.forRoot({ connection: { servers: url }, plugins: [headerPlugin] })
// headerPlugin.outbound đặt header 'x-plugin: 1'
it('publishes JSON with plugin headers', async () => {
  const sub = rawNc.subscribe('t.pub', { max: 1 });
  client.publish('t.pub', { a: 1 }, { headers: { 'x-h': 'v' } });
  const m = await first(sub);
  expect(m.json()).toEqual({ a: 1 }); expect(m.headers?.get('x-h')).toBe('v'); expect(m.headers?.get('x-plugin')).toBe('1');
});
it('request returns decoded reply', async () => { /* rawNc responder trả {ok:true} */ expect(await client.request('t.req', {})).toEqual({ ok: true }); });
it('maps timeout', async () => {
  rawNc.subscribe('t.silent');
  await expect(client.request('t.silent', {}, { timeout: 200 })).rejects.toMatchObject({ code: 'timeout' });
});
it('maps no responders', async () => await expect(client.request('t.none', {})).rejects.toMatchObject({ code: 'no_responders' }));
it('maps service errors', async () => {
  /* responder: m.respond(empty, { headers: headers(...) với Nats-Service-Error-Code: 404, Nats-Service-Error: 'not found' }) */
  await expect(client.request('t.err', {})).rejects.toMatchObject({ code: 'service_error', serviceCode: 404, description: 'not found' });
});

// test/integration/nats-module.int.spec.ts
it('registers two named connections', async () => {
  const app = await createTestApp({ imports: [NatsModule.forRoot({ connection: { servers: url } }),
    NatsModule.forRoot({ name: 'b', connection: { servers: url } })] });
  expect(app.get(getNatsClientToken())).toBeInstanceOf(NatsClient);
  expect(app.get(getNatsClientToken('b'))).not.toBe(app.get(getNatsClientToken()));
});
it('forRootAsync uses the factory', async () => { /* useFactory: () => ({ connection: { servers: url } }) → client resolve được */ });
it('fails bootstrap on duplicate name', async () => {
  // options phải khác nhau (servers: url vs [url]) để Nest không gộp hai dynamic module giống hệt nhau
  await expect(createTestApp({ imports: [NatsModule.forRoot({ connection: { servers: url } }),
    NatsModule.forRoot({ connection: { servers: [url] } })] })).rejects.toThrow(NatsConfigError);
});
it('close() drains connections', async () => { /* sau app.close(): mgr.status().state === 'closed' */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- compose && npm run test:int -- nats-client nats-module`
Expected: FAIL — module chưa tồn tại.

- [ ] **Step 3: Implement** `compose.ts`, `types.ts`, `nats-client.ts`, `nats-core.module.ts`, `nats.module.ts`, `inject.decorators.ts`. `NatsCoreModule` tạm thời có một provider `onApplicationShutdown → mgr.closeAll()` (Task 7 chuyển vào `NatsLifecycle`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int -- nats-client nats-module`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: NatsModule, NatsClient and outbound plugin pipeline"
```

---

### Task 4: Stream/consumer sync, `NatsStreamManager`, `JetStreamClient.publish`

**Files:**
- Create: `src/jetstream/stream-sync.ts`, `src/jetstream/stream-manager.ts`, `src/jetstream/jetstream-client.ts`, `src/module/nats-lifecycle.ts`
- Modify: `src/module/nats.module.ts` (thêm provider JetStream client + stream manager), `src/module/nats-core.module.ts` (thêm `NatsLifecycle`)
- Test: `test/unit/stream-sync.spec.ts`, `test/integration/jetstream.int.spec.ts`

**Interfaces:**
- Consumes: Task 1–3
- Produces:
  - `STREAM_IMMUTABLE_FIELDS = ['storage', 'retention'] as const`
  - `CONSUMER_IMMUTABLE_FIELDS = ['ack_policy', 'deliver_policy', 'opt_start_seq', 'opt_start_time', 'replay_policy'] as const`
  - `interface ConfigDiff { field: string; current: unknown; declared: unknown; immutable: boolean }`
  - `diffConfig(current: object, declared: object, immutable: readonly string[]): ConfigDiff[]` — chỉ so các key có trong `declared`, so sánh deep-equal (mảng giữ thứ tự)
  - `withConsumerDefaults(cfg: Partial<ConsumerConfig>): Partial<ConsumerConfig>` — nếu thiếu: `ack_policy: AckPolicy.Explicit`, `max_deliver: -1`
  - `class NatsStreamManager` — `constructor(entry: ConnectionEntry)`; `ensureStream(cfg: StreamDeclaration, mode?: StreamSyncMode): Promise<StreamInfo | undefined>`; `ensureConsumer(stream: string, cfg: ConsumerDeclaration['config'], mode?: StreamSyncMode): Promise<ConsumerInfo | undefined>`; `info(stream): Promise<StreamInfo>`; `purge(stream, opts?: PurgeOpts): Promise<PurgeResponse>`; `delete(stream): Promise<boolean>`; `jsm(): Promise<JetStreamManager>`
    - Hành vi theo mode (mặc định `options.jetstream?.sync ?? 'create-or-update'`), giống nhau cho stream và consumer:
      | mode | thiếu | có, lệch mutable | có, lệch immutable |
      |---|---|---|---|
      | `create-or-update` | tạo | `update` | `StreamConfigConflictError` |
      | `create-only` | tạo | không đụng | không đụng |
      | `verify` | `NatsConfigError` | `NatsConfigError` (liệt kê field) | `NatsConfigError` |
      | `none` | trả `undefined`, không gọi server | | |
    - "Thiếu" = lỗi stream/consumer not found từ JSM (kiểm tra `err.code === '404'` hoặc `api_error.err_code` 10059 stream / 10014 consumer).
    - Không bao giờ gọi `delete` trong quá trình sync.
  - `class JetStreamClient` — `constructor(entry: ConnectionEntry)`; `publish(subject: string, data?: unknown, opts?: { msgId?: string; expect?: Partial<StreamExpectations>; headers?: HeadersInit; codec?: Codec; timeout?: number }): Promise<PubAck>` — chạy outbound pipeline với `kind: 'jetstream'`; map `msgId → msgID`.
  - `@Injectable() class NatsLifecycle implements OnModuleInit` — `onModuleInit()`: với mỗi connection trong `mgr.names()`, gom `streams`/`consumers` từ `entry.options` và mọi feature provider (lấy qua `DiscoveryService.getProviders()` lọc `wrapper.token` bắt đầu bằng `NATS_FEATURE_TOKEN_PREFIX`, `wrapper.instance` là `NatsFeatureOptions`, connection không tồn tại → `NatsConfigError`), rồi `ensureStream` tất cả trước, `ensureConsumer` sau.

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/stream-sync.spec.ts
it('only compares declared keys', () =>
  expect(diffConfig({ name: 'S', max_msgs: 10, replicas: 1 }, { name: 'S', max_msgs: 10 }, STREAM_IMMUTABLE_FIELDS)).toEqual([]));
it('flags immutable fields', () =>
  expect(diffConfig({ storage: 'file' }, { storage: 'memory' }, STREAM_IMMUTABLE_FIELDS))
    .toEqual([{ field: 'storage', current: 'file', declared: 'memory', immutable: true }]));
it('compares arrays deeply', () =>
  expect(diffConfig({ subjects: ['a'] }, { subjects: ['a', 'b'] }, STREAM_IMMUTABLE_FIELDS)[0].field).toBe('subjects'));
it('defaults consumer to explicit ack and unlimited deliveries', () =>
  expect(withConsumerDefaults({ durable_name: 'd' })).toMatchObject({ ack_policy: 'explicit', max_deliver: -1 }));
it('keeps user max_deliver', () => expect(withConsumerDefaults({ max_deliver: 3 }).max_deliver).toBe(3));

// test/integration/jetstream.int.spec.ts (mỗi test dùng tên stream riêng)
it('creates declared streams and consumers on init', async () => {
  const app = await createTestApp({ imports: [NatsModule.forRoot({ connection: { servers: url },
    streams: [{ name: 'S1', subjects: ['s1.>'] }], consumers: [{ stream: 'S1', config: { durable_name: 'c1' } }] })] });
  const jsm = await app.get(getStreamManagerToken()).jsm();
  expect((await jsm.streams.info('S1')).config.subjects).toEqual(['s1.>']);
  expect((await jsm.consumers.info('S1', 'c1')).config.max_deliver).toBe(-1);
});
it('picks up forFeature declarations', async () => { /* forFeature({ streams: [{ name: 'S2', subjects: ['s2.>'] }] }) → stream tồn tại */ });
it('updates mutable fields', async () => { /* tạo S3 max_msgs 10, ensureStream max_msgs 20 → info.config.max_msgs === 20 */ });
it('throws on immutable conflict without deleting', async () => {
  /* tạo S4 storage file; ensureStream({ name: 'S4', subjects, storage: 'memory' }) */
  await expect(p).rejects.toMatchObject({ name: 'StreamConfigConflictError', field: 'storage', current: 'file', declared: 'memory' });
  expect((await jsm.streams.info('S4')).config.storage).toBe('file');
});
it('verify mode rejects missing stream', async () =>
  await expect(sm.ensureStream({ name: 'S5', subjects: ['s5'] }, 'verify')).rejects.toThrow(NatsConfigError));
it('create-only leaves existing stream untouched', async () => { /* S6 max_msgs 10 → ensure create-only max_msgs 20 → vẫn 10 */ });
it('none does not touch the server', async () => expect(await sm.ensureStream({ name: 'S7', subjects: ['s7'] }, 'none')).toBeUndefined());
it('publish returns PubAck and dedupes by msgId', async () => {
  const a = await js.publish('s1.x', { a: 1 }, { msgId: 'm1' });
  const b = await js.publish('s1.x', { a: 1 }, { msgId: 'm1' });
  expect(a.stream).toBe('S1'); expect(b.duplicate).toBe(true);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- stream-sync && npm run test:int -- jetstream`
Expected: FAIL.

- [ ] **Step 3: Implement** `stream-sync.ts`, `stream-manager.ts`, `jetstream-client.ts`, `nats-lifecycle.ts`; đăng ký provider `getJetStreamClientToken(name)` / `getStreamManagerToken(name)` trong `forRoot`/`forRootAsync`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int -- jetstream`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: declarative stream/consumer sync and JetStream publish"
```

---

### Task 5: Handler & param decorators, `NatsContext`

**Files:**
- Create: `src/decorators/constants.ts`, `src/decorators/handler.decorators.ts`, `src/decorators/param.decorators.ts`, `src/context/nats-context.ts`
- Test: `test/unit/decorators.spec.ts`, `test/unit/nats-context.spec.ts`

**Interfaces:**
- Consumes: `Codec`, `HeadersInit`, `fromMsgHdrs` (Task 1)
- Produces:
  - Metadata keys (string): `NATS_SUBSCRIBE_METADATA = 'nats:subscribe'`, `NATS_CONSUME_METADATA = 'nats:consume'`, `NATS_SERVICE_METADATA = 'nats:service'`, `NATS_ENDPOINT_METADATA = 'nats:endpoint'`
  - `interface SubscribeOptions { connection?: string; queue?: string; codec?: Codec; concurrency?: number }`
  - `interface ConsumeOptions { connection?: string; stream: string; durable: string; config?: Partial<ConsumerConfig>; ack?: 'auto' | 'manual'; codec?: Codec; concurrency?: number }`
  - `interface NatsServiceOptions { name: string; version: string; description?: string; queue?: string; metadata?: Record<string, string>; connection?: string }`
  - `interface ServiceEndpointOptions { subject?: string; queue?: string; metadata?: Record<string, string>; codec?: Codec }`
  - `Subscribe(subject: string, options?: SubscribeOptions): MethodDecorator` → metadata `{ subject, options }`
  - `Consume(options: ConsumeOptions): MethodDecorator` → metadata `{ options }`
  - `NatsService(options: NatsServiceOptions): ClassDecorator`
  - `ServiceEndpoint(name: string, options?: ServiceEndpointOptions): MethodDecorator` → metadata `{ name, options }`
  - `enum NatsParamType { PAYLOAD = 3, CONTEXT = 6, SUBJECT = 7, MSG = 8, HEADERS = 10, CLOUD_EVENT = 11 }` (PAYLOAD/CONTEXT khớp `RpcParamtype` của `@nestjs/microservices`; tránh 4/5 và 9 = `GRPC_CALL`)
    (Nest maps param type 3/4/5 to pipe type `'body'`/`'query'`/`'param'` and everything else to `'custom'`, which `ValidationPipe` skips; `PAYLOAD = 3` like Nest's `RpcParamtype.PAYLOAD` so payloads get validated, other types avoid 4/5 so context/headers never do.)
  - `Payload(property?: string, ...pipes: (Type<PipeTransform> | PipeTransform)[]): ParameterDecorator` và `Payload(...pipes)`; `Headers(name?: string)`, `Subject()`, `Msg()`, `Ctx()` — ghi vào `ROUTE_ARGS_METADATA` (từ `@nestjs/common`) với key `` `${type}:${index}` `` và value `{ index, data, pipes }` (cùng shape Nest dùng)
  - `class NatsContext` — `constructor(init: { kind: 'core' | 'jetstream' | 'service'; connection: string; raw: Msg | JsMsg })`; getter `kind`, `connection`, `subject` (= `raw.subject`), `headers` (`fromMsgHdrs(raw.headers)`, tính một lần), `replyTo` (`raw.reply` hoặc `undefined`), `raw`
  - `class NatsJetStreamContext extends NatsContext` — `raw: JsMsg`; getter `stream`, `seq`, `deliveryCount` (từ `raw.info`); `ack()`, `nak(delayMs?: number)`, `term(reason?: string)`, `working()`; getter `settled: boolean` — `true` sau `ack`/`nak`/`term` đầu tiên; gọi settle lần hai là no-op (không gọi xuống `raw`)

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/decorators.spec.ts
class H {
  @Subscribe('a.*', { queue: 'q' }) onA(@Payload() p: unknown, @Headers('x') x: string, @Subject() s: string, @Ctx() c: unknown, @Msg() m: unknown) {}
  @Consume({ stream: 'S', durable: 'd' }) onC(@Payload('id', ParseIntPipe) id: number) {}
}
it('stores subscribe metadata', () =>
  expect(Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, H.prototype.onA)).toEqual({ subject: 'a.*', options: { queue: 'q' } }));
it('stores params under ROUTE_ARGS_METADATA', () => {
  const meta = Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onA');
  expect(meta[`${NatsParamType.PAYLOAD}:0`]).toEqual({ index: 0, data: undefined, pipes: [] });
  expect(meta[`${NatsParamType.HEADERS}:1`].data).toBe('x');
});
it('supports property + pipes on Payload', () =>
  expect(Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onC')[`${NatsParamType.PAYLOAD}:0`]).toEqual({ index: 0, data: 'id', pipes: [ParseIntPipe] }));

// test/unit/nats-context.spec.ts — JsMsg giả là object có ack/nak/term/working = vi.fn(), info { stream: 'S', streamSequence: 7, deliveryCount: 2 }
it('exposes delivery info', () => expect([ctx.stream, ctx.seq, ctx.deliveryCount]).toEqual(['S', 7, 2]));
it('settles once', () => { ctx.ack(); ctx.nak(); expect(ctx.settled).toBe(true); expect(raw.ack).toHaveBeenCalledTimes(1); expect(raw.nak).not.toHaveBeenCalled(); });
it('passes nak delay through', () => { ctx.nak(500); expect(raw.nak).toHaveBeenCalledWith(500); });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- decorators nats-context`
Expected: FAIL.

- [ ] **Step 3: Implement** bốn file theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: handler/param decorators and NatsContext"
```

---

### Task 6: Inbound pipeline, invoker, Nest enhancers, reply mapping

**Files:**
- Create: `src/pipeline/delivery.ts`, `src/pipeline/invoker.ts`, `src/pipeline/basic-settlement.ts`, `src/pipeline/reply.ts`, `src/discovery/nats-params-factory.ts`, `src/discovery/handler-explorer.ts`
- Modify: `src/pipeline/types.ts` (thay `Delivery` tạm bằng type thật)
- Test: `test/unit/reply.spec.ts`, `test/unit/basic-settlement.spec.ts`, `test/unit/pipeline.spec.ts`, `test/unit/enhancers.spec.ts`

**Interfaces:**
- Consumes: Task 1–5
- Produces:
  - `type HandlerKind = 'subscribe' | 'consume' | 'service-endpoint'`
  - `type HandlerDefinition = { kind: 'subscribe'; subject: string; options: SubscribeOptions } | { kind: 'consume'; options: ConsumeOptions } | { kind: 'service-endpoint'; endpoint: string; options: ServiceEndpointOptions; service: NatsServiceOptions }`, cộng chung `{ connection: string; className: string; methodName: string; instance: object; wrapper: InstanceWrapper; invoke: (payload: unknown, ctx: NatsContext) => Promise<unknown> }`
  - `interface InboundMessage { subject: string; data: Uint8Array; headers?: MsgHdrs; raw: Msg | JsMsg }`
  - `interface Delivery { readonly handler: HandlerDefinition; readonly messages: InboundMessage[]; readonly context: NatsContext; readonly codec: Codec; readonly logger: LoggerService; result?: unknown }`
  - `buildInboundPipeline(parts: { plugins: NatsPlugin[]; core: InboundMiddleware[] }, terminal: (d: Delivery) => Promise<void>): (d: Delivery) => Promise<void>` — thứ tự: plugin `stage === 'outer'` → `core` → plugin `stage` `'inner'`/không đặt → terminal
  - `invokeHandler(d: Delivery): Promise<void>` (terminal) — `payload = decodePayload(d.codec, msg.data, msg.headers)` với `msg = d.messages[0]`; `result = await d.handler.invoke(payload, d.context)`; nếu `result` là Observable → `lastValueFrom`; gán `d.result`. Throw đồng bộ hay reject đều thành promise bị reject.
  - `toServiceError(err: unknown): { code: number; description: string }` — `HttpException` → `getStatus()` + `message`; `NatsDecodeError` → `400` + `'Bad Request'`; khác → `500` + `'Internal Server Error'`
  - `isReplyDelivery(d: Delivery): boolean` — `d.handler.kind === 'service-endpoint'` hoặc (`'subscribe'` và `d.context.replyTo` có giá trị)
  - `basicSettlement: InboundMiddleware` (Plan 2 thay thế) — bọc `next()`:
    | handler | kết quả | hành động |
    |---|---|---|
    | consume, `ack: 'auto'` (mặc định) | thành công | `ctx.ack()` nếu chưa settle |
    | consume | lỗi `NatsDecodeError` / `NonRetryableError` / `HttpException` 4xx | `ctx.term(err.message)` nếu chưa settle; log error |
    | consume | lỗi khác | `ctx.nak()` nếu chưa settle; log error |
    | consume, `ack: 'manual'` | thành công | không làm gì |
    | reply delivery | lỗi | rethrow (adapter reply lỗi) |
    | subscribe không reply | lỗi | log error, nuốt lỗi |
  - `class NatsParamsFactory implements ParamsFactory` — `exchangeKeyForValue(type, data, [payload, ctx])`: `PAYLOAD` → `data ? payload?.[data] : payload`; `HEADERS` → `data ? ctx.headers[data] : ctx.headers`; `SUBJECT` → `ctx.subject`; `MSG` → `ctx.raw`; `CONTEXT` → `ctx`; khác → `undefined`
  - `@Injectable() class HandlerExplorer` — `constructor(discovery: DiscoveryService, scanner: MetadataScanner, ecc: ExternalContextCreator)`; `explore(): HandlerDefinition[]` — duyệt `getProviders()` + `getControllers()` có `instance` và `metatype`; method có metadata `@Subscribe`/`@Consume`, hoặc class `@NatsService` + method `@ServiceEndpoint`; `connection = options.connection ?? service.connection ?? DEFAULT_CONNECTION`; `invoke = ecc.create(instance, instance[method], method, ROUTE_ARGS_METADATA, new NatsParamsFactory(), undefined, undefined, { guards: true, interceptors: true, filters: true }, 'nats')`. Provider scope REQUEST/TRANSIENT (`!wrapper.isDependencyTreeStatic()` hoặc `wrapper.isTransient`) **không** bị bỏ qua: trả về với `instance` hiện có để validator (Task 7) báo lỗi.

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/reply.spec.ts
it('maps HttpException', () => expect(toServiceError(new NotFoundException('gone'))).toEqual({ code: 404, description: 'gone' }));
it('maps decode errors to 400', () => expect(toServiceError(new NatsDecodeError('x'))).toEqual({ code: 400, description: 'Bad Request' }));
it('hides internal errors', () => expect(toServiceError(new Error('db password=…'))).toEqual({ code: 500, description: 'Internal Server Error' }));

// test/unit/basic-settlement.spec.ts — Delivery giả với NatsJetStreamContext trên JsMsg giả
it.each([
  ['auto', undefined, 'ack'], ['auto', new Error('x'), 'nak'], ['auto', new NatsDecodeError('x'), 'term'],
  ['auto', new BadRequestException(), 'term'], ['manual', undefined, null], ['manual', new Error('x'), 'nak'],
])('consume ack=%s error=%s → %s', async (ack, err, expected) => { /* chạy basicSettlement.handle(d, next ném err) và kiểm tra raw.ack/nak/term */ });
it('rethrows for reply deliveries', async () => { /* subscribe + replyTo set → reject với lỗi gốc */ });
it('swallows for plain subscriptions', async () => { /* subscribe, không replyTo → resolve, logger.error được gọi */ });

// test/unit/pipeline.spec.ts
it('orders outer plugins, core, inner plugins, terminal', async () => { /* log thứ tự như compose.spec */ });
it('treats synchronous throws like rejections', async () => {
  const d = delivery({ invoke: () => { throw new Error('sync'); } });
  await expect(invokeHandler(d)).rejects.toThrow('sync');
});
it('unwraps observable results', async () => { /* invoke trả of(5) → d.result === 5 */ });

// test/unit/enhancers.spec.ts — TestingModule thật (không cần NATS) với HandlerExplorer
// Provider: @UseGuards(G) @UseInterceptors(I) class H { @Subscribe('x') on(@Payload(new ValidationPipe()) dto: Dto, @Tenant() t: string) { return { dto, t }; } }
// Tenant = createParamDecorator((_, ctx: ExecutionContext) => ctx.switchToRpc().getContext<NatsContext>().headers['x-tenant'])
it('runs pipes, guards, interceptors and custom param decorators', async () => {
  const [def] = explorer.explore();
  const out = await def.invoke({ name: 'a' }, natsCtx({ 'x-tenant': 't1' }));
  expect(out).toEqual({ dto: expect.objectContaining({ name: 'a' }), t: 't1' });
  expect(guardSawType).toBe('nats'); expect(interceptorCalled).toBe(true);
});
it('rejects invalid payload via ValidationPipe', async () => await expect(def.invoke({}, ctx)).rejects.toBeInstanceOf(BadRequestException));
it('lets exception filters swallow errors', async () => { /* @UseFilters(F) với F trả 'handled' → invoke resolve 'handled' */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- reply basic-settlement pipeline enhancers`
Expected: FAIL.

- [ ] **Step 3: Implement** các file theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: inbound pipeline, invoker with Nest enhancers, reply mapping"
```

---

### Task 7: Validation, core subscription adapter, lifecycle start/stop

**Files:**
- Create: `src/discovery/handler-validator.ts`, `src/adapters/limiter.ts`, `src/adapters/adapter.ts`, `src/adapters/core-subscription.adapter.ts`
- Modify: `src/module/nats-lifecycle.ts`, `src/module/nats-core.module.ts` (bỏ provider shutdown tạm của Task 3)
- Test: `test/unit/limiter.spec.ts`, `test/unit/handler-validator.spec.ts`, `test/integration/subscribe.int.spec.ts`

**Interfaces:**
- Consumes: Task 1–6
- Produces:
  - `interface Limiter { acquire(): Promise<void>; release(): void; idle(): Promise<void>; readonly active: number }`; `createLimiter(concurrency: number): Limiter` — `concurrency < 1` → `NatsConfigError`
  - `validateHandlers(defs: HandlerDefinition[], mgr: NatsConnectionManager): void` — ném `NatsConfigError` (message nêu `Class.method`) khi: connection không tồn tại; hai handler `consume` cùng `connection + stream + durable`; provider scope REQUEST/TRANSIENT; `@ServiceEndpoint` trong class không có `@NatsService`; `concurrency < 1`
  - `interface HandlerAdapter { start(): Promise<void>; stop(): Promise<void>; idle(): Promise<void> }` — `stop()` ngừng nhận message mới; `idle()` resolve khi không còn message đang xử lý
  - `class CoreSubscriptionAdapter implements HandlerAdapter` — `constructor(def: HandlerDefinition & { kind: 'subscribe' }, entry: ConnectionEntry)`; `start()`: `nc.subscribe(subject, { queue })`, vòng `for await` với `limiter.acquire()` trước mỗi message (concurrency mặc định `1`); mỗi message: tạo `NatsContext({ kind: 'core' })`, `Delivery` (codec = `options.codec ?? entry.options.codec ?? jsonCodec()`), chạy pipeline (`core: [basicSettlement]`); thành công + `replyTo` + `d.result !== undefined` → `msg.respond(encodePayload(codec, result))`; pipeline reject + `replyTo` → `msg.respond(empty, { headers })` với `Nats-Service-Error` / `Nats-Service-Error-Code` từ `toServiceError`; lỗi khác log. `stop()`: `sub.drain()`.
  - `NatsLifecycle` thêm: `onApplicationBootstrap()` — `explore()` → `validateHandlers()` → tạo adapter theo `kind` (Task 8/9 thêm `consume`/`service-endpoint`) → `start()` tuần tự; `beforeApplicationShutdown()` — `stop()` mọi adapter, chờ `Promise.race([all idle, sleep(drainTimeout ?? 10_000)])` (drainTimeout lấy max trên các connection), rồi `mgr.closeAll()`

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/limiter.spec.ts
it('caps concurrency', async () => { const l = createLimiter(2); await l.acquire(); await l.acquire();
  let third = false; void l.acquire().then(() => { third = true; }); await tick(); expect(third).toBe(false);
  l.release(); await tick(); expect(third).toBe(true); });
it('idle resolves when all released', async () => { /* acquire, idle() pending, release → resolve */ });

// test/unit/handler-validator.spec.ts
it.each([
  ['unknown connection', def({ connection: 'x' })],
  ['duplicate durable', [consumeDef('S', 'd'), consumeDef('S', 'd')]],
  ['request scoped provider', def({ requestScoped: true })],
  ['concurrency 0', def({ options: { concurrency: 0 } })],
])('rejects %s', (_, defs) => expect(() => validateHandlers(toArray(defs), mgr)).toThrow(NatsConfigError));

// test/integration/subscribe.int.spec.ts — app với provider Handlers và NatsModule.forRoot
it('delivers decoded payload and wildcard subject', async () => {
  client.publish('orders.created', { id: 1 });
  expect(await handlers.received()).toEqual({ payload: { id: 1 }, subject: 'orders.created' }); // @Subscribe('orders.*')
});
it('replies with the return value', async () => expect(await client.request('orders.get', { id: 2 })).toEqual({ id: 2 }));
it('does not reply when handler returns undefined', async () =>
  await expect(client.request('orders.void', {}, { timeout: 300 })).rejects.toMatchObject({ code: 'timeout' }));
it('replies 400 for non-JSON payload', async () => {
  const m = await rawNc.request('orders.get', new TextEncoder().encode('hello'));
  expect(m.headers?.get('Nats-Service-Error-Code')).toBe('400');
});
it('replies with HttpException status', async () =>
  await expect(client.request('orders.missing', {})).rejects.toMatchObject({ code: 'service_error', serviceCode: 404 }));
it('keeps order with concurrency 1', async () => {
  for (let i = 0; i < 20; i++) client.publish('seq.in', i); // handler sleep ngẫu nhiên 0–10ms
  expect(await handlers.sequence(20)).toEqual([...Array(20).keys()]);
});
it('load-balances a queue group across two apps', async () => { /* 2 app cùng queue 'q', 20 message → tổng nhận 20, mỗi app > 0 */ });
it('fails bootstrap for unknown connection', async () =>
  await expect(createTestApp({ imports: [nats], providers: [BadHandlers] })).rejects.toThrow(NatsConfigError));
it('waits for in-flight handlers on close', async () => { /* handler chậm 300ms; publish rồi app.close() ngay → handler hoàn tất trước khi close resolve */ });
it('survives a throwing handler', async () => { /* handler ném; message tiếp theo vẫn được xử lý */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- limiter handler-validator && npm run test:int -- subscribe`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int`
Expected: PASS (bao gồm test của Task 2–4).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: @Subscribe adapter, handler validation, lifecycle start/stop"
```

---

### Task 8: JetStream consumer adapter (`@Consume`)

**Files:**
- Create: `src/adapters/jetstream-consumer.adapter.ts`
- Modify: `src/module/nats-lifecycle.ts`, `src/discovery/handler-validator.ts`
- Test: `test/integration/consume.int.spec.ts`

**Interfaces:**
- Consumes: Task 4 (`NatsStreamManager.ensureConsumer`, `withConsumerDefaults`), Task 5 (`NatsJetStreamContext`), Task 6, Task 7 (`Limiter`, `HandlerAdapter`)
- Produces:
  - `class JetStreamConsumerAdapter implements HandlerAdapter` — `constructor(def: HandlerDefinition & { kind: 'consume' }, entry: ConnectionEntry, streams: NatsStreamManager)`
    - `start()`: nếu `options.config` có → `streams.ensureConsumer(stream, { ...config, durable_name: durable })` (mode theo connection); rồi `js().consumers.get(stream, durable)`; không tồn tại → `NatsConfigError('Consumer "<stream>/<durable>" not found and no config declared for Class.method')`. Sau đó `consumer.consume({ max_messages: Math.max(2 * concurrency, 2) })`, vòng `for await` với `limiter.acquire()` trước mỗi message.
    - Mỗi message: `NatsJetStreamContext`, pipeline `core: [basicSettlement]`.
    - `stop()`: `messages.close()`; message đã nhận nhưng chưa bắt đầu xử lý → `nak()`.
  - Validator thêm: không cần gì mới (trùng durable đã có ở Task 7).

- [ ] **Step 1: Write failing tests**

```ts
// test/integration/consume.int.spec.ts — forRoot với streams [{ name: 'ORD', subjects: ['ord.>'] }]
it('creates the consumer from config and acks on success', async () => {
  await js.publish('ord.created', { id: 1 });
  expect(await handlers.received()).toEqual({ id: 1 });
  await eventually(async () => expect((await jsm.consumers.info('ORD', 'billing')).num_ack_pending).toBe(0));
  expect((await jsm.consumers.info('ORD', 'billing')).config.max_deliver).toBe(-1);
});
it('naks on error so the message is redelivered', async () => {
  // handler ném khi deliveryCount === 1
  await js.publish('ord.flaky', { id: 2 });
  expect(await handlers.deliveries('ord.flaky')).toEqual([1, 2]);
});
it('terms non-JSON payloads instead of redelivering', async () => {
  await js.publish('ord.created', new TextEncoder().encode('hello'));
  await sleep(1000); expect(handlers.receivedCount()).toBe(callsBefore); // handler không được gọi
  expect((await jsm.consumers.info('ORD', 'billing')).num_ack_pending).toBe(0);
});
it('respects manual ack', async () => { /* ack: 'manual', handler không ack → num_ack_pending === 1; handler gọi ctx.ack() ở message sau → 0 */ });
it('keeps order with concurrency 1', async () => { /* 20 message → thứ tự 0..19 */ });
it('processes in parallel with concurrency 4', async () => { /* handler sleep 200ms, 4 message → xong < 600ms */ });
it('fails bootstrap when consumer is missing and no config', async () =>
  await expect(createTestApp({ imports: [nats], providers: [MissingConsumerHandlers] })).rejects.toThrow(NatsConfigError));
it('fails bootstrap on duplicate durable', async () => { /* hai method cùng ORD/billing → NatsConfigError */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm run test:int -- consume`
Expected: FAIL.

- [ ] **Step 3: Implement** adapter và đăng ký trong `NatsLifecycle`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm run test:int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: @Consume JetStream pull consumer adapter"
```

---

### Task 9: Service endpoint adapter (`@NatsService` / `@ServiceEndpoint`)

**Files:**
- Create: `src/adapters/service-endpoint.adapter.ts`
- Modify: `src/module/nats-lifecycle.ts`
- Test: `test/integration/service.int.spec.ts`

**Interfaces:**
- Consumes: Task 6, Task 7
- Produces:
  - `class NatsServiceAdapter implements HandlerAdapter` — một adapter cho **mỗi class** `@NatsService` (gom các endpoint của class): `constructor(service: NatsServiceOptions, endpoints: Array<HandlerDefinition & { kind: 'service-endpoint' }>, entry: ConnectionEntry)`
    - `start()`: `new Svcm(nc).add({ name, version, description, metadata, queue })`; mỗi endpoint `service.addEndpoint(endpoint, { subject, queue, metadata })` (iterator), vòng `for await` với limiter riêng mỗi endpoint (concurrency `1`).
    - Mỗi message: `NatsContext({ kind: 'service' })`; pipeline `core: [basicSettlement]`; thành công → `msg.respond(encodePayload(codec, d.result))` (kể cả `undefined` → payload rỗng); lỗi → `msg.respondError(code, description)` từ `toServiceError`.
    - `stop()`: `service.stop()`.

- [ ] **Step 1: Write failing tests**

```ts
// test/integration/service.int.spec.ts
// @NatsService({ name: 'inventory', version: '1.0.0' }) class Inv {
//   @ServiceEndpoint('reserve', { subject: 'inv.reserve' }) reserve(@Payload() d: { sku: string }) { return { ok: d.sku }; }
//   @ServiceEndpoint('noop', { subject: 'inv.noop' }) noop() {}
//   @ServiceEndpoint('fail', { subject: 'inv.fail' }) fail() { throw new ConflictException('out of stock'); } }
it('responds with the return value', async () => expect(await client.request('inv.reserve', { sku: 'a' })).toEqual({ ok: 'a' }));
it('responds empty when handler returns undefined', async () => expect(await client.request('inv.noop', {})).toBeUndefined());
it('maps errors to respondError', async () =>
  await expect(client.request('inv.fail', {})).rejects.toMatchObject({ code: 'service_error', serviceCode: 409, description: 'out of stock' }));
it('is discoverable via the services API', async () => {
  const infos = await collect(await new Svcm(rawNc).client().info('inventory'));
  expect(infos[0].endpoints.map((e) => e.name).sort()).toEqual(['fail', 'noop', 'reserve']);
});
it('stops the service on close', async () => { /* sau app.close(): request 'inv.reserve' từ rawNc khác → no_responders */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm run test:int -- service`
Expected: FAIL.

- [ ] **Step 3: Implement** adapter và đăng ký trong `NatsLifecycle` (gom definitions theo `wrapper`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm run test:int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: NATS services endpoints via @NatsService/@ServiceEndpoint"
```

---

### Task 10: Public API, build, package checks

**Files:**
- Modify: `src/index.ts`
- Test: `test/unit/public-api.spec.ts`

**Interfaces:**
- Produces — `src/index.ts` export đúng các tên sau (không export các thứ nội bộ như `HandlerExplorer`, `NatsLifecycle`, adapter, `compose`):
  - Module: `NatsModule`, `NatsModuleOptions`, `NatsModuleAsyncOptions`, `NatsOptionsFactory`, `NatsFeatureOptions`, `StreamDeclaration`, `ConsumerDeclaration`, `StreamSyncMode`, `DEFAULT_CONNECTION`, `getConnectionToken`, `getNatsClientToken`, `getJetStreamClientToken`, `getStreamManagerToken`, `InjectNatsClient`, `InjectNatsConnection`, `InjectJetStream`, `InjectStreamManager`
  - Connection: `NatsConnectionManager`, `NatsConnectionState`, `NatsStatusEvent`
  - Client: `NatsClient`, `JetStreamClient`, `NatsStreamManager`
  - Decorators: `Subscribe`, `Consume`, `NatsService`, `ServiceEndpoint`, `Payload`, `Headers`, `Subject`, `Msg`, `Ctx`, `SubscribeOptions`, `ConsumeOptions`, `NatsServiceOptions`, `ServiceEndpointOptions`
  - Context: `NatsContext`, `NatsJetStreamContext`
  - Pipeline: `NatsPlugin`, `InboundMiddleware`, `OutboundMiddleware`, `OutgoingMessage`, `Delivery`, `InboundMessage`, `Next`
  - Codec: `Codec`, `jsonCodec`, `HeadersInit`
  - Errors: tất cả class ở Task 1 + `NatsRequestErrorCode`

- [ ] **Step 1: Write the failing test**

```ts
// test/unit/public-api.spec.ts
import * as api from '../../src/index.js';
it('exports the runtime API', () => expect(Object.keys(api).sort()).toEqual([
  'Consume', 'Ctx', 'DEFAULT_CONNECTION', 'Headers', 'InjectJetStream', 'InjectNatsClient', 'InjectNatsConnection',
  'InjectStreamManager', 'JetStreamClient', 'Msg', 'NatsClient', 'NatsConfigError', 'NatsConnectionError',
  'NatsConnectionManager', 'NatsContext', 'NatsDecodeError', 'NatsJetStreamContext', 'NatsModule', 'NatsModuleError',
  'NatsRequestError', 'NatsService', 'NatsStreamManager', 'NonRetryableError', 'Payload', 'ServiceEndpoint',
  'StreamConfigConflictError', 'Subject', 'Subscribe', 'getConnectionToken', 'getJetStreamClientToken',
  'getNatsClientToken', 'getStreamManagerToken', 'jsonCodec',
].sort()));
it('does not pull optional peers', async () => {
  const src = await readFile(new URL('../../src/index.ts', import.meta.url), 'utf8');
  expect(src).not.toMatch(/@opentelemetry|@nestjs\/terminus/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- public-api`
Expected: FAIL — danh sách export chưa khớp.

- [ ] **Step 3: Implement `src/index.ts`** với các export ở trên (type export bằng `export type`).

- [ ] **Step 4: Build and verify the package**

Run: `npm run build && npm run check:pkg && npm run lint && npm test && npm run test:int`
Expected: build ra `dist/index.js` + `dist/index.d.ts`; `publint` không lỗi; `attw` profile `esm-only` không lỗi; lint sạch; mọi test PASS.

Run: `node --input-type=module -e "import('@loipv/nestjs-nats').then(m => console.log(typeof m.NatsModule))"` (chạy trong thư mục gốc, package tự tham chiếu qua `exports`)
Expected: in ra `function`.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: public API surface and package checks"
```
