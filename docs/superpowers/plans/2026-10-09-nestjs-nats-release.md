# `@loipv/nestjs-nats` — Plan 4: Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Đưa thư viện tới trạng thái phát hành được trên npm: metadata gói chuẩn, docs tách theo tính năng có kiểm tra tự động, smoke test CJS/ESM trên Nest 11/12, app ví dụ, CI matrix và quy trình release bằng Changesets + npm trusted publishing (OIDC) có provenance.

**Architecture:** Không đổi code thư viện. Thêm các script Node (`scripts/*.mjs`, ESM, không dependency mới ngoài devDependencies) để kiểm tra tarball, typecheck ví dụ trong docs và chạy smoke test trên fixture cài từ tarball thật. Hai workflow GitHub Actions: `ci.yml` (mọi push/PR) và `release.yml` (push lên `main`, Changesets tạo PR "Version Packages" hoặc publish).

**Tech Stack:** Node 22/24, npm, GitHub Actions (`actions/checkout@v5`, `actions/setup-node@v5`, `changesets/action@v1`), `@changesets/cli@^3.0.3`, `@changesets/changelog-github@^1.0.1`, actionlint (Docker image `rhysd/actionlint:1.7.7`) để kiểm tra workflow cục bộ.

**Spec:** `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` (§16 "CI matrix", §17). Follow-ups: `docs/superpowers/notes/2026-10-09-plan{1,2,3}-followups.md`.

**Phạm vi** = spec §19 bước 4, cộng một việc treo từ Plan 3: cập nhật spec §9 theo các ruling (`datacontenttype`, `Nats-Msg-Id`). Ngoài phạm vi: thay đổi hành vi thư viện; quyết định heartbeat `concurrency > 1` (Plan 2, còn mở).

## Global Constraints

- Repo: `github.com/loipv/nestjs-nats` — `package.json#repository` = `{ "type": "git", "url": "git+https://github.com/loipv/nestjs-nats.git" }`, `homepage` = `https://github.com/loipv/nestjs-nats#readme`, `bugs.url` = `https://github.com/loipv/nestjs-nats/issues`.
- `author` = `"loipv"` (không ghi email). License MIT.
- `publishConfig` = `{ "access": "public", "provenance": true }`.
- CI matrix (spec §16): Node `22` và `24` × Nest `11` và `12`; smoke test app Nest 11 CommonJS `require()` được thư viện (Node 22).
- Release: Changesets; GitHub Actions publish qua npm trusted publishing (OIDC, `id-token: write`), không lưu npm token trong repo. npm CLI trong job release ≥ `11.5.1`.
- Mọi khối ```ts trong `README.md` và `docs/**/*.md` phải typecheck với bản build thật (`npm run docs:check`).
- Không thêm `dependencies`; chỉ devDependencies. Script trong `scripts/` là ESM `.mjs`, chạy bằng Node có sẵn, dùng `node:` built-ins.
- Integration test trên máy local vẫn cần `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock TESTCONTAINERS_RYUK_DISABLED=true`; trên GitHub Actions dùng Docker mặc định của runner `ubuntu-latest` (Ryuk bật).
- Phiên bản đầu tiên `0.1.0` được publish **thủ công một lần** (npm chỉ cho cấu hình trusted publisher trên gói đã tồn tại); các bản sau đi qua `release.yml`.

## Review Focus

1. **App Nest 11 CommonJS trên Node 22.12+** `require('@loipv/nestjs-nats')` và cả ba subpath → chạy được, dựng được app context và xử lý một message qua harness. Test ở Task 3.
2. **Tarball chỉ chứa đúng thứ cần ship** (`dist/`, `src/`, `README.md`, `LICENSE`, `CHANGELOG.md`, `package.json`) — không có `test/`, `docs/superpowers/`, `.superpowers/`, `examples/`, file `.map` trỏ tới nguồn không có. Test ở Task 1.
3. **App không cài optional peer** (`@opentelemetry/api`, `@nestjs/terminus`) import entry chính không lỗi; import `/otel` khi thiếu peer báo lỗi module rõ ràng. Test ở Task 3.
4. **Workflow release không publish từ PR hay khi build/test lỗi**; job publish chỉ chạy trên `main`. Test ở Task 6 (actionlint + kiểm tra cấu trúc YAML).
5. **Ví dụ trong docs lệch API** (đổi tên, xoá option) → CI đỏ. Test ở Task 2.

---

## File structure

```
package.json                                   # Task 1 (metadata, files, scripts), Task 2/3/4/6 (scripts)
scripts/check-pack.mjs                          # Task 1
scripts/check-docs.mjs                          # Task 2
docs/*.md, README.md                            # Task 2
docs/superpowers/specs/2026-10-09-nestjs-nats-design.md  # Task 2 (§9 sync)
test/smoke/cjs-nest11/{package.json,index.cjs}  # Task 3
test/smoke/esm-nest12/{package.json,index.mjs}  # Task 3
test/smoke/no-peers/{package.json,index.mjs}    # Task 3
scripts/smoke.mjs                               # Task 3
examples/orders/**                              # Task 4
.github/workflows/ci.yml                        # Task 5
.changeset/config.json, .changeset/README.md, .github/workflows/release.yml, RELEASING.md   # Task 6
biome.json                                      # Task 1 (ignore patterns nếu cần)
```

---

### Task 1: Package metadata và kiểm tra tarball

**Files:**
- Modify: `package.json`, `biome.json` (nếu cần bỏ qua `examples/**/node_modules`, `test/smoke/**/node_modules`)
- Create: `scripts/check-pack.mjs`
- Test: chính script (chạy trong `npm run check:pack`)

**Interfaces:**
- Produces:
  - `package.json`: `description` = `"Decorator-driven NATS and JetStream integration for NestJS: subscribers, consumers, services, CloudEvents, retry/DLQ, idempotency, batching, OpenTelemetry, health checks and testing utilities"`; `keywords` = `["nestjs", "nats", "jetstream", "microservices", "messaging", "cloudevents", "opentelemetry", "dead-letter-queue", "idempotency"]`; `repository`/`homepage`/`bugs`/`author`/`publishConfig` theo Global Constraints; `files` = `["dist", "src"]` (ship `src` để sourcemap trong `dist` trỏ tới đúng nguồn).
  - Script `"check:pack": "node scripts/check-pack.mjs"`.
  - `scripts/check-pack.mjs`: chạy `npm pack --dry-run --json`, lấy danh sách file; **fail** (exit 1, in danh sách vi phạm) khi: có file ngoài các mẫu cho phép `package.json`, `README.md`, `LICENSE`, `CHANGELOG.md`, `dist/**`, `src/**`; thiếu bất kỳ entry nào trong `package.json#exports` (`types` + `default` của 4 entry); có file `src/**` là test (`*.spec.ts`); có file `.map` trong `dist` mà `sources` trỏ tới file không có trong tarball. In tổng số file và kích thước khi pass.
  - `prepublishOnly` thêm `npm run check:pack` (sau `build`).

- [ ] **Step 1: Viết script và chạy trên trạng thái hiện tại** — Run: `npm run build && npm run check:pack` — Expected: FAIL vì `files` chưa có `src` nên sourcemap trỏ tới file thiếu (chứng minh check hoạt động).
- [ ] **Step 2: Cập nhật `package.json`** theo Interfaces.
- [ ] **Step 3: Verify** — Run: `npm run build && npm run check:pack && npm run check:pkg && npm run lint` — Expected: PASS; thêm thử một file `test/x.spec.ts` vào `files` tạm thời → FAIL (rồi hoàn tác).
- [ ] **Step 4: Commit** — `git commit -m "chore: package metadata and tarball content check"`

---

### Task 2: Docs theo tính năng + kiểm tra ví dụ trong docs

**Files:**
- Create: `docs/connections.md`, `docs/handlers.md`, `docs/publishing.md`, `docs/streams.md`, `docs/retry-and-dlq.md`, `docs/idempotency.md`, `docs/batching.md`, `docs/cloudevents.md`, `docs/plugins.md`, `docs/tracing.md`, `docs/health.md`, `docs/testing.md`, `docs/lifecycle.md`, `docs/errors.md`, `scripts/check-docs.mjs`
- Modify: `README.md`, `package.json`, `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` (§9)

**Interfaces:**
- Produces:
  - Nội dung chi tiết hiện có trong README được **chuyển nguyên văn** (không viết lại hành vi) sang các trang `docs/*.md` tương ứng với các mục `##` hiện tại (Codecs gộp vào `publishing.md`; Parameters/pipes vào `handlers.md`; Known limitations ở lại README). Mỗi trang bắt đầu bằng `# <Tiêu đề>` và một dòng link về README.
  - `README.md` còn: banner trạng thái, Features (mỗi dòng link tới trang docs), Requirements, Installation, Quick start, bảng "Documentation" liệt kê các trang, Known limitations, License. Link nội bộ cũ (`#retry-and-dead-letter-queue`, …) được sửa thành link tới trang docs (kể cả link chéo giữa các trang).
  - `scripts/check-docs.mjs`: tìm mọi khối ```ts trong `README.md` và `docs/*.md` (không gồm `docs/superpowers/**`); ghi mỗi khối vào thư mục tạm trong `os.tmpdir()`; thêm phần prelude khai báo các placeholder dùng trong docs (`OrderDto`, `OrderEvent`, `CreateOrderDto`, `OrderService`, `PaymentService`, `PaymentDeclinedError`, `InvoiceService`, `AnalyticsService`, `TenantGuard`, `client`, …) và stub module cho gói không cài (`@nestjs/config`, `ioredis`, `@opentelemetry/sdk-node`, `@opentelemetry/exporter-trace-otlp-http`); khối là "đoạn method" (bắt đầu bằng decorator mà không có `class`) được bọc trong class; tạo `tsconfig.json` kế thừa tsconfig gốc (`noEmit`, `noUnusedLocals: false`); chạy `tsc -p`; in lỗi kèm `file:block` gốc; xoá thư mục tạm; exit code theo `tsc`.
  - Script `"docs:check": "npm run build && node scripts/check-docs.mjs"`.
  - Spec §9 cập nhật đúng hai điểm theo ruling Plan 3: `datacontenttype` lấy từ `cloudEvent.datacontenttype` nếu có, mặc định theo codec, và bỏ qua khi data là `Uint8Array` không khai báo; `Nats-Msg-Id` ưu tiên `msgId` > header `Nats-Msg-Id` của caller > `ce.id`; bên nhận: `datacontenttype` không phải JSON và khác codec → handler nhận `Uint8Array`.

- [ ] **Step 1: Viết `check-docs.mjs`, chạy trên README hiện tại** — Run: `npm run docs:check` — Expected: PASS (27 khối hiện có typecheck).
- [ ] **Step 2: Chứng minh check bắt lỗi** — sửa tạm một khối dùng option không tồn tại (vd. `@Subscribe('x', { nope: 1 })`) → `npm run docs:check` FAIL và chỉ đúng file/khối; hoàn tác.
- [ ] **Step 3: Tách docs và rút gọn README** theo Interfaces; cập nhật spec §9.
- [ ] **Step 4: Verify** — `check-docs.mjs` cũng kiểm link: mọi link tương đối (`](./docs/x.md)`, `](x.md#anchor)`, `](#anchor)`) trong README và `docs/*.md` phải trỏ tới file tồn tại và anchor sinh từ heading kiểu GitHub (chữ thường, bỏ ký tự đặc biệt, khoảng trắng → `-`). Run: `npm run docs:check && npm run lint` — Expected: PASS, 0 link hỏng; thêm tạm một link hỏng → FAIL (hoàn tác).
- [ ] **Step 5: Commit** — `git commit -m "docs: per-feature docs pages and automated docs example check"`

---

### Task 3: Smoke test CJS / ESM / thiếu peer từ tarball thật

**Files:**
- Create: `test/smoke/cjs-nest11/package.json`, `test/smoke/cjs-nest11/index.cjs`, `test/smoke/esm-nest12/package.json`, `test/smoke/esm-nest12/index.mjs`, `test/smoke/no-peers/package.json`, `test/smoke/no-peers/index.mjs`, `scripts/smoke.mjs`
- Modify: `package.json`, `.gitignore` (bỏ qua `test/smoke/*/node_modules`, `test/smoke/*/package-lock.json`)

**Interfaces:**
- Produces:
  - `scripts/smoke.mjs [fixture...]`: `npm run build`, `npm pack` vào thư mục tạm; với mỗi fixture (mặc định cả ba): copy fixture sang thư mục tạm, `npm install <tarball> --no-audit --no-fund` cùng dependencies của fixture, chạy entry (`node index.cjs` / `node index.mjs`); fail nếu exit ≠ 0 hoặc stdout không chứa `SMOKE OK`; dọn thư mục tạm.
  - `cjs-nest11`: `"type": "commonjs"`, dependencies `@nestjs/common@^11`, `@nestjs/core@^11`, `@nestjs/terminus@^11`, `@nats-io/transport-node@^3`, `@nats-io/jetstream@^3`, `@nats-io/services@^3`, `@opentelemetry/api@^1.9`, `reflect-metadata@^0.2`, `rxjs@^7`. `index.cjs`: `require('reflect-metadata')`; `require('@loipv/nestjs-nats')`, `/otel`, `/health`, `/testing`; viết handler bằng decorator **không dùng cú pháp decorator** (gọi `Subscribe(...)(proto, key, descriptor)` và `Payload()(proto, key, 0)` thủ công, vì CJS JS thuần); dựng `NestFactory.createApplicationContext` với `NatsTestingModule.forRoot()` + provider; `NatsTestHarness.create(app).dispatch({ handler: 'Handlers.onOrder' }, { id: 1 })` → kiểm `settlement === 'none'` và handler nhận `{ id: 1 }`; `typeof tracing === 'function'`, `typeof NatsHealthIndicator === 'function'`; in `SMOKE OK`.
  - `esm-nest12`: như trên nhưng `"type": "module"`, Nest `^12`, `@nestjs/terminus@^12`, `index.mjs` dùng `import`.
  - `no-peers`: `"type": "module"`, chỉ Nest 12 + `@nats-io/*` + `reflect-metadata` + `rxjs` (không `@opentelemetry/api`, không `@nestjs/terminus`); import entry chính thành công; `import('@loipv/nestjs-nats/otel')` reject với lỗi có mã `ERR_MODULE_NOT_FOUND` nhắc tới `@opentelemetry/api`; in `SMOKE OK`.
  - Script `"test:smoke": "node scripts/smoke.mjs"`.

- [ ] **Step 1: Viết fixture + script** theo Interfaces.
- [ ] **Step 2: Chạy** — Run: `npm run test:smoke` — Expected: ba fixture đều in `SMOKE OK` (Node 24 local).
- [ ] **Step 3: Chạy với Node 22** nếu có sẵn (`npx -y node@22 scripts/smoke.mjs cjs-nest11` hoặc `nvm`/`fnm`), ghi kết quả vào report; nếu không có Node 22 local, ghi rõ CI sẽ chạy (Task 5).
- [ ] **Step 4: Chứng minh smoke bắt lỗi** — tạm đổi `exports["."].default` trong bản pack (sửa `package.json` rồi chạy) → FAIL; hoàn tác.
- [ ] **Step 5: Commit** — `git commit -m "test: CJS/ESM/no-peer smoke tests against the packed tarball"`

---

### Task 4: App ví dụ `examples/orders`

**Files:**
- Create: `examples/orders/package.json`, `examples/orders/tsconfig.json`, `examples/orders/docker-compose.yml`, `examples/orders/README.md`, `examples/orders/src/main.ts`, `examples/orders/src/app.module.ts`, `examples/orders/src/orders.handlers.ts`, `examples/orders/src/events.router.ts`, `examples/orders/src/health.controller.ts`, `examples/orders/src/tracing.ts`, `examples/orders/test/orders.handlers.spec.ts`
- Modify: root `package.json` (script), root `biome.json` (nếu cần), `.gitignore`

**Interfaces:**
- Produces:
  - `examples/orders/package.json`: private, `"type": "module"`, dependency `"@loipv/nestjs-nats": "file:../.."` + Nest 12 (`@nestjs/common`, `core`, `platform-express`, `terminus`), `@nats-io/*`, `@opentelemetry/api`, `@opentelemetry/sdk-trace-base` (ConsoleSpanExporter), `@opentelemetry/context-async-hooks`, `@opentelemetry/core`, `reflect-metadata`, `rxjs`; devDependencies `typescript@~6.0.3`, `vitest`, `unplugin-swc`, `@swc/core`; scripts `start` (build rồi chạy), `typecheck`, `test`.
  - `docker-compose.yml`: service `nats` image `nats:2.12-alpine`, command `["-js", "-m", "8222"]`, ports `4222:4222`, `8222:8222`.
  - App: stream `ORDERS` (`orders.>`) + `DLQ` (`dlq.>`); `@Subscribe('orders.get')` request/reply; `@Consume` durable `billing` với `retry { maxRetries: 3, initialDelay: 500 }` và `dlq: true`; `@CloudEventRouter({ subscribe: 'events.>' })` với hai `@OnCloudEvent`; health endpoint `GET /health` dùng `NatsHealthIndicator`; tracing bằng `tracing()` + `BasicTracerProvider` với `ConsoleSpanExporter`; `main.ts` bật `enableShutdownHooks()`, publish vài message mẫu khi khởi động (một message cố tình lỗi để thấy retry → DLQ).
  - `test/orders.handlers.spec.ts`: dùng `NatsTestingModule` + `NatsTestHarness` (không cần NATS) — billing thành công → `ack`; message lỗi ở attempt 4 → `dlq`; router route đúng type.
  - `examples/orders/README.md`: `docker compose up -d`, `npm install`, `npm start`, `curl localhost:3000/health`, `npm test`.
  - Root script `"example:check": "npm run build && cd examples/orders && npm install --no-audit --no-fund && npm run typecheck && npm test"`.

- [ ] **Step 1: Viết test của ví dụ trước** (harness) — Run: `npm run example:check` — Expected: FAIL (handlers chưa có).
- [ ] **Step 2: Viết app** theo Interfaces.
- [ ] **Step 3: Verify** — Run: `npm run example:check` — Expected: PASS. Chạy thật một lần: `docker compose -f examples/orders/docker-compose.yml up -d` (local cần `DOCKER_HOST` colima), `npm start` trong `examples/orders` khoảng 15 giây, xác nhận log có message được xử lý, retry rồi DLQ, `curl localhost:3000/health` trả `status: ok`, rồi dừng app và `docker compose down`; ghi output vào report.
- [ ] **Step 4: Commit** — `git commit -m "docs: runnable orders example with docker-compose"`

---

### Task 5: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `package.json` (script `ci:lint-workflows` nếu dùng)

**Interfaces:**
- Produces — `ci.yml`, `name: CI`, `on: { push: { branches: [main] }, pull_request: {} }`, `permissions: { contents: read }`, `concurrency: { group: ci-${{ github.ref }}, cancel-in-progress: true }`, các job:
  - `checks` (ubuntu-latest, Node 24): `npm ci`, `npm run lint`, `npm run typecheck`, `npm run build`, `npm run check:pkg`, `npm run check:pack`, `npm run docs:check`
  - `test` (ubuntu-latest), `strategy.matrix: { node: [22, 24], nest: [11, 12] }`, `fail-fast: false`: `npm ci`; nếu `matrix.nest == 11`: `npm install --no-save @nestjs/common@^11 @nestjs/core@^11 @nestjs/testing@^11 @nestjs/terminus@^11`; in phiên bản `@nestjs/core` thực tế; `npm test`; `npm run test:int`
  - `smoke` (ubuntu-latest), `strategy.matrix: { node: ['22.12', 24] }`: `npm ci`, `npm run test:smoke`
  - `example` (ubuntu-latest, Node 24): `npm ci`, `npm run example:check`
  - Mọi job: `actions/checkout@v5`, `actions/setup-node@v5` với `node-version` từ matrix và `cache: npm`
  - Script `"lint:workflows": "docker run --rm -v \"$PWD\":/repo -w /repo rhysd/actionlint:1.7.7 -color"`

- [ ] **Step 1: Viết `ci.yml`.**
- [ ] **Step 2: Verify** — Run: `npm run lint:workflows` (local: thêm `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock`) — Expected: không lỗi. Chạy local tương đương job `test` Nest 11: `npm install --no-save @nestjs/common@^11 @nestjs/core@^11 @nestjs/testing@^11 @nestjs/terminus@^11 && npm test && npm run test:int` (kèm env Docker local) — Expected: PASS; rồi `npm ci` để về lại Nest 12 và chạy `npm test` một lần nữa. Ghi phiên bản Nest 11 thực tế và kết quả vào report.
- [ ] **Step 3: Commit** — `git commit -m "ci: lint, typecheck, Node 22/24 × Nest 11/12 tests, smoke and example jobs"`

---

### Task 6: Changesets + release workflow + hướng dẫn phát hành

**Files:**
- Create: `.changeset/config.json`, `.changeset/README.md`, `.github/workflows/release.yml`, `RELEASING.md`
- Modify: `package.json` (devDependencies `@changesets/cli@^3.0.3`, `@changesets/changelog-github@^1.0.1`; scripts), `README.md` (link `CHANGELOG.md` và `RELEASING.md`)

**Interfaces:**
- Produces:
  - `.changeset/config.json`: `{ "$schema": "https://unpkg.com/@changesets/config@3.1.1/schema.json", "changelog": ["@changesets/changelog-github", { "repo": "loipv/nestjs-nats" }], "commit": false, "access": "public", "baseBranch": "main", "updateInternalDependencies": "patch", "ignore": [] }` (kiểm `$schema` đúng phiên bản `@changesets/config` được cài, sửa cho khớp)
  - Scripts: `"changeset": "changeset"`, `"version-packages": "changeset version"`, `"release": "changeset publish"` (`prepublishOnly` đã build + check + test unit)
  - `release.yml`, `name: Release`, `on: { push: { branches: [main] } }`, `concurrency: { group: release, cancel-in-progress: false }`, job `release` (ubuntu-latest) chỉ khi `github.repository == 'loipv/nestjs-nats'`; `permissions: { contents: write, pull-requests: write, id-token: write }`; steps: checkout (`fetch-depth: 0`), setup-node Node 24 với `registry-url: https://registry.npmjs.org`, `npm install -g npm@^11.5.1`, `npm ci`, `changesets/action@v1` với `version: npm run version-packages`, `publish: npm run release`, `commit: "chore: version packages"`, `title: "chore: version packages"`, env `GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}` (không `NPM_TOKEN`)
  - `RELEASING.md` (tiếng Anh, cho maintainer): (1) bootstrap một lần — đăng nhập npm, `npm run build && npm publish --access public` cho `0.1.0` từ máy local (2FA), rồi trên npmjs.com cấu hình Trusted Publisher: GitHub Actions, owner `loipv`, repo `nestjs-nats`, workflow `release.yml`; (2) quy trình thường — `npm run changeset` trong PR, merge → bot mở PR "chore: version packages" → merge PR đó → workflow publish với provenance; (3) kiểm tra provenance trên trang npm; (4) xử lý sự cố: publish lỗi OIDC (npm < 11.5.1, thiếu `id-token: write`, sai tên workflow trong cấu hình trusted publisher).
  - Không tạo changeset nào cho `0.1.0` (bản đầu publish thủ công); `CHANGELOG.md` khởi tạo với mục `## 0.1.0` tóm tắt tính năng (Changesets sẽ ghi tiếp phía trên).

- [ ] **Step 1: Thêm Changesets** — Run: `npm install -D @changesets/cli@^3.0.3 @changesets/changelog-github@^1.0.1` rồi viết config — Verify: `npx changeset status` không lỗi (báo không có changeset).
- [ ] **Step 2: Thử luồng version cục bộ** — tạo changeset `patch` tạm (`npx changeset add --empty` hoặc file thủ công), `npx changeset version` (cần `GITHUB_TOKEN` cho changelog-github: nếu không có, ghi nhận và chạy thử với `changelog: false` tạm thời) → kiểm version thành `0.1.1` và `CHANGELOG.md` có mục mới; **hoàn tác toàn bộ** (`git checkout -- . && git clean -fd .changeset`).
- [ ] **Step 3: Viết `release.yml`, `RELEASING.md`, `CHANGELOG.md`** theo Interfaces.
- [ ] **Step 4: Verify** — Run: `npm run lint:workflows` — Expected: không lỗi; thêm `scripts/check-workflows.mjs` (script `"check:workflows"`, chạy trong job `checks` của CI) kiểm bằng so khớp chuỗi trên nội dung file, xác nhận: `release.yml` chỉ `on.push.branches: [main]`, có `id-token: write`, không tham chiếu `NPM_TOKEN`; `ci.yml` không có quyền ghi. Ghi output vào report.
- [ ] **Step 5: Commit** — `git commit -m "build: changesets and trusted-publishing release workflow"`
