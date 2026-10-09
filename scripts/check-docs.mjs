// Checks the documentation: every ```ts block in README.md and docs/*.md must typecheck
// against the real build (dist/, resolved through the package's own `exports`), and every
// relative Markdown link must point to an existing file and, if it has one, an existing anchor.
// Run through `npm run docs:check`, which builds first.
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
// The temp dir lives inside the repo (git-ignored) so that `@loipv/nestjs-nats` and its subpaths
// resolve through the package self-reference, exactly like for a consumer. It is always removed.
const tmp = join(root, '.docs-check-tmp');
const LIB = '@loipv/nestjs-nats';

const docFiles = [
  'README.md',
  ...readdirSync(join(root, 'docs'), { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.endsWith('.md'))
    .map((e) => `docs/${e.name}`)
    .sort(),
];

// Splits a Markdown file into lines, tracking fenced code blocks.
function scan(text) {
  const lines = text.split('\n');
  const blocks = []; // { lang, startLine (1-based line of the opening fence), code }
  const fenced = new Array(lines.length).fill(false);
  let open = null;
  for (let i = 0; i < lines.length; i++) {
    const m = /^(\s*)```(\S*)/.exec(lines[i]);
    if (open === null) {
      if (m) open = { lang: m[2], startLine: i + 1, body: [] };
      fenced[i] = m !== null;
    } else {
      fenced[i] = true;
      if (m && m[2] === '') {
        blocks.push({ lang: open.lang, startLine: open.startLine, code: open.body.join('\n') });
        open = null;
      } else {
        open.body.push(lines[i]);
      }
    }
  }
  return { lines, fenced, blocks };
}

// ---------------------------------------------------------------- examples

const PLACEHOLDER_NAMES = [
  'OrderDto',
  'OrderEvent',
  'CreateOrderDto',
  'OrderService',
  'PaymentService',
  'PaymentDeclinedError',
  'InvoiceService',
  'AnalyticsService',
  'TenantGuard',
  'client',
];

const placeholders = {
  OrderDto: 'declare class OrderDto { id: string; total: number }',
  OrderEvent: 'declare class OrderEvent { id: string; total: number }',
  CreateOrderDto: 'declare class CreateOrderDto { total: number }',
  OrderService:
    'declare class OrderService { create(tenant: string, dto: unknown): unknown; project(order: unknown, id: string): unknown; touch(subject: string | undefined, data: unknown): unknown; noteIgnored(type: string): void }',
  PaymentService: 'declare class PaymentService { charge(order: unknown): Promise<void> }',
  PaymentDeclinedError: 'declare class PaymentDeclinedError extends Error {}',
  InvoiceService: 'declare class InvoiceService { create(order: unknown): Promise<void> }',
  AnalyticsService:
    'declare class AnalyticsService { record(order: unknown, deliveryCount: number): Promise<void> }',
  TenantGuard: 'declare class TenantGuard { canActivate(): boolean }',
  client: `declare const client: __lib.NatsClient`,
};

// Identifiers made available to blocks that have no imports at all (decorator fragments and
// bare statements); blocks with their own imports are left exactly as written.
const FRAGMENT_IMPORTS = [
  'Consume',
  'Ctx',
  'NatsJetStreamContext',
  'NatsModule',
  'Payload',
  'Subject',
  'Subscribe',
];

const STUB_MODULES = `declare module '@nestjs/config' {
  export class ConfigModule {}
  export class ConfigService {
    get<T = string>(key: string): T | undefined;
    getOrThrow<T = string>(key: string): T;
  }
}
declare module 'ioredis' {
  export class Redis {
    constructor(...args: unknown[]);
    set(...args: unknown[]): Promise<string | null>;
    get(key: string): Promise<string | null>;
    del(key: string): Promise<number>;
  }
}
declare module '@opentelemetry/sdk-node' {
  export class NodeSDK {
    constructor(options?: Record<string, unknown>);
    start(): void;
  }
}
declare module '@opentelemetry/exporter-trace-otlp-http' {
  export class OTLPTraceExporter {
    constructor(options?: Record<string, unknown>);
  }
}
`;

function declares(code, name) {
  return new RegExp(`\\b(?:class|interface|type|const|let|var|function|enum)\\s+${name}\\b`).test(
    code,
  );
}

function buildExample(code) {
  const hasImports = /^\s*import\s/m.test(code);
  const isMethodFragment = /^\s*@/.test(code) && !/\bclass\s/.test(code);
  const head = [`import type * as __lib from '${LIB}';`];
  if (!hasImports) head.push(`import { ${FRAGMENT_IMPORTS.join(', ')} } from '${LIB}';`);
  for (const name of PLACEHOLDER_NAMES) {
    if (!declares(code, name)) head.push(placeholders[name]);
  }
  // `export {}` makes every block a module (top-level await, scoped declarations).
  head.push('export {};');
  let body = code;
  if (isMethodFragment) {
    head.push('class __Fragment {');
    body = `${code}\n}`;
  }
  return { text: `${head.join('\n')}\n${body}\n`, headLines: head.length };
}

function checkExamples(failures) {
  rmSync(tmp, { recursive: true, force: true });
  mkdirSync(tmp, { recursive: true });
  const cleanup = () => rmSync(tmp, { recursive: true, force: true });
  process.on('SIGINT', () => {
    cleanup();
    process.exit(130);
  });
  try {
    const index = new Map(); // generated file name -> { label, headLines, startLine }
    let count = 0;
    for (const file of docFiles) {
      const { blocks } = scan(readFileSync(join(root, file), 'utf8'));
      let n = 0;
      for (const block of blocks) {
        if (block.lang !== 'ts') continue;
        n++;
        count++;
        const name = `${file.replace(/[^A-Za-z0-9]+/g, '_')}__block${n}.ts`;
        const { text, headLines } = buildExample(block.code);
        writeFileSync(join(tmp, name), text);
        index.set(name, {
          label: `${file}:block ${n} (line ${block.startLine})`,
          headLines,
          startLine: block.startLine,
        });
      }
    }
    writeFileSync(join(tmp, 'stubs.d.ts'), STUB_MODULES);
    writeFileSync(
      join(tmp, 'tsconfig.json'),
      JSON.stringify(
        {
          extends: '../tsconfig.json',
          compilerOptions: {
            noEmit: true,
            declaration: false,
            sourceMap: false,
            noUnusedLocals: false,
            noUnusedParameters: false,
          },
          include: ['./*.ts'],
        },
        null,
        2,
      ),
    );

    const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc');
    const res = spawnSync(
      process.execPath,
      [tsc, '-p', join(tmp, 'tsconfig.json'), '--pretty', 'false'],
      {
        cwd: root,
        encoding: 'utf8',
        maxBuffer: 64 * 1024 * 1024,
      },
    );
    const output = `${res.stdout ?? ''}${res.stderr ?? ''}`;
    if (res.status !== 0) {
      for (const line of output.split('\n')) {
        if (line.trim() === '') continue;
        const m = /^(?:\.docs-check-tmp[\\/])?([^()]+)\((\d+),(\d+)\): (.*)$/.exec(line);
        const info = m ? index.get(m[1].split(/[\\/]/).pop()) : undefined;
        if (m && info) {
          const srcLine = info.startLine + (Number(m[2]) - info.headLines);
          failures.push(`${info.label}: line ${srcLine}: ${m[4]}`);
        } else {
          failures.push(line);
        }
      }
      if (failures.length === 0) failures.push(`tsc exited with ${res.status}`);
    }
    return count;
  } finally {
    cleanup();
  }
}

// ---------------------------------------------------------------- links

// GitHub-style heading anchor: lowercase, drop punctuation, spaces to hyphens.
function slug(heading) {
  return heading
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*_~]/g, (c) => (c === '_' ? '_' : ''))
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}_\- ]/gu, '')
    .replace(/ /g, '-');
}

const anchorCache = new Map();
function anchorsOf(path) {
  if (anchorCache.has(path)) return anchorCache.get(path);
  const { lines, fenced } = scan(readFileSync(path, 'utf8'));
  const seen = new Map();
  const set = new Set();
  lines.forEach((line, i) => {
    if (fenced[i]) return;
    const m = /^#{1,6}\s+(.+?)\s*#*\s*$/.exec(line);
    if (!m) return;
    const base = slug(m[1]);
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    set.add(n === 0 ? base : `${base}-${n}`);
  });
  anchorCache.set(path, set);
  return set;
}

function checkLinks(failures) {
  let count = 0;
  for (const file of docFiles) {
    const path = join(root, file);
    const { lines, fenced } = scan(readFileSync(path, 'utf8'));
    lines.forEach((line, i) => {
      if (fenced[i]) return;
      const stripped = line.replace(/`[^`]*`/g, (s) => ' '.repeat(s.length));
      for (const m of stripped.matchAll(/\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g)) {
        const target = m[1];
        if (/^[a-z][a-z0-9+.-]*:/i.test(target)) continue; // http:, https:, mailto:, ...
        count++;
        const [filePart, anchor] = target.split('#');
        const where = `${file}:${i + 1}`;
        const targetPath = filePart === '' ? path : resolve(dirname(path), filePart);
        if (!existsSync(targetPath)) {
          failures.push(
            `${where}: broken link "${target}": ${relative(root, targetPath)} does not exist`,
          );
          continue;
        }
        if (anchor !== undefined && anchor !== '') {
          if (!targetPath.endsWith('.md')) continue;
          if (!anchorsOf(targetPath).has(anchor)) {
            failures.push(
              `${where}: broken link "${target}": no heading with anchor "#${anchor}" in ${relative(root, targetPath)}`,
            );
          }
        }
      }
    });
  }
  return count;
}

// ---------------------------------------------------------------- main

const exampleFailures = [];
const linkFailures = [];
const blocks = checkExamples(exampleFailures);
const links = checkLinks(linkFailures);

if (exampleFailures.length > 0) {
  console.error(`docs:check: ${exampleFailures.length} typecheck error(s) in examples:`);
  for (const f of exampleFailures) console.error(`  ${f}`);
}
if (linkFailures.length > 0) {
  console.error(`docs:check: ${linkFailures.length} broken link(s):`);
  for (const f of linkFailures) console.error(`  ${f}`);
}
if (exampleFailures.length > 0 || linkFailures.length > 0) process.exit(1);
console.log(
  `docs:check: ${blocks} ts block(s) typecheck, ${links} relative link(s) resolve, in ${docFiles.length} file(s)`,
);
