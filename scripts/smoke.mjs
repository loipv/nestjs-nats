// Installs the real `npm pack` tarball into each fixture app under test/smoke and runs it.
// Usage: node scripts/smoke.mjs [fixture...]   (default: every fixture)
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const fixturesDir = join(root, 'test', 'smoke');
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function run(command, args, cwd, { capture = false } = {}) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    stdio: capture ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'inherit', 'inherit'],
    maxBuffer: 64 * 1024 * 1024,
    shell: process.platform === 'win32',
  });
  if (result.error) throw result.error;
  return result;
}

function must(command, args, cwd, label) {
  const result = run(command, args, cwd, { capture: true });
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? '');
    process.stderr.write(result.stderr ?? '');
    throw new Error(`${label} failed (exit ${result.status})`);
  }
  return result.stdout;
}

const available = readdirSync(fixturesDir, { withFileTypes: true })
  .filter((d) => d.isDirectory())
  .map((d) => d.name)
  .sort();
const requested = process.argv.slice(2);
const unknown = requested.filter((name) => !available.includes(name));
if (unknown.length > 0) {
  console.error(`unknown fixture(s): ${unknown.join(', ')}. Available: ${available.join(', ')}`);
  process.exit(2);
}
const fixtures = requested.length > 0 ? requested : available;

const work = mkdtempSync(join(tmpdir(), 'nestjs-nats-smoke-'));
const failures = [];
try {
  console.log('smoke: building');
  must(npm, ['run', 'build'], root, 'npm run build');

  console.log('smoke: packing');
  const packed = JSON.parse(
    must(npm, ['pack', '--json', '--ignore-scripts', '--pack-destination', work], root, 'npm pack'),
  );
  const tarball = join(work, packed[0].filename);

  for (const name of fixtures) {
    const dir = join(work, name);
    const entry = readdirSync(join(fixturesDir, name)).find((f) => /^index\.[cm]?js$/.test(f));
    cpSync(join(fixturesDir, name), dir, {
      recursive: true,
      filter: (src) => !/[\\/](node_modules|package-lock\.json)$/.test(src),
    });
    console.log(`smoke: ${name}: installing (node ${process.version})`);
    try {
      must(npm, ['install', tarball, '--no-audit', '--no-fund'], dir, `${name}: npm install`);
      const result = run(process.execPath, [entry], dir, { capture: true });
      const out = `${result.stdout}${result.stderr}`;
      if (result.status !== 0 || !result.stdout.includes('SMOKE OK')) {
        throw new Error(`${name}: exit ${result.status}, no "SMOKE OK"\n${out}`);
      }
      console.log(`smoke: ${name}: SMOKE OK`);
    } catch (err) {
      console.error(`smoke: ${name}: FAILED\n${err.message}`);
      failures.push(name);
    }
  }
} finally {
  rmSync(work, { recursive: true, force: true });
}

if (failures.length > 0) {
  console.error(`smoke FAILED: ${failures.join(', ')}`);
  process.exit(1);
}
console.log(`smoke OK: ${fixtures.join(', ')}`);
