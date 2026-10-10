// Installs the real `npm pack` tarball into each fixture app under test/smoke and runs it.
// Usage: node scripts/smoke.mjs [fixture...]   (default: every fixture)
// The fixtures' dependency ranges (`^11`, `^12`, `^3`, ...) float on purpose and no lockfile is
// kept: each run tests the tarball against the latest releases a consumer would install today.
import { spawn } from 'node:child_process';
import { cpSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const fixturesDir = join(root, 'test', 'smoke');
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const MINUTE = 60_000;

let current = null; // the running child process, stopped on SIGINT/SIGTERM

// Runs a command to completion, capturing its output. Asynchronous (not spawnSync) so that the
// signal handlers below can run while a child is busy. Kills the child after `timeout` ms.
function run(command, args, cwd, timeout) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: process.platform === 'win32',
    });
    current = child;
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    child.stdout.setEncoding('utf8').on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.setEncoding('utf8').on('data', (chunk) => {
      stderr += chunk;
    });
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
    }, timeout);
    child.on('error', (err) => {
      clearTimeout(timer);
      current = null;
      reject(err);
    });
    child.on('close', (status, signal) => {
      clearTimeout(timer);
      current = null;
      resolveRun({ status, signal, stdout, stderr, timedOut });
    });
  });
}

function describe(result, timeout) {
  if (result.timedOut) return `timed out after ${timeout / MINUTE} min`;
  return result.signal ? `killed by ${result.signal}` : `exit ${result.status}`;
}

async function must(command, args, cwd, label, timeout) {
  const result = await run(command, args, cwd, timeout);
  if (result.status !== 0) {
    process.stderr.write(result.stdout);
    process.stderr.write(result.stderr);
    throw new Error(`${label} failed (${describe(result, timeout)})`);
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
const cleanup = () => rmSync(work, { recursive: true, force: true });

// Ctrl-C or a CI cancel: stop the running child, wait briefly for it to exit (it may still be
// writing into the temp dir), then remove the temp dir.
let stopping = false;
for (const [signal, code] of [
  ['SIGINT', 130],
  ['SIGTERM', 143],
]) {
  process.on(signal, () => {
    if (stopping) return;
    stopping = true;
    console.error(`smoke: ${signal}, cleaning up`);
    const exit = () => {
      try {
        cleanup();
      } finally {
        process.exit(code);
      }
    };
    if (current === null) return exit();
    current.once('close', exit);
    current.kill(signal);
    setTimeout(exit, 5000).unref();
  });
}

const failures = [];
try {
  console.log('smoke: building');
  await must(npm, ['run', 'build'], root, 'npm run build', 5 * MINUTE);

  console.log('smoke: packing');
  const packed = JSON.parse(
    await must(
      npm,
      ['pack', '--json', '--ignore-scripts', '--pack-destination', work],
      root,
      'npm pack',
      2 * MINUTE,
    ),
  );
  const tarball = join(work, packed[0].filename);

  for (const name of fixtures) {
    try {
      const dir = join(work, name);
      const entry = readdirSync(join(fixturesDir, name)).find((f) => /^index\.[cm]?js$/.test(f));
      if (entry === undefined) throw new Error(`${name}: no index.js, index.mjs or index.cjs`);
      cpSync(join(fixturesDir, name), dir, {
        recursive: true,
        filter: (src) => !/[\\/](node_modules|package-lock\.json)$/.test(src),
      });
      console.log(`smoke: ${name}: installing (node ${process.version})`);
      await must(
        npm,
        ['install', tarball, '--no-audit', '--no-fund'],
        dir,
        `${name}: npm install`,
        10 * MINUTE,
      );
      const timeout = 2 * MINUTE;
      const result = await run(process.execPath, [entry], dir, timeout);
      if (result.status !== 0 || !result.stdout.includes('SMOKE OK')) {
        throw new Error(
          `${name}: ${describe(result, timeout)}, no "SMOKE OK"\n${result.stdout}${result.stderr}`,
        );
      }
      console.log(`smoke: ${name}: SMOKE OK`);
    } catch (err) {
      console.error(`smoke: ${name}: FAILED\n${err.message}`);
      failures.push(name);
    }
  }
} finally {
  cleanup();
}

if (failures.length > 0) {
  console.error(`smoke FAILED: ${failures.join(', ')}`);
  process.exit(1);
}
console.log(`smoke OK: ${fixtures.join(', ')}`);
