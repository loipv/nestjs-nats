// Verifies the npm tarball contents before publishing.
// Runs `npm pack --dry-run --json` and fails (exit 1) on any violation.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join, posix, resolve } from 'node:path';

// Paths are resolved from the repository root, not the cwd.
const root = resolve(import.meta.dirname, '..');
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));

let info;
try {
  const out = execFileSync('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
    maxBuffer: 64 * 1024 * 1024,
    shell: process.platform === 'win32',
  });
  [info] = JSON.parse(out);
} catch (err) {
  const detail = err.status != null ? `exit ${err.status}` : err.message;
  console.error(
    `check:pack FAILED: \`npm pack --dry-run --json\` did not succeed (${detail}); see the npm output above.`,
  );
  process.exit(1);
}
const files = info.files.map((f) => f.path);
const fileSet = new Set(files);

const violations = [];

// 1. Only allowed paths, and every required top-level file is present.
const requiredTop = ['package.json', 'README.md', 'LICENSE', 'CHANGELOG.md'];
const allowedTop = new Set(requiredTop);
const allowed = (p) => allowedTop.has(p) || p.startsWith('dist/') || p.startsWith('src/');
for (const p of files) {
  if (!allowed(p)) violations.push(`unexpected file in tarball: ${p}`);
}
for (const p of requiredTop) {
  if (!fileSet.has(p)) violations.push(`required file missing from tarball: ${p}`);
}

// 2. Every exports entry (types + default) must exist.
const exportsMap = pkg.exports ?? {};
for (const [entry, conditions] of Object.entries(exportsMap)) {
  for (const cond of ['types', 'default']) {
    const target = conditions?.[cond];
    if (!target) {
      violations.push(`exports["${entry}"] has no "${cond}" condition`);
      continue;
    }
    const rel = posix.normalize(target);
    if (!fileSet.has(rel)) {
      violations.push(`exports["${entry}"].${cond} missing from tarball: ${rel}`);
    }
  }
}

// 3. No test files under src.
for (const p of files) {
  if (p.startsWith('src/') && /\.(spec|test)\.[cm]?[jt]sx?$/.test(p)) {
    violations.push(`test file shipped in tarball: ${p}`);
  }
}

// 4. Sourcemap `sources` must resolve to files inside the tarball.
for (const p of files) {
  if (!p.startsWith('dist/') || !p.endsWith('.js.map')) continue;
  let map;
  try {
    map = JSON.parse(readFileSync(join(root, p), 'utf8'));
  } catch (err) {
    violations.push(`cannot read sourcemap ${p}: ${err.message}`);
    continue;
  }
  const sourceRoot = map.sourceRoot ?? '';
  for (const source of map.sources ?? []) {
    const resolved = posix.normalize(posix.join(posix.dirname(p), sourceRoot, source));
    if (!fileSet.has(resolved)) {
      violations.push(`sourcemap ${p} references file not in tarball: ${resolved}`);
    }
  }
}

if (violations.length > 0) {
  console.error(`check:pack FAILED with ${violations.length} violation(s):`);
  for (const v of violations) console.error(`  - ${v}`);
  process.exit(1);
}

console.log(
  `check:pack OK: ${files.length} files, ${info.size} bytes packed, ${info.unpackedSize} bytes unpacked`,
);
