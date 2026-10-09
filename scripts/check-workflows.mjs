// Guards the security-relevant shape of the workflow files by matching strings in their text.
// Fails (exit 1) on any violation. Cheap and dependency-free; actionlint covers the syntax.
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const dir = resolve(import.meta.dirname, '..', '.github', 'workflows');
const read = (name) => readFileSync(join(dir, name), 'utf8');
const stripComments = (text) =>
  text
    .split('\n')
    .map((line) => line.replace(/(^|\s)#.*$/, ''))
    .join('\n');

// Non-empty, comment-free, trimmed lines of the top-level `on:` block.
function onBlock(text) {
  const lines = stripComments(text).split('\n');
  const start = lines.findIndex((l) => /^on:\s*$/.test(l));
  if (start === -1) return null;
  const block = [];
  for (const line of lines.slice(start + 1)) {
    if (/^\S/.test(line)) break;
    if (line.trim() !== '') block.push(line.trim());
  }
  return block;
}

const violations = [];
const release = read('release.yml');
const ci = read('ci.yml');

// release.yml: triggers only on push to main.
const releaseOn = onBlock(release);
if (!releaseOn) {
  violations.push('release.yml: missing top-level `on:` block');
} else if (releaseOn.join('|') !== 'push:|branches: [main]') {
  violations.push(
    `release.yml: \`on:\` must be exactly push -> branches: [main], found: ${releaseOn.join(' / ')}`,
  );
}

// release.yml: OIDC permission present, no npm token anywhere.
if (!/^\s*id-token:\s*write\s*$/m.test(stripComments(release))) {
  violations.push('release.yml: missing `id-token: write` (required for npm trusted publishing)');
}
if (/NPM_TOKEN/.test(release)) {
  violations.push('release.yml: must not reference NPM_TOKEN (use trusted publishing)');
}
if (!release.includes("github.repository == 'loipv/nestjs-nats'")) {
  violations.push(
    "release.yml: release job must be gated on github.repository == 'loipv/nestjs-nats'",
  );
}

// ci.yml: read-only, no secrets for publishing.
const ciBody = stripComments(ci);
if (!/^permissions:\s*\n\s+contents:\s*read\s*$/m.test(ciBody)) {
  violations.push('ci.yml: top-level `permissions:` must be `contents: read`');
}
if (/:\s*write\s*$/m.test(ciBody)) {
  violations.push('ci.yml: must not request any `write` permission');
}
if (/NPM_TOKEN|id-token/.test(ciBody)) {
  violations.push('ci.yml: must not reference NPM_TOKEN or id-token');
}

if (violations.length > 0) {
  console.error('check-workflows failed:');
  for (const v of violations) console.error(`  - ${v}`);
  process.exit(1);
}
console.log(
  'check-workflows: ok (release.yml push-to-main only, id-token: write, no NPM_TOKEN; ci.yml read-only)',
);
