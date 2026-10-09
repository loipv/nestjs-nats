// Guards the security-relevant shape of the workflow files by matching strings in their text.
// Fails (exit 1) on any violation. Cheap and dependency-free; actionlint covers the syntax.
// Rules: release.yml is the only workflow allowed to publish; every other workflow is read-only.
import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const dir = resolve(import.meta.dirname, '..', '.github', 'workflows');
const stripComments = (text) =>
  text
    .split('\n')
    .map((line) => line.replace(/(^|\s)#.*$/, ''))
    .join('\n');

// Non-empty, comment-free, trimmed lines of the top-level `on:` block.
function onBlock(text) {
  const lines = text.split('\n');
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
const files = readdirSync(dir)
  .filter((f) => f.endsWith('.yml') || f.endsWith('.yaml'))
  .sort();

if (!files.includes('release.yml')) violations.push('release.yml is missing');
if (!files.includes('ci.yml')) violations.push('ci.yml is missing');

for (const file of files) {
  const body = stripComments(readFileSync(join(dir, file), 'utf8'));

  if (/NPM_TOKEN/.test(body)) {
    violations.push(`${file}: must not reference NPM_TOKEN (use trusted publishing)`);
  }

  if (file === 'release.yml') {
    // Triggers only on push to main.
    const on = onBlock(body);
    if (!on) {
      violations.push('release.yml: missing top-level `on:` block');
    } else if (on.join('|') !== 'push:|branches: [main]') {
      violations.push(
        `release.yml: \`on:\` must be exactly push -> branches: [main], found: ${on.join(' / ')}`,
      );
    }
    // OIDC permission for npm trusted publishing.
    if (!/^\s*id-token:\s*write\s*$/m.test(body)) {
      violations.push(
        'release.yml: missing `id-token: write` (required for npm trusted publishing)',
      );
    }
    // Only runs in the canonical repository.
    if (!/^\s*if:\s*github\.repository\s*==\s*'loipv\/nestjs-nats'\s*$/m.test(body)) {
      violations.push(
        "release.yml: release job needs `if: github.repository == 'loipv/nestjs-nats'`",
      );
    }
    // The only secret it may use is the built-in GITHUB_TOKEN.
    for (const [, name] of body.matchAll(/secrets\.([A-Za-z0-9_]+)/g)) {
      if (name !== 'GITHUB_TOKEN') {
        violations.push(`release.yml: only secrets.GITHUB_TOKEN is allowed, found secrets.${name}`);
      }
    }
  } else {
    // Every other workflow is read-only and cannot publish.
    if (!/^permissions:\s*\n\s+contents:\s*read\s*$/m.test(body)) {
      violations.push(`${file}: top-level \`permissions:\` must be \`contents: read\``);
    }
    if (/:\s*write\s*$/m.test(body)) {
      violations.push(`${file}: must not request any \`write\` permission`);
    }
    if (/id-token/.test(body)) {
      violations.push(`${file}: must not reference id-token`);
    }
    if (/secrets\./.test(body)) {
      violations.push(`${file}: must not reference secrets`);
    }
  }
}

if (violations.length > 0) {
  console.error('check-workflows failed:');
  for (const v of violations) console.error(`  - ${v}`);
  process.exit(1);
}
console.log(
  `check-workflows: ok (${files.join(', ')}; release.yml push-to-main only, id-token: write, no NPM_TOKEN; others read-only)`,
);
