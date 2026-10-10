// Guards the security-relevant shape of the workflow files by matching strings in their text.
// Fails (exit 1) on any violation. Cheap and dependency-free; actionlint covers the syntax.
// Rules: release.yml is the only workflow allowed to publish; every other workflow is read-only;
// every action is pinned by commit SHA.
import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const dir = resolve(import.meta.dirname, '..', '.github', 'workflows');
const stripComments = (text) =>
  text
    .split('\n')
    .map((line) => line.replace(/(^|\s)#.*$/, ''))
    .join('\n');

// The top-level `on:` block as { trigger: [trimmed child lines] }, from comment-free text.
function onBlock(text) {
  const lines = text.split('\n');
  const start = lines.findIndex((l) => /^on:\s*$/.test(l));
  if (start === -1) return null;
  const body = [];
  for (const line of lines.slice(start + 1)) {
    if (/^\S/.test(line)) break;
    if (line.trim() !== '') body.push(line);
  }
  if (body.length === 0) return {};
  const indent = (l) => l.length - l.trimStart().length;
  const top = Math.min(...body.map(indent));
  const triggers = {};
  let current = null;
  for (const line of body) {
    if (indent(line) === top) {
      current = line.trim();
      triggers[current] = [];
    } else {
      triggers[current].push(line.trim());
    }
  }
  return triggers;
}

const violations = [];
const files = readdirSync(dir)
  .filter((f) => f.endsWith('.yml') || f.endsWith('.yaml'))
  .sort();

if (!files.includes('release.yml')) violations.push('release.yml is missing');
if (!files.includes('ci.yml')) violations.push('ci.yml is missing');

for (const file of files) {
  const raw = readFileSync(join(dir, file), 'utf8');
  const body = stripComments(raw);

  // Every action is pinned by full commit SHA with a `# vX.Y.Z` comment (Dependabot keeps both
  // current); tags are mutable. Local actions (`./...`) and `docker://` images are exempt.
  for (const [, ref, rest] of raw.matchAll(/^\s*(?:-\s*)?uses:\s*(\S+)(.*)$/gm)) {
    if (ref.startsWith('./') || ref.startsWith('docker://')) continue;
    if (!/^[^@]+@[0-9a-f]{40}$/.test(ref) || !/^\s+#\s*v\d+\.\d+\.\d+\s*$/.test(rest)) {
      violations.push(
        `${file}: \`${ref}\` must be pinned by full commit SHA with a \`# vX.Y.Z\` comment`,
      );
    }
  }

  if (/NPM_TOKEN/.test(body)) {
    violations.push(`${file}: must not reference NPM_TOKEN (use trusted publishing)`);
  }

  if (file === 'release.yml') {
    // Triggers only on push to main, plus an optional manual workflow_dispatch (no inputs).
    // Never pull_request / pull_request_target: they would run with fork-controlled code.
    if (/\bpull_request/.test(body)) {
      violations.push('release.yml: must not use a pull_request or pull_request_target trigger');
    }
    const on = onBlock(body);
    if (!on) {
      violations.push('release.yml: missing top-level `on:` block');
    } else {
      const found = Object.entries(on)
        .map(([k, v]) => [k, ...v].join(' '))
        .join(' / ');
      const pushOk = on['push:']?.join('|') === 'branches: [main]';
      const dispatchOk = (k) =>
        (k === 'workflow_dispatch:' || k === 'workflow_dispatch: {}') && on[k].length === 0;
      const extra = Object.keys(on).filter((k) => k !== 'push:' && !dispatchOk(k));
      if (!pushOk || extra.length > 0) {
        violations.push(
          `release.yml: \`on:\` must be exactly push -> branches: [main] (plus optional workflow_dispatch), found: ${found}`,
        );
      }
    }
    // OIDC permission for npm trusted publishing.
    if (!/^\s*id-token:\s*write\s*$/m.test(body)) {
      violations.push(
        'release.yml: missing `id-token: write` (required for npm trusted publishing)',
      );
    }
    // Only runs in the canonical repository, and only on main (a manual run can pick any ref).
    if (
      !/^\s*if:\s*github\.repository\s*==\s*'loipv\/nestjs-nats'\s*&&\s*github\.ref\s*==\s*'refs\/heads\/main'\s*$/m.test(
        body,
      )
    ) {
      violations.push(
        "release.yml: release job needs `if: github.repository == 'loipv/nestjs-nats' && github.ref == 'refs/heads/main'`",
      );
    }
    // No dependency lifecycle scripts while the job holds id-token and write permissions.
    for (const [cmd] of body.matchAll(/\bnpm\s+(?:ci|install|i)\b.*$/gm)) {
      if (!/\s--ignore-scripts\b/.test(cmd)) {
        violations.push(`release.yml: \`${cmd.trim()}\` must pass --ignore-scripts`);
      }
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
  `check-workflows: ok (${files.join(', ')}; actions pinned by SHA; release.yml push-to-main (+ workflow_dispatch) only, id-token: write, --ignore-scripts, no NPM_TOKEN; others read-only)`,
);
