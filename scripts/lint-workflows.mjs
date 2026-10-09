// Lints every .github/workflows/*.yml with actionlint, run through Docker.
// Each file is piped over stdin, so no bind mount is needed (bind mounts hang on some local
// Docker hosts such as Colima). Honors DOCKER_HOST from the environment.
import { spawnSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const dir = resolve(import.meta.dirname, '..', '.github', 'workflows');
const files = readdirSync(dir)
  .filter((f) => f.endsWith('.yml') || f.endsWith('.yaml'))
  .sort();

if (files.length === 0) {
  console.error('lint-workflows: no workflow files found');
  process.exit(1);
}

let failed = false;
for (const file of files) {
  const result = spawnSync(
    'docker',
    ['run', '-i', '--rm', 'rhysd/actionlint:1.7.7', '-color', '-'],
    { input: readFileSync(join(dir, file)), encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 },
  );
  if (result.error) {
    console.error(`lint-workflows: cannot run docker: ${result.error.message}`);
    process.exit(1);
  }
  if (result.status === 0) {
    console.log(`ok   ${file}`);
  } else {
    failed = true;
    console.error(`FAIL ${file}`);
    process.stdout.write(result.stdout ?? '');
    process.stderr.write(result.stderr ?? '');
  }
}
process.exit(failed ? 1 : 0);
