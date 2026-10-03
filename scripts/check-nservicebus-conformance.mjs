// Fails when the NServiceBus TransactionalSession acceptance tests and docs/nservicebus-conformance.md diverge:
// every upstream When_*.cs test must be listed in the matrix, and the matrix must not list tests that no longer exist.
import { readFile } from 'node:fs/promises';

const upstreamUrl =
  'https://api.github.com/repos/Particular/NServiceBus.TransactionalSession/contents/src/NServiceBus.TransactionalSession.AcceptanceTests';
const matrixPath = new URL('../docs/nservicebus-conformance.md', import.meta.url);

const headers = { Accept: 'application/vnd.github+json', 'User-Agent': 'zusammen-conformance-check' };
if (process.env.GITHUB_TOKEN) {
  headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
}

const response = await fetch(upstreamUrl, { headers });
if (!response.ok) {
  console.error(`Failed to list upstream acceptance tests: ${response.status} ${response.statusText}`);
  process.exit(2);
}

const upstream = new Set(
  (await response.json())
    .map((entry) => entry.name)
    .filter((name) => /^When_.*\.cs$/.test(name))
    .map((name) => name.slice(0, -'.cs'.length)),
);

const matrix = new Set(
  (await readFile(matrixPath, 'utf8'))
    .split('\n')
    .map((line) => /^\|\s*`(When_[^`]+)`/.exec(line)?.[1])
    .filter(Boolean),
);

const missing = [...upstream].filter((name) => !matrix.has(name)).sort();
const stale = [...matrix].filter((name) => !upstream.has(name)).sort();

for (const name of missing) console.error(`Missing from conformance matrix: ${name}`);
for (const name of stale) console.error(`No longer upstream, remove or update: ${name}`);

if (missing.length > 0 || stale.length > 0) {
  process.exit(1);
}

console.log(`Conformance matrix covers all ${upstream.size} upstream acceptance tests.`);
