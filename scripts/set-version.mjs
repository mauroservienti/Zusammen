// Stamps a version into every workspace package before publishing; the release tag is the only source of truth,
// nothing is committed back.
import { readdir, readFile, writeFile } from 'node:fs/promises';

const version = process.argv[2];
// Official SemVer 2.0 regular expression (semver.org)
const semver =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/;

if (version === undefined || !semver.test(version)) {
  console.error(`Not a SemVer version: '${String(version)}'`);
  process.exit(1);
}

const packagesDir = new URL('../packages/', import.meta.url);
for (const entry of await readdir(packagesDir, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const path = new URL(`${entry.name}/package.json`, packagesDir);
  const manifest = JSON.parse(await readFile(path, 'utf8'));
  manifest.version = version;
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(`${manifest.name}@${version}`);
}
