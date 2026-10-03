# Releasing

For maintainers. Every release publishes **all** packages with the **same version**: the 8 `@zusammen/*` npm packages and the `Zusammen.NServiceBus` NuGet package.

## Cutting a release

1. Make sure `main` contains everything to release and CI is green.
2. Tag the commit on `main` with a SemVer version, without prefix, and push the tag:

   ```sh
   git checkout main && git pull
   git tag 0.1.0-alpha.2
   git push origin 0.1.0-alpha.2
   ```

   Tags matching `[0-9]+.[0-9]+.[0-9]+` are stable releases, `[0-9]+.[0-9]+.[0-9]+-*` are prereleases. Only repository admins can create, move or delete them (tag ruleset "Release tags").

3. Watch the [Release workflow](https://github.com/mauroservienti/Zusammen/actions/workflows/release.yml). It:
   1. checks that the tag is strict SemVer and points to a commit on `main`;
   2. runs the full CI, including the container and NServiceBus compatibility tests;
   3. stamps the version into every package (nothing is committed back: the tag is the source of truth), builds, and publishes to npm with provenance. Prereleases get the `next` dist-tag, stable versions `latest`;
   4. packs and pushes `Zusammen.NServiceBus` to NuGet;
   5. creates a GitHub release with notes generated from the merged pull requests and the `.nupkg` attached.

New versions take a few minutes to show up on npm and NuGet after the workflow finishes.

## Credentials

- **npm: trusted publishing**, no secret. Each package trusts `mauroservienti/Zusammen`, workflow `release.yml`, no environment. Check with `npm trust list @zusammen/<package>`. **New packages** can't have a trusted publisher until they exist: publish their first version with a short-lived granular token stored as the `NPM_API_KEY` secret (the workflow uses it when present), then configure trusted publishing (`npm trust github @zusammen/<package> --file release.yml --repo mauroservienti/Zusammen --allow-publish --yes`, requires 2FA) and delete the secret.
- **NuGet: `NUGET_API_KEY` repository secret.** API keys expire: rotate it on nuget.org and update the secret before it does.

## When a release fails

Look at which step failed:

- **Validation or CI**: nothing was published. Fix the problem on `main` (through a pull request), then move the tag to the fixed commit, or tag the next version.
- **Publishing**: re-running the workflow skips versions already on npm and NuGet, so a transient failure (registry outage, expired NuGet key after updating the secret) is fixed by re-running the failed job.
- **A bug in the workflow itself**: a re-run uses the workflow as it was at the tagged commit, so it fails again. Fix the workflow on `main`, then:
  - if nothing was published yet, move the tag: `git tag -d X && git push origin :refs/tags/X`, then tag and push again;
  - otherwise, tag the next version (published versions can't be republished).

## Dist-tags

npm sets `latest` on a package's very first version, whatever tag it's published with, so `0.1.0-alpha.1` is also `latest`. The first stable release takes `latest` over; from then on prereleases only move `next`.
