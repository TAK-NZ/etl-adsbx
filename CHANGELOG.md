# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v1.3.1

- :bug: When `ADSBX_Filtering` is enabled, always additively include aircraft squawking an emergency status, regardless of `ADSBX_Includes` or `ADSBX_Include_Below_Elevation`. Previously an emergency aircraft outside the configured filtering scope was silently dropped before ever reaching CloudTAK, even though `ADSBX_Emergency_Alert` would have flagged it had it been included

### v1.3.0

- :tada: Add `capabilities.json` manifest (validated against `@tak-ps/etl`'s `StaticCapabilitiesSchema`) describing this task's permissions, compute requirements and invocation modes, embedded in the pushed image as the `com.cloudtak.capabilities` OCI annotation via `docker buildx` in CI
- :tada: Add `ADSBX_Include_Below_Elevation` / `ADSBX_Below_Elevation_Feet` options to additively include all aircraft below a configurable altitude when `ADSBX_Filtering` is enabled, regardless of the `ADSBX_Includes` list
- :tada: Add `included` flag to output metadata, indicating whether an aircraft matched the `ADSBX_Includes` list (by ICAO hex or registration)
- :white_check_mark: Add basic test suite (`npm test`) covering the task's static config and Input/Output schemas
- :rocket: Switch to `Task.init()` for local-dev ETL_TOKEN auto-generation (no behavior change in Lambda)
- :arrow_up: Bump CI and `engines` Node version requirement to 24, matching the Dockerfile runtime and `@tak-ps/etl`'s own requirement
- :arrow_up: Update dependencies (`@tak-ps/etl` 10.13.0 → 10.18.0, `eslint` 10.9.0 → 10.10.0, `typescript-eslint` 8.67.0 → 8.70.0) and resolve all npm audit advisories (`fast-uri`, `morgan`, `qs`, `@humanfs/node`); `typescript` stays pinned to `^6.0.3` until `typescript-eslint` supports 7.x (its peer dependency currently caps at `<6.1.0`)

### v1.2.11

- :rocket: Update NZ public safety aircraft roster (add current rescue helicopters, remove deregistered aircraft, refresh station/callsign details) and sort by domain/group/registration
- :arrow_up: Update dependencies to latest and resolve npm audit advisories

### v1.0.0

- :tada: Initial Commit based on https://github.com/dfpc-coe/etl-adsbx
