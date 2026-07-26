## What and why

Brief description of what this PR changes and why.

## Checklist

- [ ] `tests/run-smoke.sh` passes locally
- [ ] `aep_sdk/gen/` is never hand-edited (it's regenerated from the
      protocol repository at the `SPEC_VERSION` pin; the `regen-diff` CI
      job enforces byte-identity)
- [ ] Public-API changes follow the SemVer rules in `RELEASING.md` and get
      a `CHANGELOG.md` entry under Unreleased
- [ ] README updated where usage changed
