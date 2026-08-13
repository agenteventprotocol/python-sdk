# Releasing

How `agenteventprotocol` ships to PyPI. The versioning **policy** is the
organization's canonical
[VERSIONING.md](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/VERSIONING.md)
in the protocol repository: the package versions independently under
SemVer, decoupled from the protocol version; the README declares which
protocol version(s) the package implements. The version string is
single-sourced in `aep_sdk/__init__.py` (`__version__`); `pyproject.toml`
reads it via hatchling's `dynamic = ["version"]`. The next section
applies the policy to this package.

## Versioning policy

`agenteventprotocol` follows [SemVer 2.0.0](https://semver.org/). This is the
package-track instance of the canonical org-wide policy — see the
[protocol repository's `VERSIONING.md`](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/VERSIONING.md)
for the full cross-repository rules; what follows is how that policy applies
here.

**PEP 440 ↔ SemVer.** PyPI releases use
[PEP 440](https://peps.python.org/pep-0440/) version identifiers, but the
release *core* — `MAJOR.MINOR.PATCH` — is identical to the sibling
[`typescript-sdk`](https://github.com/agenteventprotocol/typescript-sdk)'s
SemVer core for the same release, and `MAJOR`/`MINOR`/`PATCH` mean the same
thing in both. Only the pre-release spelling differs by ecosystem: this
package's current version, `0.1.0.dev0`, is the same version core as npm's
`0.1.0-dev` — a PEP 440 developmental release is the ecosystem-native
way to spell what npm spells with a `-dev` pre-release tag. Development and
pre-release segments (`.devN`, `aN`, `bN`, `rcN`) are PEP 440 mechanics, not
a departure from SemVer semantics.

**Public API.** Semver applies to the package's declared public API: the
documented import surface of `aep_sdk` — `Emitter`, `SessionEmitter`,
`http_sink`, `jsonl_sink`, `ulid`, `Subscription`, `subscribe`,
`NACK_REASONS`, `ControlSender`, `ControlTarget`, `NackError`,
`ControlClient`, `Duplex`, `open_duplex` (the sync control surface),
`StateProjection`, `project_state`, the `aio`
submodule (`aio.subscribe`, `aio.AsyncSubscription`, `aio.ControlClient`,
`aio.Duplex`, `aio.open_duplex`), the `testing` submodule
(`testing.MemorySink`, `testing.ScriptedSource`, `testing.ControlStub`),
and the generated protocol types
(`aep_sdk.gen.aep_types`, re-exported at both `aep_sdk` top level and
`aep_sdk.types` — `AepEvent`, `ToolCompletedData`, and the rest of the
schema-generated models). `__version__` is part of the public API as the
version single-source. Underscore-prefixed names, anything not reachable
from the paths above (e.g. internals of `emit.py`/`control.py`/`aio.py` not
listed here), the `tests/` package, and the vendored relay test fixture
(`tests/fixtures/relay/`, a Node program used only by the local smoke run)
are **not** public API and may change in any release.

**Pre-1.0 honesty.** Before `1.0.0`, a **minor** version bump may contain a
breaking change — semver's pre-1.0 allowance, used deliberately rather than
jumping straight to `1.0.0`. Every breaking change, whatever the bump, lands
with a `CHANGELOG.md` entry that states what breaks and how to migrate.

**Generated code.** `aep_sdk/gen/` is regenerated from the protocol
repository's schemas at the commit pinned in `SPEC_VERSION`, but it is
versioned exactly like hand-written code: a regeneration that changes the
public surface (a field, a type, a model) follows the same SemVer rules as
any other change, and its `CHANGELOG.md` entry names the schema change that
caused it — not just "regenerated types."

## The release is the tag

[`.github/workflows/release.yml`](.github/workflows/release.yml) publishes
to PyPI on any `v*` tag push, via
[trusted publishing](https://docs.pypi.org/trusted-publishers/) (OIDC — no
long-lived token secret anywhere). No tags exist yet, so the workflow is
dormant. Before publishing it re-verifies the shape, and three guards make a
stray tag safe:

1. `uv build` and the smoke (envelope + control machine + live HTTP→SSE)
   must pass;
2. the tag must equal `__version__` exactly (`v0.1.0` ⇔ `0.1.0`);
3. the `Private :: Do Not Upload` classifier must be gone from
   `pyproject.toml` — the pre-release shape can never reach the registry.

## Cutting a release (maintainer-run; tagging is never automated)

1. **Sibling agreement — a local pre-tag step.** The two SDK packages
   version independently, but when they release together they ride the same
   release number. With a checkout of
   [`typescript-sdk`](https://github.com/agenteventprotocol/typescript-sdk)
   next to this repository, compare:

   ```sh
   grep -oE '__version__ = "[^"]+"' aep_sdk/__init__.py
   node -p 'require("../typescript-sdk/package.json").version'
   ```

   The release cores (`major.minor.patch`) must agree; only the dev-suffix
   convention differs by ecosystem (PyPI `0.1.0.dev0`, npm `0.1.0-dev`).
   This stays a documented local step: the repositories cannot see each
   other from CI.

2. **Make the shape publishable — one commit.** Remove the
   `Private :: Do Not Upload` classifier from `pyproject.toml`, set
   `__version__` in `aep_sdk/__init__.py` to the release (e.g. `0.1.0`),
   roll `CHANGELOG.md` (`[Unreleased]` → `[0.1.0] — <date>`), and confirm
   the README's "implements AEP x.y" line still holds.

3. **Verify locally** on that commit — the same steps the workflow runs:

   ```sh
   uv build
   bash tests/run-smoke.sh
   ```

4. **Tag and push the tag.**

   ```sh
   git tag -a v0.1.0 -m "agenteventprotocol 0.1.0"
   git push origin v0.1.0
   ```

   The workflow does the rest. If publishing fails after the tag exists,
   never move or delete the tag — fix forward and cut the next patch.

## One-time registry setup (before the first tag)

- Create the PyPI project's
  [trusted publisher](https://docs.pypi.org/trusted-publishers/) for
  `agenteventprotocol`: owner `agenteventprotocol`, repository `python-sdk`, workflow
  `release.yml` (no environment). Until that exists, the publish step is
  rejected by PyPI even on a valid tag.
