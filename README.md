# `agenteventprotocol`: the official AEP Python SDK

**Typed emit/consume/control helpers for the
[Agent Event Protocol](https://github.com/agenteventprotocol/agent-event-protocol), built
on the schema-generated pydantic v2 models.**

AEP is an open standard for the events AI agents emit while they work —
sessions, runs, tool calls, attention requests — so any consumer can observe
and steer any agent. This SDK pairs the protocol's generated pydantic models
with the pieces every Python emitter or consumer needs: envelope construction
with per-session `(epoch, seq)` ownership, SSE subscription with dedupe and
resume, and the control command state machine with correlated acks. Python
≥ 3.10; the only dependency is `pydantic>=2`; fully typed (`py.typed`,
PEP 561).

[![CI](https://github.com/agenteventprotocol/python-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/agenteventprotocol/python-sdk/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## Install

The distribution name is `agenteventprotocol`; the import name is `aep_sdk`.

**Unpublished — build from the repo.** The package is not on PyPI yet (the
`Private :: Do Not Upload` classifier stays until the protocol's `v0.1` tag);
until then:

```sh
git clone https://github.com/agenteventprotocol/python-sdk.git
cd python-sdk
uv build                      # sdist + wheel via hatchling
uv pip install dist/agenteventprotocol-*.whl
```

## Quickstart

```python
from aep_sdk import Emitter, http_sink, subscribe

session = Emitter("my-agent", "host-1", http_sink("http://127.0.0.1:8787"), epoch=1) \
    .session("s_001")
session.emit("session.started", {"client": {"name": "my-agent"}})

sub = subscribe("http://127.0.0.1:8787", print, filter={"session": "s_001"})
```

## asyncio

`aep_sdk.aio` mirrors the sync consumer surface as an async iterable —
standard library only (no aiohttp), same id-dedupe, `(session, epoch, seq)`
position tracking, and reconnect-with-resume semantics (both surfaces accept
`from_="all"` for the enumeration-free replay-all cold start, AEP-0003 §5;
both `ControlClient`s offer `roster()`, the live-claim snapshot of AEP-0003
§4.1 — gate it on the endpoint's `capabilities.roster` advertisement):

```python
import asyncio
from aep_sdk import aio


async def main() -> None:
    sub = await aio.subscribe("http://127.0.0.1:8787",
                              filter={"session": "s_001"})
    async for ev in sub:
        print(ev["type"], ev.get("seq"))
        if ev["type"] == "session.ended":
            break
    print("resume from:", sub.positions())
    sub.close()


asyncio.run(main())
```

`await aio.subscribe(...)` returns once the stream is established; pass
`live=False` to iterate buffered history only (iteration ends by itself at
the relay's `replay-complete` marker), and persist `sub.positions()` to
resume later via `from_=`.

Both consumer flavors expose their transport tuning as keyword arguments:
`backoff_initial_ms` / `backoff_max_ms` (the reconnect schedule; defaults
500 ms initial, 10 s max), `from_budget` (the encoded `from` budget riding
the live request; default 6000 encoded characters), and `max_replay_chunks`
(the bound on `live=0` drain requests per reconnect; default 20). A
per-attempt timeout completes the set: `timeout_ms` on the sync surface
(default 30 s; the `urlopen` timeout, covering connect and each read) and
`connect_timeout_ms` on the aio surface (default 30 s; bounding connection
establishment only; body reads stay unbounded so an idle stream is never
cut). `http_sink` takes `timeout_ms` (default 10 s per POST) the same way.

Control sending has the same parity: `aio.ControlClient` owns a WebSocket
duplex (stdlib-only, RFC 6455 client side) and mirrors the sync
`ControlSender.send()` contract — same envelope builder, same
`ack_window_ms`/`retries` semantics with every retry reusing the command
`id`, and the same `NackError` (wire nacks, relay-on-behalf `unsupported`,
or the locally synthesized `timeout`). One idiom difference from the
TypeScript SDK: `aio.ControlClient` requires an explicit `await connect()`
before `send()`, where the TypeScript SDK's `ControlClient` connects in its
constructor.

```python
from aep_sdk import NackError, aio

async def answer(relay: str, session: str, request_id: str) -> None:
    ctl = aio.ControlClient(relay, agent="ops-console", host="my-host")
    await ctl.connect()
    try:
        ack = await ctl.send("control.attention.respond", session,
                             subject=request_id, cause=request_id,
                             data={"answer": {"option": "allow"}})
        print("accepted:", ack["id"])
    except NackError as e:
        print("nacked:", e.reason, e.detail)
    finally:
        await ctl.close()
```

## Synchronous control

The same bundled stdlib WS transport ships in a blocking flavor at the top
level: `ControlClient`, `open_duplex()`, and `Duplex` are the synchronous
twins of the `aio` trio (one shared RFC 6455 byte layout; the blocking
flavor reads on `socket.makefile` and dispatches inbound frames from a
daemon reader thread). `connect()` completes the hello exchange — bounded by
`connect_timeout_ms` (default 30 s, both flavors: one budget over the socket
open, the upgrade, and the hello wait) — `send()` carries the exact
`ControlSender` contract (same envelopes, same `ack_window_ms`/`retries`,
same `NackError`), `roster()` mirrors the aio surface, and `close()` is
idempotent. `open_duplex()` takes the same `connect_timeout_ms` when you
speak the duplex protocol yourself. `ControlSender` remains the
transport-agnostic builder when you own the wire yourself.

```python
from aep_sdk import ControlClient, NackError

ctl = ControlClient("http://127.0.0.1:8787", agent="ops-console", host="my-host")
ctl.connect()
try:
    ack = ctl.send("control.pause", "s_001")
    print("accepted:", ack["id"])
    for entry in ctl.roster():
        print(entry["session"], (entry.get("control") or {}).get("accepts"))
except NackError as e:
    print("nacked:", e.reason, e.detail)
finally:
    ctl.close()
```

## Errors

Two exception types, at two different levels:

- **`NackError`** — protocol-level: a command was delivered and answered
  `control.rejected`, or the relay answered on the target's behalf (e.g.
  `unsupported`), or the local ack window closed on the last retry
  (`synthesized=True`). `reason` is always one of `NACK_REASONS`.
- **`TransportError`** (`ConnectionError` subclass) — delivery itself didn't
  complete as a well-formed exchange. `kind` is one of:
  - `"network"` — a transport-level failure (connect/read/TLS/WebSocket
    framing/a handshake that lies).
  - `"http"` — the relay ANSWERED with a refusing HTTP status (non-2xx
    ingest/SSE, non-101 upgrade; carries `status`).
  - `"parse"` — a payload arrived but its JSON is undecodable — the frame is
    still dropped, and the drop is reported.

A parse failure never raises into the stream: `subscribe()` and
`aio.subscribe()` report it to `on_error` (when supplied) and drop the frame.

Timeouts live in this taxonomy too: an expired `timeout_ms` /
`connect_timeout_ms` (on `subscribe()`, `http_sink()`, `open_duplex()`, or
`ControlClient.connect()`) surfaces as a `network` `TransportError`, never a
raw `socket.timeout` or `asyncio.TimeoutError`. WebSocket connects and the
aio SSE connect fail after 30 s by default on a peer that accepts the socket
but never answers.

## Testing utilities

`aep_sdk.testing` ships the test doubles an emitter, consumer, or control
test needs — **relay-free**. (The smoke's vendored relay fixture is a Node
program and is not usable from a pure-Python install; these exported
utilities are the supported way to test code built on this SDK.) Everything
rides the SDK's own envelope machinery, so what a test observes is what
production code emits:

- **`MemorySink`** — a `Sink` that records: pass the instance to `Emitter`,
  read `.events`, `.clear()` between cases.
- **`ScriptedSource`** — scripts a whole source with real envelopes:
  `session(id, epoch=...)` returns a genuine `SessionEmitter` (fresh ULID
  ids, contiguous per-session `seq` from 0); `.events` accumulates across
  sessions and `.play(on_event)` feeds any consumer.
- **`ControlStub`** — both sides of the control plane (AEP-0004):
  target-side, `.emitter` is a real `SessionEmitter` to hand to
  `ControlTarget` with every ack recorded in `.acks`, and `.command(...)`
  mints well-formed command frames (fresh id, target `session`, no `seq`);
  client-side, pass `.send` to `ControlSender` as its transport, read the
  captured frames in `.sent`, and answer them with `.accept(cmd)` /
  `.reject(cmd, reason=...)` via `sender.on_event()`.

```python
from aep_sdk import ControlTarget
from aep_sdk.testing import ControlStub

stub = ControlStub()
target = ControlTarget(stub.emitter, accepts=["control.pause"])
target.handle(stub.command("control.pause", "s_001", data={}), print)
assert stub.acks[-1]["type"] == "control.accepted"
```

## State projection

`StateProjection` (incremental `apply()`) and `project_state(events)` (batch)
fold a stream of events into the current per-session state — the
"what is true now" read model a dashboard or supervisor needs:

```python
from aep_sdk import project_state

state = project_state(events)
# {"sessions": [{"source": ..., "session": ..., "agent": ...,
#                "started": ..., "ended": ...,
#                "position": {"epoch": 0, "seq": 8},
#                "runs": [{"run": ..., "status": "finished",
#                          "started": ..., "ended": ...}],
#                "pending": [{"id": ..., "kind": "form", "since": ...}]}],
#  "violations": [{"rule": "seq-regression", "source": ..., ...}]}
```

The fold mirrors the reference CLI's read-side disciplines: dedupe on
`(source, id)` with byte-identical redeliveries collapsing silently and
same-key collisions reported (AEP-0001 §7.4), sessions keyed
`(source, session)` — emitter-scoped identity (AEP-0001 §5.2) — `(epoch,
seq)` ordering with regressions reported, never repaired (AEP-0001 §7), run
terminal exclusivity (AEP-0002 §2 convention 5), and pending attention where
`attention.resolved`/`attention.timeout` clear a request and
`attention.answered` deliberately does not (AEP-0002 §7.2). Command frames
and `agent.*` events are deduplicated but never folded into session state.
`project_state` batch-sorts each session by `(epoch, seq)` before folding;
unseen timestamps and positions are `None` (JSON `null`). The golden corpus
under `tests/fixtures/projection/` is vendored byte-identically in the
TypeScript SDK, so both implementations answer to one definition.

## Layout

| Path | What |
|---|---|
| `aep_sdk/gen/aep_types.py` | **Generated** from the protocol's schema registry — `AepEvent` + payload models. Never edit; CI regenerates and diffs it against the spec repo pinned in `SPEC_VERSION` |
| `aep_sdk/emit.py` | `Emitter` / `SessionEmitter` (per-session `(epoch, seq)` ownership), `http_sink`, `jsonl_sink`, `ulid` |
| `aep_sdk/consume.py` | `subscribe()`: SSE + attr-match in a daemon thread, id-dedupe, resume `positions()`. The resume set is bounded on the wire: the newest positions ride the live request (a ~6 KB budget keeps the URL far under server header limits), older ones drain through bounded replay requests, and a relay refusing a resume-carrying request (4xx) gets a shrinking retry — resume is an optimization, never worth a dead stream |
| `aep_sdk/aio.py` | asyncio mirror of the consumer AND the control sender: `aio.subscribe()` → `AsyncSubscription` (async-iterable, stdlib-only transport incl. chunked SSE decoding) · `aio.ControlClient` (async `send()` mirroring the sync contract over an owned stdlib WebSocket duplex, `aio.open_duplex()`) |
| `aep_sdk/control.py` | `ControlSender` (blocking `send()` with correlated ack/nack, window timeout, retries reusing the command `id`) + `ControlTarget` (dedupe/ack/`unsupported`-nack helper) + the bundled sync WS transport: `ControlClient` / `open_duplex()` / `Duplex`, the blocking twins of the `aio` trio (stdlib `socket`; frame/handshake byte layout shared with `aio` via the private `_ws` module) — see [Synchronous control](#synchronous-control). `ControlSender` stays transport-agnostic for caller-owned wires: inject `send`, feed inbound events to `on_event()` |
| `aep_sdk/projection.py` | `StateProjection` / `project_state`: fold events into current per-session state (runs, pending attention, `(epoch, seq)` positions) with conformance violations reported — mirrors the reference CLI's folds; see [State projection](#state-projection) |
| `aep_sdk/testing.py` | Exported test doubles, relay-free: `MemorySink`, `ScriptedSource`, `ControlStub` — see [Testing utilities](#testing-utilities) |
| `aep_sdk/errors.py` | `TransportError` (`ConnectionError` subclass): the `network`/`http`/`parse` taxonomy raised or reported by the SSE and WebSocket transports — see [Errors](#errors) |
| `tests/` | Smoke (`tests/run-smoke.sh`): a relay-free pass over the exported testing utilities and the state projection (golden corpus in `tests/fixtures/projection/`), then envelope/payload validation, the full control machine (fake-duplex sync, bundled sync WS, and async — each against a scripted target), and the live HTTP→SSE path against a vendored snapshot of the reference relay (`tests/fixtures/relay/`, a Node program) |
| `RELEASING.md` | How the package ships: the tag-triggered publish workflow (PyPI trusted publishing) and the maintainer procedure around it |

## Verify

```sh
bash tests/run-smoke.sh   # needs uv + Node >= 22 (the relay fixture is Node)
```

Typing: the package ships `py.typed`, so pyright/mypy resolve every public
symbol — CI checks a typed consumer sample (`tests/typing/consumer.py`)
against a fresh wheel install.

`SPEC_VERSION` pins the exact
[agent-event-protocol](https://github.com/agenteventprotocol/agent-event-protocol) commit
the committed generated models were produced from; CI regenerates from that
pin and fails on any diff.

## Versioning

This package implements AEP 0.1.

Pre-1.0, a **minor version bump may be breaking** (the compatibility boundary
set by the protocol's
[GOVERNANCE](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/GOVERNANCE.md)).
The SDK versions independently of the protocol; the protocol version an event
carries is its `aep` attribute. The version string is single-sourced from
`aep_sdk/__init__.py` (`hatchling` `dynamic = ["version"]`). See
[RELEASING.md](RELEASING.md) for the full versioning policy.

## Links

- [Specification](https://github.com/agenteventprotocol/agent-event-protocol) — spec,
  schema registry, conformance fixtures, docs
- [TypeScript SDK](https://github.com/agenteventprotocol/typescript-sdk)
- [Reference stack](https://github.com/agenteventprotocol/reference) — relay, CLI,
  adapters, bridges, demo

## License

Apache-2.0 — see [LICENSE](LICENSE).
