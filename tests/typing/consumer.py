"""Typed-consumer sample: a type checker must see ``aep_sdk`` as fully typed
(PEP 561 ``py.typed``). CI runs pyright over this file against a fresh-venv
wheel install, so the check proves the *published artifact* carries its types
— an untyped package would fail import resolution here.
"""
from __future__ import annotations

from aep_sdk import AepEvent, Emitter, SessionEmitter, ToolCompletedData

events: list[dict] = []
em: Emitter = Emitter("typing-check", "host-1", events.append, epoch=1)
session: SessionEmitter = em.session("s_typing")

ev: dict = session.emit("session.started", {"client": {"name": "typing-check"}})

parsed: AepEvent = AepEvent.model_validate(ev)
session_id: str | None = parsed.session
seq: int | None = parsed.seq

data: ToolCompletedData = ToolCompletedData.model_validate(
    {"tool": {"name": "Bash", "call_id": "t_1"}, "status": "success", "duration_ms": 12}
)
duration_ms: int | None = data.duration_ms

# asyncio surface: the aio module must be visible to the checker with full types
from aep_sdk import aio


async def consume_async(relay: str) -> list[dict]:
    sub: aio.AsyncSubscription = await aio.subscribe(
        relay, filter={"session": "s_typing"}, live=False,
        backoff_initial_ms=500, backoff_max_ms=10_000,
        from_budget=6_000, max_replay_chunks=20, connect_timeout_ms=5_000)
    got: list[dict] = []
    async for aio_ev in sub:
        got.append(aio_ev)
    positions: list[dict] = sub.positions()
    assert isinstance(positions, list)
    sub.close()
    return got


async def control_async(relay: str) -> None:
    from aep_sdk import NackError

    ctl: aio.ControlClient = aio.ControlClient(relay, agent="typing-check", host="host-1")
    await ctl.connect(connect_timeout_ms=5_000)
    try:
        ack: dict = await ctl.send(
            "control.attention.respond", "s_typing",
            subject="01TYPINGREQUESTID000000000",
            data={"answer": {"option": "allow"}},
            ack_window_ms=5_000, retries=1)
        cause: str | None = ack.get("cause")
        assert cause is None or isinstance(cause, str)
    except NackError as e:
        reason: str = e.reason
        synthesized: bool = e.synthesized
        assert reason and isinstance(synthesized, bool)
    finally:
        await ctl.close()


# sync control surface: the bundled stdlib WS transport (blocking twin of aio)
from aep_sdk import ControlClient, Duplex, open_duplex


def control_sync(relay: str) -> None:
    from aep_sdk import NackError

    ctl: ControlClient = ControlClient(relay, agent="typing-check", host="host-1",
                                       token=None)
    ctl.connect(connect_timeout_ms=5_000)
    try:
        ack: dict = ctl.send(
            "control.pause", "s_typing",
            data={"scope": "run"},
            ack_window_ms=5_000, retries=1)
        ack_cause: str | None = ack.get("cause")
        assert ack_cause is None or isinstance(ack_cause, str)
        entries: list = ctl.roster(timeout_ms=5_000)
        assert isinstance(entries, list)
    except NackError as e:
        sync_reason: str = e.reason
        sync_synthesized: bool = e.synthesized
        assert sync_reason and isinstance(sync_synthesized, bool)
    finally:
        ctl.close()


def duplex_sync(relay: str) -> None:
    duplex: Duplex = open_duplex(relay, token=None, connect_timeout_ms=5_000)
    duplex.send_json({"type": "hello", "versions": ["0.1"], "role": "consumer",
                      "identity": {"agent": "typing-check", "instance": "host-1"},
                      "capabilities": {}})
    reply: dict | None = duplex.recv_json()
    assert reply is None or isinstance(reply, dict)
    duplex.close()


async def duplex_async(relay: str) -> None:
    aio_duplex: aio.Duplex = await aio.open_duplex(relay, token=None,
                                                   connect_timeout_ms=5_000)
    await aio_duplex.close()


# transport tuning knobs on the sync emit/consume surfaces
from aep_sdk import Subscription, http_sink, subscribe


def consume_sync(relay: str) -> Subscription:
    sink = http_sink(relay, token=None, on_error=None, timeout_ms=5_000)
    sink(ev)
    sub: Subscription = subscribe(
        relay, events.append, filter={"session": "s_typing"},
        backoff_initial_ms=500, backoff_max_ms=10_000,
        from_budget=6_000, max_replay_chunks=20, timeout_ms=15_000)
    positions: list[dict] = sub.positions()
    assert isinstance(positions, list)
    sub.close()
    return sub


# exported test doubles + state projection: relay-free, fully typed
from aep_sdk import StateProjection, project_state
from aep_sdk.testing import ControlStub, MemorySink, ScriptedSource

recorder: MemorySink = MemorySink()
recorder_em: Emitter = Emitter("typing-check", "host-1", recorder)
recorder_em.session("s_mem").emit("session.started", {"client": {"name": "typing-check"}})
recorded: list[dict] = recorder.events
recorder.clear()

scripted: ScriptedSource = ScriptedSource(agent="typing-check")
scripted_session: SessionEmitter = scripted.session("s_typing", epoch=1)
scripted_session.emit("progress.status", {})
scripted_events: list[dict] = scripted.events
scripted.play(events.append)

stub: ControlStub = ControlStub(session="s_typing")
target_emitter: SessionEmitter = stub.emitter
command: dict = stub.command("control.pause", "s_typing", data={})
accepted: dict = stub.accept(command)
rejected: dict = stub.reject(command, reason="busy", detail="mid-run")
outbound: list[dict] = stub.sent
acks: list[dict] = stub.acks

projection: StateProjection = StateProjection()
projection.apply(ev)
snapshot: dict = projection.state()
batch: dict = project_state(scripted_events)
assert isinstance(snapshot["sessions"], list) and isinstance(batch["violations"], list)
