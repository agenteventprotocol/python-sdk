"""agenteventprotocol-sdk smoke: the exported testing utilities (MemorySink / ScriptedSource /
ControlStub) and the state projection (StateProjection / project_state) over
the golden corpus in tests/fixtures/projection/. Relay-free by design: runs
without AEP_RELAY. Run via tests/run-smoke.sh."""
from __future__ import annotations

import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aep_sdk import (  # noqa: E402
    ControlSender, ControlTarget, Emitter, NackError, SessionEmitter,
    StateProjection, project_state,
)
from aep_sdk.testing import ControlStub, MemorySink, ScriptedSource  # noqa: E402

failures = 0


def check(name: str, ok: bool) -> None:
    global failures
    print(f"{'PASS' if ok else 'FAIL'}  {name}")
    if not ok:
        failures += 1


def canon(obj: object) -> str:
    """Canonical sorted-keys JSON — byte-identity and state comparison."""
    return json.dumps(obj, sort_keys=True)


# ---- MemorySink: a Sink that records ---------------------------------------------
sink = MemorySink()
mem_em = Emitter("mem-agent", "mem-host", sink)
mem_s = mem_em.session("s-mem")
mem_1 = mem_s.emit("session.started", {"client": {"name": "testing-smoke"}})
mem_2 = mem_s.emit("session.ended", {"reason": "normal"})
check("MemorySink is a Sink: Emitter-built events recorded in order",
      sink.events == [mem_1, mem_2])
sink.clear()
check("MemorySink.clear() empties the record", sink.events == [])

# ---- ScriptedSource: SessionEmitter over an internal recorder --------------------
src = ScriptedSource()
sx = src.session("s-x")
check("ScriptedSource.session returns a SessionEmitter over the stub recorder",
      isinstance(sx, SessionEmitter))
sx_1 = sx.emit("session.started", {"client": {"name": "testing-smoke"}})
sx_2 = sx.emit("progress.status", {})
sx_3 = sx.emit("session.ended", {"reason": "normal"})
check("ScriptedSource envelopes carry the stub source (aep:// URI) and agent",
      all(ev["aep"] == "0.1"
          and isinstance(ev.get("source"), str) and ev["source"].startswith("aep://")
          and isinstance(ev.get("agent"), str) and ev["agent"] != ""
          for ev in (sx_1, sx_2, sx_3)))
check("ScriptedSource seq contiguous from 0; session and default epoch stamped",
      [ev["seq"] for ev in (sx_1, sx_2, sx_3)] == [0, 1, 2]
      and all(ev["session"] == "s-x" and ev["epoch"] == 0 for ev in (sx_1, sx_2, sx_3)))
sy = src.session("s-y", epoch=2)
sy_1 = sy.emit("session.started", {"client": {"name": "testing-smoke"}})
check("ScriptedSource.session honors epoch (s-y at epoch 2, seq restarts at 0)",
      sy_1["session"] == "s-y" and sy_1["epoch"] == 2 and sy_1["seq"] == 0)
check("ScriptedSource.events accumulates across sessions in emit order",
      src.events == [sx_1, sx_2, sx_3, sy_1])
played: list[dict] = []
src.play(played.append)
check("ScriptedSource.play delivers the recorded events in order",
      played == [sx_1, sx_2, sx_3, sy_1])

# ---- ControlStub, target side: ControlTarget over stub.emitter -------------------
stub = ControlStub()
cmd = stub.command("control.pause", "s-x", data={})
cmd_other = stub.command("control.cancel", "s-x", data={"scope": "run"})
check("ControlStub.command mints a well-formed command frame "
      "(aep 0.1, fresh id, session, source/agent, time, NO seq)",
      cmd["aep"] == "0.1"
      and isinstance(cmd["id"], str) and cmd["id"] != ""
      and cmd["id"] != cmd_other["id"]
      and cmd["session"] == "s-x"
      and "seq" not in cmd
      and isinstance(cmd.get("source"), str) and cmd["source"].startswith("aep://")
      and isinstance(cmd.get("agent"), str) and cmd["agent"] != ""
      and "time" in cmd)

target = ControlTarget(stub.emitter, accepts=("control.pause",))
executed: list[dict] = []
first = target.handle(cmd, executed.append)
acks_cmd = [e for e in stub.acks if e.get("cause") == cmd["id"]]
check("ControlTarget over stub.emitter: accepted ack in stub.acks, cause = command id",
      first is True and executed == [cmd]
      and len(acks_cmd) == 1 and acks_cmd[0]["type"] == "control.accepted")

# retransmit of the SAME frame: one decision, the recorded ack re-emitted
# byte-identically (AEP-0004 §3), never a re-execute
executed_dup: list[dict] = []
dup = target.handle(cmd, executed_dup.append)
acks_cmd = [e for e in stub.acks if e.get("cause") == cmd["id"]]
check("target re-acks a retransmit byte-identically (canonical JSON equal, no re-execute)",
      dup is False and not executed_dup
      and len(acks_cmd) == 2 and canon(acks_cmd[0]) == canon(acks_cmd[1]))

# a command type outside accepts nacks `unsupported`
undeclared: list[dict] = []
refused = target.handle(cmd_other, undeclared.append)
nacks = [e for e in stub.acks if e.get("cause") == cmd_other["id"]]
check("command outside accepts is nacked unsupported",
      refused is False and not undeclared
      and len(nacks) == 1 and nacks[0]["type"] == "control.rejected"
      and (nacks[0].get("data") or {}).get("reason") == "unsupported")

# ---- ControlStub, client side: sync ControlSender over stub.send -----------------
client_stub = ControlStub()
sender = ControlSender(client_stub.send, agent="stub-phone", host="stub-host")


def accept_side() -> None:
    """Ack the first captured outbound frame via stub.accept -> sender.on_event."""
    while len(client_stub.sent) < 1:
        time.sleep(0.01)
    sender.on_event(client_stub.accept(client_stub.sent[0]))


t = threading.Thread(target=accept_side, daemon=True)
t.start()
ack = sender.send("control.pause", "s-x", data={}, ack_window_ms=2000, retries=1)
t.join(2)
check("client-side: sender.send over stub.send returns stub.accept's ack (cause = command id)",
      ack["type"] == "control.accepted"
      and ack["cause"] == client_stub.sent[0]["id"]
      and client_stub.sent[0]["type"] == "control.pause"
      and "seq" not in client_stub.sent[0])


def reject_side() -> None:
    while len(client_stub.sent) < 2:
        time.sleep(0.01)
    sender.on_event(client_stub.reject(client_stub.sent[1], reason="busy"))


t2 = threading.Thread(target=reject_side, daemon=True)
t2.start()
try:
    sender.send("control.resume", "s-x", ack_window_ms=2000, retries=0)
    check("client-side: stub.reject raises NackError with reason busy", False)
except NackError as e:
    check("client-side: stub.reject raises NackError with reason busy", e.reason == "busy")
t2.join(2)

# ---- projection: the golden corpus (tests/fixtures/projection/) ------------------
fixture_dir = os.path.join(os.path.dirname(__file__), "fixtures", "projection")
with open(os.path.join(fixture_dir, "events.jsonl"), encoding="utf-8") as f:
    corpus = [json.loads(line) for line in f if line.strip()]
with open(os.path.join(fixture_dir, "expected.json"), encoding="utf-8") as f:
    expected = json.load(f)["state"]

check("projection batch: corpus fold equals the golden expected state (canonical compare)",
      canon(project_state(corpus)) == canon(expected))

# batch sort repairs arrival order: the s-alpha subset (both sources; no
# colliders there, the redelivered ev-a-03 is byte-identical) reversed folds
# to the same state as file order
alpha = [ev for ev in corpus
         if ev.get("session") == "s-alpha"
         and ev.get("source") in ("aep://corpus/adapter-a", "aep://corpus/adapter-b")]
check("projection sort-repair: reversed s-alpha subset folds identically to file order",
      canon(project_state(list(reversed(alpha)))) == canon(project_state(alpha)))

# incremental apply folds in arrival order and reports the epoch regression
inc = StateProjection()
inc_base = {
    "aep": "0.1", "type": "log.emitted", "time": "2026-07-22T07:00:00.000Z",
    "source": "aep://corpus/adapter-x", "agent": "agent-x", "session": "s-x",
}
inc.apply({**inc_base, "id": "ev-x-00", "epoch": 1, "seq": 0})
inc.apply({**inc_base, "id": "ev-x-01", "epoch": 0, "seq": 0,
           "time": "2026-07-22T07:00:01.000Z"})
check("projection incremental: epoch 1 then epoch 0 arrival yields an epoch-regression violation",
      any(v.get("rule") == "epoch-regression" for v in inc.state()["violations"]))

# attention.answered does NOT clear pending (AEP-0002 §7.2 mirror): apply A's
# ev-a-00..ev-a-05 (through attention.answered) — ev-a-04 must still be pending
ans = StateProjection()
for ev in corpus:
    ans.apply(ev)
    if ev["id"] == "ev-a-05":
        break
alpha_a = next((s for s in ans.state()["sessions"]
                if s["source"] == "aep://corpus/adapter-a" and s["session"] == "s-alpha"), None)
check("projection: attention.answered does not clear pending (ev-a-04 still pending)",
      alpha_a is not None and any(p.get("id") == "ev-a-04" for p in alpha_a["pending"]))

# command frames and agent.* events are deduped but never folded into session
# state: no phantom (aep://corpus/mc, s-alpha) session, no presence session
cmd_frame = next(ev for ev in corpus if ev["id"] == "ev-cmd-01")
presence = next(ev for ev in corpus if ev["id"] == "ev-agent-00")
only_cmd = StateProjection()
only_cmd.apply(cmd_frame)
only_presence = StateProjection()
only_presence.apply(presence)
batch_keys = {(s["source"], s["session"]) for s in project_state(corpus)["sessions"]}
check("projection: command frames and agent.presence create no session "
      "(no phantom aep://corpus/mc)",
      only_cmd.state()["sessions"] == []
      and only_presence.state()["sessions"] == []
      and not any(source == "aep://corpus/mc" for source, _ in batch_keys))

print("PY SDK TESTING SMOKE OK" if failures == 0 else f"PY SDK TESTING SMOKE FAILED ({failures})")
sys.exit(0 if failures == 0 else 1)
