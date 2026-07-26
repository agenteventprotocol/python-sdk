"""aep-sdk asyncio smoke: the aio consumer against the same vendored relay
fixture the sync smoke uses — live order, resume without gap or duplicate,
capture down-level honored, clean close — plus the async ControlClient
against an in-process scripted target (accept with a forced same-id
retransmit, wire nack, synthesized window timeout, relay-on-behalf
unsupported nack), plus the tuning-knob kwargs on subscribe / open_duplex /
ControlClient.connect. Run via tests/run-smoke.sh."""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aep_sdk import Emitter, NackError, TransportError, http_sink, ulid  # noqa: E402
from aep_sdk import aio  # noqa: E402

RELAY = os.environ["AEP_RELAY"]
TOKEN = os.environ.get("AEP_TOKEN")  # the fixture relay is watch-only without it (AEP-0004 §4.1)
failures = 0


def check(name: str, ok: bool) -> None:
    global failures
    print(f"{'PASS' if ok else 'FAIL'}  aio: {name}")
    if not ok:
        failures += 1


async def collect(sub: aio.AsyncSubscription, n: int, timeout: float = 5.0) -> list[dict]:
    got: list[dict] = []

    async def drain() -> None:
        async for ev in sub:
            got.append(ev)
            if len(got) >= n:
                return

    try:
        await asyncio.wait_for(drain(), timeout)
    except asyncio.TimeoutError:
        pass
    return got


async def main() -> None:
    # ---- live: subscribe, then emit; events arrive in order ---------------------
    sid = f"s_aio_live_{ulid()[-6:].lower()}"
    sub = await aio.subscribe(RELAY, filter={"session": sid}, token=TOKEN)
    em = Emitter("py-agent", "smoke-host", http_sink(RELAY, TOKEN), epoch=1).session(sid)
    em.emit("session.started", {"client": {"name": "aep-sdk-smoke-aio"}})
    em.emit("tool.completed",
            {"tool": {"name": "Bash", "call_id": "t_1"}, "status": "success", "duration_ms": 7})
    em.emit("session.ended", {"reason": "normal"})
    got = await collect(sub, 3)
    check("live events arrive in emit order",
          [e["type"] for e in got] == ["session.started", "tool.completed", "session.ended"])
    check("live seq values contiguous from 0", [e["seq"] for e in got] == [0, 1, 2])
    check("positions track the last (session, epoch, seq)",
          any(p["session"] == sid and p["seq"] == 2 and p["epoch"] == 1
              for p in sub.positions()))

    # ---- clean close: async-for ends promptly after close() ---------------------
    t0 = time.monotonic()

    async def iterate_after_close() -> int:
        count = 0
        async for _ev in sub:
            count += 1
        return count

    task = asyncio.create_task(iterate_after_close())
    await asyncio.sleep(0.1)
    sub.close()
    extra = await asyncio.wait_for(task, 2.0)
    check("close() ends iteration promptly", (time.monotonic() - t0) < 2.0 and extra == 0)

    # ---- resume: replay strictly after a stored position, no gap, no dupe -------
    sid2 = f"s_aio_res_{ulid()[-6:].lower()}"
    em2 = Emitter("py-agent", "smoke-host", http_sink(RELAY, TOKEN), epoch=1).session(sid2)
    em2.emit("session.started", {"client": {"name": "aep-sdk-smoke-aio"}})
    for i in range(3):
        em2.emit("tool.completed",
                 {"tool": {"name": "Bash", "call_id": f"t_{i}"}, "status": "success",
                  "duration_ms": i})
    em2.emit("session.ended", {"reason": "normal"})  # seqs 0..4 buffered
    sub2 = await aio.subscribe(
        RELAY, filter={"session": sid2}, token=TOKEN,
        from_=[{"session": sid2, "epoch": 1, "seq": 1}], live=False)
    replayed = [ev async for ev in sub2]
    check("resume replays exactly the tail after the position (no gap, no dupe)",
          [e["seq"] for e in replayed] == [2, 3, 4])
    check("live=False iteration ends by itself at replay-complete", True)

    # ---- capture: down-level honored by the relay on this subscription ----------
    sid3 = f"s_aio_cap_{ulid()[-6:].lower()}"
    em3 = Emitter("py-agent", "smoke-host", http_sink(RELAY, TOKEN), epoch=1, capture="redacted").session(sid3)
    em3.emit("session.started", {"client": {"name": "aep-sdk-smoke-aio"}})
    sub3 = await aio.subscribe(RELAY, filter={"session": sid3}, capture="metadata", live=False, token=TOKEN)
    caught = [ev async for ev in sub3]
    check("capture param down-levels delivered events",
          len(caught) == 1 and caught[0]["capture"] == "metadata" and "data" not in caught[0])

    # ---- control parity: the async ControlClient vs an in-process scripted target --
    # The target rides the same stdlib duplex the client ships (the live-checker
    # stub pattern): it declares three command types, acks `respond` only after
    # a delay past the first ack window (forcing a same-id retransmit it must
    # dedupe, AEP-0004 §2.3), nacks `cancel` with `refused`, never acks `pause`
    # (scripted silence), and leaves an undeclared type to the relay's
    # on-behalf `unsupported` nack (§4.2).
    ctl_ses = f"s_aio_ctl_{ulid()[-6:].lower()}"
    tgt = await aio.open_duplex(RELAY, token=TOKEN)
    await tgt.send_json({
        "type": "hello", "versions": ["0.1"], "role": "emitter",
        "identity": {"agent": "py-target", "instance": "smoke-host"},
        "capabilities": {"control": {
            "accepts": ["control.attention.respond", "control.cancel", "control.pause"],
            "ack_window_ms": 10_000,
        }},
    })
    hello = await tgt.recv_json()
    check("scripted target completed the duplex hello", (hello or {}).get("type") == "hello")

    tseq = 0

    def tmk(type_: str, data: dict | None = None, **extra: object) -> dict:
        nonlocal tseq
        ev: dict = {
            "aep": "0.1", "id": ulid(), "type": type_,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "source": "aep://smoke-host/agent/py-target",
            "agent": "py-target", "session": ctl_ses,
            "seq": tseq, "epoch": 1, "capture": "metadata", **extra,
        }
        tseq += 1
        if data:
            ev["data"] = data
        return ev

    await tgt.send_json(tmk("session.started", {"client": {"name": "py-target"}}))

    delivered: list[dict] = []
    executed: set[str] = set()

    async def target_loop() -> None:
        while True:
            msg = await tgt.recv_json()
            if msg is None:
                return
            t = msg.get("type")
            if not (isinstance(t, str) and t.startswith("control.")):
                continue
            delivered.append(msg)
            if msg["id"] in executed:
                continue  # §2.3: a retransmit is the same command — no second ack
            executed.add(msg["id"])
            if t == "control.attention.respond":
                async def delayed_ack(mid: str = msg["id"]) -> None:
                    await asyncio.sleep(1.2)  # past window 1: the client must retry
                    await tgt.send_json(tmk("control.accepted", None, cause=mid))
                asyncio.get_running_loop().create_task(delayed_ack())
            elif t == "control.cancel":
                await tgt.send_json(tmk("control.rejected", {"reason": "refused"}, cause=msg["id"]))
            # control.pause: scripted silence — the sender's window must expire

    tloop = asyncio.get_running_loop().create_task(target_loop())

    ctl = aio.ControlClient(RELAY, agent="py-console", host="smoke-host", token=TOKEN)
    await ctl.connect()

    ack = await ctl.send("control.attention.respond", ctl_ses,
                         subject="01SMOKEREQUESTID0000000000",
                         data={"answer": {"option": "allow"}},
                         ack_window_ms=800, retries=2)
    check("async send returns the correlated ack",
          ack["type"] == "control.accepted" and isinstance(ack.get("cause"), str))
    resp = [m for m in delivered if m["type"] == "control.attention.respond"]
    check("slow ack forced a same-id retransmit and the target deduped it",
          len(resp) >= 2 and len({m["id"] for m in resp}) == 1
          and resp[0]["id"] == ack["cause"])
    check("command envelopes omit seq (AEP-0004 §2.2)",
          all("seq" not in m for m in resp))

    try:
        await ctl.send("control.cancel", ctl_ses, data={"scope": "run"})
        check("target nack raises NackError with the wire reason", False)
    except NackError as e:
        check("target nack raises NackError with the wire reason",
              e.reason == "refused" and not e.synthesized)

    t_timeout = time.monotonic()
    try:
        await ctl.send("control.pause", ctl_ses, ack_window_ms=250, retries=1)
        check("window silence raises the synthesized timeout nack", False)
    except NackError as e:
        check("window silence raises the synthesized timeout nack",
              e.reason == "timeout" and e.synthesized
              and (time.monotonic() - t_timeout) >= 0.45)

    try:
        await ctl.send("control.resume", ctl_ses, ack_window_ms=2000, retries=0)
        check("undeclared command type nacked unsupported on the target's behalf", False)
    except NackError as e:
        check("undeclared command type nacked unsupported on the target's behalf",
              e.reason == "unsupported" and not e.synthesized)

    # ---- roster (AEP-0003 §4.1): the live-claim snapshot ---------------------------
    # The scripted target above is still connected and claims ctl_ses with a
    # declared control block — the roster must list it verbatim.
    entries = await ctl.roster()
    mine = [e for e in entries if e.get("session") == ctl_ses]
    check("roster lists the live claim with its declared accepts verbatim",
          len(mine) == 1 and mine[0].get("agent") == "py-target"
          and "control.attention.respond" in (mine[0].get("control") or {}).get("accepts", [])
          and (mine[0].get("control") or {}).get("ack_window_ms") == 10_000)

    await ctl.close()
    tloop.cancel()
    await tgt.close()

    # ---- replay-all (AEP-0003 §5): a cold subscribe without enumeration ------------
    # ctl_ses's history is held by the fixture; from_="all" must replay it
    # with no position list and no session name enumerated into from_.
    all_got: list[dict] = []
    sub_all = await aio.subscribe(RELAY, filter={"session": ctl_ses},
                                  from_="all", live=False, token=TOKEN)
    async for ev in sub_all:
        all_got.append(ev)
    check("aio from_='all' replays held history without enumerating sessions",
          any(e.get("type") == "session.started" for e in all_got)
          and all(e.get("session") == ctl_ses for e in all_got))

    # ---- transport-error taxonomy -------------------------------------------------
    # http: the fixture's /socket upgrade refuses a wrong token with a plain
    # HTTP 401 (never reaching 101), same auth gate the control smoke above
    # already depends on.
    try:
        await aio.open_duplex(RELAY, token="wrong-token-entirely")
        check("wrong token on the duplex upgrade raises an http TransportError", False)
    except TransportError as e:
        check("wrong token on the duplex upgrade raises an http TransportError",
              e.kind == "http" and e.status == 401)

    # network: nothing is listening on the dead port. aio.subscribe()'s own
    # await returns only once connected, which never happens here, so drive
    # it as a background task and observe on_error instead of awaiting it —
    # its loop() wraps the raw connect failure (a bare ConnectionRefusedError,
    # since that happens before open_duplex's own raise sites) as a network
    # TransportError.
    dead_errors: list[Exception] = []

    async def _dead_subscribe() -> None:
        await aio.subscribe("http://127.0.0.1:9", on_error=dead_errors.append)

    dead_task = asyncio.create_task(_dead_subscribe())
    dead_deadline = time.monotonic() + 5
    while time.monotonic() < dead_deadline and not dead_errors:
        await asyncio.sleep(0.05)
    dead_task.cancel()
    try:
        await dead_task
    except (asyncio.CancelledError, Exception):  # noqa: BLE001 — best-effort teardown
        pass
    check("dead-port aio.subscribe() reports a network TransportError",
          len(dead_errors) > 0 and isinstance(dead_errors[0], TransportError)
          and dead_errors[0].kind == "network")

    # error-frame normalization: a stub duplex feeding ControlClient._read_loop
    # a relay error frame with a code outside the closed nack-reason set.
    class _StubDuplex:
        def __init__(self, frames: list[dict | None]) -> None:
            self._frames = list(frames)

        async def recv_json(self) -> dict | None:
            return self._frames.pop(0) if self._frames else None

    norm_ctl = aio.ControlClient(RELAY, agent="py-norm", host="smoke-host", token=TOKEN)
    norm_ctl._duplex = _StubDuplex([
        {"type": "error", "cause": "CMD1", "code": "limit-exceeded", "detail": "too big"},
        None,
    ])
    norm_fut: asyncio.Future = asyncio.get_running_loop().create_future()
    norm_ctl._pending["CMD1"] = norm_fut
    await norm_ctl._read_loop()
    try:
        norm_fut.result()
        check("unknown relay error code normalizes to reason invalid", False)
    except NackError as e:
        check("unknown relay error code normalizes to reason invalid",
              e.reason == "invalid" and e.detail is not None
              and "limit-exceeded" in e.detail and "too big" in e.detail)

    # ---- tuning knobs: kwargs exposure on the aio surface --------------------------
    # subscribe: all five knobs ride one real call that must still deliver —
    # acceptance for the backoff/budget values (held at their defaults here);
    # the behavioral backoff coverage lives in the sync smoke's closed-port
    # beat. sid2's history (seqs 0..4) is still held by the fixture.
    knob_sub = await aio.subscribe(
        RELAY, filter={"session": sid2}, token=TOKEN,
        from_=[{"session": sid2, "epoch": 1, "seq": 1}], live=False,
        backoff_initial_ms=500, backoff_max_ms=10_000,
        from_budget=6_000, max_replay_chunks=20, connect_timeout_ms=5_000)
    knob_replayed = [ev async for ev in knob_sub]
    check("subscribe carries the tuning knobs and still replays the tail",
          [e["seq"] for e in knob_replayed] == [2, 3, 4])
    knob_sub.close()

    # open_duplex connect_timeout_ms, behavioral: a raw listener that ACCEPTS
    # and never answers the upgrade (a plain daemon thread holds every
    # connection open) — the shape only the timeout knob can escape, and the
    # failure surfaces in the standing taxonomy (a network TransportError)
    hang_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    hang_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    hang_srv.bind(("127.0.0.1", 0))
    hang_srv.listen(8)
    hang_url = f"http://127.0.0.1:{hang_srv.getsockname()[1]}"
    hang_conns: list[socket.socket] = []

    def hang_accept_loop() -> None:
        while True:
            try:
                conn, _ = hang_srv.accept()
            except OSError:
                return  # listener closed: this beat is over
            hang_conns.append(conn)  # hold the socket open, never respond

    threading.Thread(target=hang_accept_loop, daemon=True).start()

    dpx_err: Exception | None = None
    t_dpx = time.monotonic()
    try:
        await aio.open_duplex(hang_url, connect_timeout_ms=300)
    except Exception as e:  # noqa: BLE001 — the exception type is the assertion
        dpx_err = e
    check("open_duplex connect_timeout_ms raises a network TransportError on "
          "a never-answered upgrade",
          isinstance(dpx_err, TransportError) and dpx_err.kind == "network"
          and (time.monotonic() - t_dpx) < 2.0)
    for c in hang_conns:
        try:
            c.close()
        except OSError:
            pass
    hang_srv.close()

    # ControlClient.connect connect_timeout_ms: plain acceptance over the real
    # relay (the knob bounds open_duplex plus the hello wait; the hang path is
    # proven right above on open_duplex, which connect() rides)
    knob_ctl = aio.ControlClient(RELAY, agent="py-knob-console",
                                 host="smoke-host", token=TOKEN)
    await knob_ctl.connect(connect_timeout_ms=5_000)
    await knob_ctl.close()
    check("ControlClient.connect accepts connect_timeout_ms over a healthy "
          "handshake", True)

    print("PY SDK AIO SMOKE OK" if failures == 0 else f"PY SDK AIO SMOKE FAILED ({failures})")
    sys.exit(0 if failures == 0 else 1)


asyncio.run(main())
