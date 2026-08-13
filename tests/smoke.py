"""agenteventprotocol smoke: pydantic validation of built envelopes, the control state
machine (ack / nack / dedupe-on-retry / window timeout), and — when AEP_RELAY
is set — live emit-over-HTTP + consume-over-SSE against the reference relay,
plus the sync WS control surface (open_duplex / Duplex / ControlClient)
against an in-process scripted duplex target and the transport tuning knobs
(backoff schedule, resume budget, connect/POST timeouts). Run via
tests/run-smoke.sh."""
from __future__ import annotations

import http.server
import json
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aep_sdk import (  # noqa: E402
    AepEvent, AttentionRequestedData, ControlClient, ControlSender,
    ControlTarget, Duplex, Emitter, NackError, ToolCompletedData,
    TransportError, http_sink, open_duplex, subscribe, ulid,
)

failures = 0


def check(name: str, ok: bool) -> None:
    global failures
    print(f"{'PASS' if ok else 'FAIL'}  {name}")
    if not ok:
        failures += 1


# ---- envelope building validates against the generated pydantic types ----------
built: list[dict] = []
em = Emitter("py-agent", "smoke-host", built.append, epoch=1)
s = em.session("s_py_1")
s.emit("session.started", {"client": {"name": "agenteventprotocol-smoke"}})
s.emit("tool.completed",
       {"tool": {"name": "Bash", "call_id": "t_1"}, "status": "success", "duration_ms": 12},
       subject="Bash", run="r_1")
s.emit("attention.requested",
       {"kind": "permission", "prompt": "ok?",
        "options": [{"id": "allow", "label": "Allow"}], "respond_via": ["control"]},
       severity="notice")
em.emit("agent.presence", {"state": "online"})

for ev in built:
    AepEvent.model_validate(ev)
check("pydantic envelope validation on built events", True)
ToolCompletedData.model_validate(built[1]["data"])
AttentionRequestedData.model_validate(built[2]["data"])
check("pydantic payload validation on built events", True)
check("seq monotonic per session, absent on agent scope",
      [e.get("seq") for e in built] == [0, 1, 2, None])

# extensions are validated at the producing boundary (AEP-0001 §5.4): a key
# that collides with a core attribute is refused, not merged — a clobbered
# core attribute is invisible once on the wire, so the emitter is the only
# place it can be caught.


def _refuses(extensions: dict) -> bool:
    try:
        em.emit("progress.status", {}, extensions=extensions)
        return False
    except ValueError:
        return True


check("extensions: a reserved-name collision is refused, not merged (§5.4)",
      _refuses({"id": "spoofed"}) and _refuses({"subject": "clobber"}))
check("extensions: colliding on session/epoch/seq is refused (§5.4)",
      _refuses({"session": "x"}) and _refuses({"epoch": 9}) and _refuses({"seq": 0}))
check("extensions: an ill-formed name is refused (§5.4 [a-z0-9]{1,20})",
      _refuses({"my_ext": "v"}) and _refuses({"Ext": "v"}) and _refuses({"toolongextensionname123": "v"}))
check("extensions: a non-scalar value (or a float) is refused (§5.4)",
      _refuses({"meta": {"k": 1}}) and _refuses({"ratio": 1.5}) and _refuses({"items": [1]}))
legal = em.emit("progress.status", {}, extensions={"acme1": "v", "acmecount": 3, "acmeok": True})
check("extensions: legal scalar extensions still emit and round-trip",
      legal["acme1"] == "v" and legal["acmecount"] == 3 and legal["acmeok"] is True)

# ---- control state machine over a fake duplex ------------------------------------
wire: list[dict] = []
target_events: list[dict] = []
target_session = Emitter("py-target", "smoke-host", target_events.append, epoch=1).session("s_py_1")
target = ControlTarget(target_session, accepts=["control.attention.respond"])
sender = ControlSender(wire.append, agent="py-phone", host="smoke-host")


def relay_side() -> None:
    """Deliver wire commands to the target; loop target acks back to the sender."""
    while len(wire) < 1:
        time.sleep(0.01)
    executed: list[dict] = []
    target.handle(wire[0], executed.append)
    for ev in target_events:
        sender.on_event(ev)


t = threading.Thread(target=relay_side, daemon=True)
t.start()
ack = sender.send("control.attention.respond", "s_py_1",
                  subject=ulid(), data={"answer": {"option": "allow"}},
                  ack_window_ms=2000, retries=1)
t.join(2)
check("control ack correlated (cause = command id)",
      ack["type"] == "control.accepted" and ack["cause"] == wire[0]["id"])
check("command omits seq (foreign-emitter rule)", "seq" not in wire[0])

# duplicate delivery: MUST NOT re-execute; the recorded ack is re-emitted
# byte-identically — the §2.3 retry recovers a lost ack (AEP-0004 §3)
executed2: list[dict] = []
before = len(target_events)
dup_ok = target.handle(wire[0], executed2.append) is False and not executed2
acks_dup = [e for e in target_events
            if e.get("cause") == wire[0]["id"]
            and e["type"] in ("control.accepted", "control.rejected")]
check("target re-acks a retransmit byte-identically (one decision, no re-execute)",
      dup_ok and len(target_events) == before + 1
      and len(acks_dup) == 2 and acks_dup[0] == acks_dup[1]
      and len({e["id"] for e in acks_dup}) == 1)

# same command id, different source: dedupe scope is (source, id) — AEP-0001
# §7.4 — so a different sender minting the same id is NOT a duplicate
same_id_other_source = dict(wire[0], source="aep://smoke-host/agent/py-phone-2")
executed3: list[dict] = []
before_other_source = len(target_events)
check("target executes the same id from a different source (not a duplicate)",
      target.handle(same_id_other_source, executed3.append) is True
      and executed3 == [same_id_other_source]
      and len(target_events) == before_other_source + 1)  # one more control.accepted

# undeclared command type nacks `unsupported`
nacked = ControlSender(wire.append, agent="py-phone", host="smoke-host")


def relay_side_2() -> None:
    while len(wire) < 2:
        time.sleep(0.01)
    target.handle(wire[1], lambda _cmd: None)
    for ev in target_events[before:]:
        nacked.on_event(ev)


t2 = threading.Thread(target=relay_side_2, daemon=True)
t2.start()
try:
    nacked.send("control.cancel", "s_py_1", data={"scope": "run"},
                ack_window_ms=2000, retries=0)
    check("undeclared command nacked unsupported", False)
except NackError as e:
    check("undeclared command nacked unsupported", e.reason == "unsupported")
t2.join(2)

# window silence: synthesized timeout after retries, same id each attempt
silent_wire: list[dict] = []
lonely = ControlSender(silent_wire.append, agent="py-phone", host="smoke-host")
t0 = time.monotonic()
try:
    lonely.send("control.pause", "s_py_1", ack_window_ms=150, retries=2)
    check("window silence synthesizes timeout", False)
except NackError as e:
    elapsed = time.monotonic() - t0
    check("window silence synthesizes timeout",
          e.reason == "timeout" and e.synthesized and 0.4 <= elapsed < 2.0)
check("retries reuse the same command id",
      len(silent_wire) == 3 and len({c["id"] for c in silent_wire}) == 1)

# ---- transport-error taxonomy: network + parse -----------------------------------
network_errors: list[Exception] = []
dead_sub = subscribe("http://127.0.0.1:9", lambda ev: None,
                      on_error=network_errors.append)
deadline = time.monotonic() + 5
while time.monotonic() < deadline and not network_errors:
    time.sleep(0.05)
check("subscribe against a dead port reports a network TransportError",
      len(network_errors) > 0 and isinstance(network_errors[0], TransportError)
      and network_errors[0].kind == "network")
dead_sub.close()


class _OneShotSSEHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:  # noqa: D401 — keep the smoke output quiet
        pass

    def do_GET(self) -> None:  # noqa: N802 — http.server's naming convention
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        good = {
            "aep": "0.1", "id": ulid(), "type": "session.started",
            "time": "2026-01-01T00:00:00.000Z",
            "source": "aep://smoke-host/agent/py-parse-beat", "agent": "py-parse-beat",
            "session": "s_py_parse", "seq": 0, "capture": "metadata",
            "data": {"client": {"name": "parse-beat"}},
        }
        self.wfile.write(b"data: {not json\n\n")
        self.wfile.write(f"data: {json.dumps(good)}\n\n".encode())


parse_httpd = http.server.HTTPServer(("127.0.0.1", 0), _OneShotSSEHandler)
parse_port = parse_httpd.server_port
parse_thread = threading.Thread(target=parse_httpd.serve_forever, daemon=True)
parse_thread.start()

parse_errors: list[Exception] = []
parse_events: list[dict] = []
parse_sub = subscribe(f"http://127.0.0.1:{parse_port}", parse_events.append,
                      on_error=parse_errors.append)
deadline = time.monotonic() + 5
while time.monotonic() < deadline and not (parse_errors and parse_events):
    time.sleep(0.05)
check("malformed SSE data line reports a parse TransportError and the "
      "next valid event still arrives",
      len(parse_errors) > 0 and parse_errors[0].kind == "parse"
      and len(parse_events) > 0 and parse_events[0]["type"] == "session.started")
parse_sub.close()
parse_httpd.shutdown()
parse_httpd.server_close()

# ---- live path (optional): emit over HTTP, consume over SSE ----------------------
relay = os.environ.get("AEP_RELAY")
token = os.environ.get("AEP_TOKEN")  # the fixture relay is watch-only without it (AEP-0004 §4.1)
if relay:
    got: list[dict] = []
    session_id = f"s_py_live_{ulid()[-6:].lower()}"
    sub = subscribe(relay, got.append, filter={"session": session_id}, token=token)
    time.sleep(0.5)
    live = Emitter("py-agent", "smoke-host", http_sink(relay, token), epoch=1).session(session_id)
    live.emit("session.started", {"client": {"name": "agenteventprotocol-smoke"}})
    live.emit("session.ended", {"reason": "normal"})
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(got) < 2:
        time.sleep(0.05)
    check("live: SSE consumer received both events over HTTP ingest",
          [e["type"] for e in got] == ["session.started", "session.ended"])
    check("live: resume positions tracked",
          any(p["session"] == session_id and p["seq"] == 1 for p in sub.positions()))
    sub.close()

    # ---- consumer dedupe scope is (source, id), not bare id (AEP-0001 §7.4) -----
    # Two different emitters may legally mint the same event id: both must be
    # delivered. A repeat of the exact same (source, id) must be dropped.
    dedupe_session_a = f"s_py_dedupe_a_{ulid()[-6:].lower()}"
    dedupe_session_b = f"s_py_dedupe_b_{ulid()[-6:].lower()}"
    dedupe_got: list[dict] = []
    dedupe_sub = subscribe(relay, dedupe_got.append,
                            filter={"session": [dedupe_session_a, dedupe_session_b]}, token=token)
    time.sleep(0.3)
    sink = http_sink(relay, token)
    shared_id = ulid()
    ev_from_a = {
        "aep": "0.1", "id": shared_id, "type": "session.started",
        "time": "2026-01-01T00:00:00.000Z",
        "source": "aep://smoke-host/agent/py-dedupe-emitter-a", "agent": "py-dedupe-emitter-a",
        "session": dedupe_session_a, "seq": 0, "capture": "metadata",
        "data": {"client": {"name": "dedupe-a"}},
    }
    ev_from_b = {
        "aep": "0.1", "id": shared_id, "type": "session.started",
        "time": "2026-01-01T00:00:00.000Z",
        "source": "aep://smoke-host/agent/py-dedupe-emitter-b", "agent": "py-dedupe-emitter-b",
        "session": dedupe_session_b, "seq": 0, "capture": "metadata",
        "data": {"client": {"name": "dedupe-b"}},
    }
    sink(ev_from_a)
    sink(ev_from_b)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(dedupe_got) < 2:
        time.sleep(0.05)
    check("live: same id, different source, both delivered (AEP-0001 §7.4)",
          any(e["id"] == shared_id and e["source"] == ev_from_a["source"] for e in dedupe_got)
          and any(e["id"] == shared_id and e["source"] == ev_from_b["source"] for e in dedupe_got))

    # exact repeat of (source, id) ev_from_a, addressed to the other session so
    # the relay's own per-session id-set (a separate, session-scoped mechanism)
    # doesn't itself swallow it before it reaches the SDK's subscriber dedupe
    repeat_of_a = dict(ev_from_a, session=dedupe_session_b, seq=1)
    sink(repeat_of_a)
    time.sleep(0.5)
    check("live: repeat of the same (source, id) is dropped",
          len([e for e in dedupe_got if e["id"] == shared_id and e["source"] == ev_from_a["source"]]) == 1)
    dedupe_sub.close()

    # ---- bounded resume: a large known-session set must never kill the pipe ----
    # Hundreds of persisted sessions once rode a single `from` query param; past
    # ~16 KB the server's HTTP layer refuses the request before any relay code
    # runs, permanently. The SDK must bound what rides the live request and
    # drain the rest through bounded replay requests.
    big_session = f"s_py_big_{ulid()[-8:].lower()}"
    big_got: list[dict] = []
    for seq in (0, 1, 2):
        sink({
            "aep": "0.1", "id": ulid(), "type": "log.emitted",
            "time": "2026-01-01T00:00:00.000Z",
            "source": f"aep://smoke-host/agent/py-big/session/{big_session}",
            "agent": "py-big", "session": big_session, "seq": seq, "epoch": 1,
            "capture": "metadata",
        })
    time.sleep(0.4)
    phantoms = [{"session": f"s_py_phantom_{i:04d}_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                 "epoch": 0, "seq": -1} for i in range(240)]
    big_sub = subscribe(relay, big_got.append, token=token,
                        filter={"session": big_session},
                        from_=[*phantoms, {"session": big_session, "epoch": 0, "seq": -1}])
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(big_got) < 3:
        time.sleep(0.05)
    check("live: buffered events replay despite a 241-position known-session set",
          all(any(e.get("seq") == seq for e in big_got) for seq in (0, 1, 2)))

    # ---- replay-all (AEP-0003 §5): the enumeration-free cold start -----------------
    # The same held history, no position list at all: from_="all" replays every
    # session the fixture holds from its earliest position.
    ra_got: list[dict] = []
    ra_sub = subscribe(relay, ra_got.append, token=token,
                       filter={"session": big_session}, from_="all")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(ra_got) < 3:
        time.sleep(0.05)
    check("sync: from_='all' replays held history without enumerating sessions",
          all(any(e.get("seq") == seq for e in ra_got) for seq in (0, 1, 2)))
    ra_sub.close()
    sink({
        "aep": "0.1", "id": ulid(), "type": "log.emitted",
        "time": "2026-01-01T00:00:00.000Z",
        "source": f"aep://smoke-host/agent/py-big/session/{big_session}",
        "agent": "py-big", "session": big_session, "seq": 3, "epoch": 1,
        "capture": "metadata",
    })
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not any(e.get("seq") == 3 for e in big_got):
        time.sleep(0.05)
    check("live: the stream is LIVE after the oversized cold start",
          any(e.get("seq") == 3 for e in big_got))
    big_sub.close()

    # ---- sync control: the bundled stdlib WS transport (AEP-0003 §4 duplex,
    # AEP-0004 commands) — the blocking twin of smoke_aio's control section.
    # The scripted target rides the same sync Duplex the client ships: it
    # claims a session via its hello control block, emits sequenced session
    # events, acks `pause`, nacks `cancel` with a scripted reason/detail, and
    # answers from a plain daemon thread (threading, not asyncio: the sync
    # surface is what is under test here).
    ctl_ses = f"s_py_ctl_{ulid()[-6:].lower()}"
    tgt = open_duplex(relay, token=token)
    check("sync control: open_duplex returns a Duplex", isinstance(tgt, Duplex))
    tgt.send_json({
        "type": "hello", "versions": ["0.1"], "role": "emitter",
        "identity": {"agent": "py-sync-target", "instance": "smoke-host"},
        "capabilities": {"control": {
            "accepts": ["control.pause", "control.cancel"],
            "ack_window_ms": 9_000,
        }},
    })
    hello = tgt.recv_json()
    check("sync control: scripted target completed the duplex hello",
          (hello or {}).get("type") == "hello")

    tgt_seq = [0]  # box, not nonlocal: this section runs at module level

    def tgt_mk(type_: str, data: dict | None = None, **extra: object) -> dict:
        # target-side envelope mint (sequenced session events, epoch 1) —
        # after session.started below, only the reader thread calls this
        ev: dict = {
            "aep": "0.1", "id": ulid(), "type": type_,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "source": "aep://smoke-host/agent/py-sync-target",
            "agent": "py-sync-target", "session": ctl_ses,
            "seq": tgt_seq[0], "epoch": 1, "capture": "metadata", **extra,
        }
        tgt_seq[0] += 1
        if data:
            ev["data"] = data
        return ev

    tgt.send_json(tgt_mk("session.started", {"client": {"name": "py-sync-target"}}))

    delivered: list[dict] = []
    handled: set[str] = set()

    def target_loop() -> None:
        # blocking read loop: recv_json() returns None once the duplex closes
        while True:
            msg = tgt.recv_json()
            if msg is None:
                return
            mtype = msg.get("type")
            if not (isinstance(mtype, str) and mtype.startswith("control.")):
                continue
            delivered.append(msg)
            if msg["id"] in handled:
                continue  # §2.3: a retransmit is the same command — one decision
            handled.add(msg["id"])
            if mtype == "control.pause":
                tgt.send_json(tgt_mk("control.accepted", None, cause=msg["id"]))
            elif mtype == "control.cancel":
                tgt.send_json(tgt_mk(
                    "control.rejected",
                    {"reason": "refused", "detail": "scripted sync refusal"},
                    cause=msg["id"]))

    tgt_thread = threading.Thread(target=target_loop, daemon=True)
    tgt_thread.start()

    ctl = ControlClient(relay, agent="py-sync-console", host="smoke-host", token=token)
    ctl.connect()
    check("sync control: connect() completes the hello handshake", True)

    ack = ctl.send("control.pause", ctl_ses, ack_window_ms=5000, retries=1)
    pauses = [m for m in delivered if m["type"] == "control.pause"]
    check("sync control: send returns the correlated control.accepted",
          ack["type"] == "control.accepted"
          and len(pauses) >= 1 and ack["cause"] == pauses[0]["id"])
    check("sync control: command envelopes omit seq (AEP-0004 §2.2)",
          all("seq" not in m for m in delivered))

    try:
        ctl.send("control.cancel", ctl_ses, data={"scope": "run"},
                 ack_window_ms=5000, retries=0)
        check("sync control: target nack raises NackError with the scripted "
              "reason/detail", False)
    except NackError as e:
        check("sync control: target nack raises NackError with the scripted "
              "reason/detail",
              e.reason == "refused" and e.detail == "scripted sync refusal"
              and not e.synthesized)

    # roster (AEP-0003 §4.1): the scripted target is still connected and
    # claims ctl_ses with a declared control block — listed verbatim
    entries = ctl.roster(timeout_ms=10_000)
    mine = [e for e in entries if e.get("session") == ctl_ses]
    check("sync control: roster lists the live claim with its declared "
          "accepts verbatim",
          len(mine) == 1 and mine[0].get("agent") == "py-sync-target"
          and (mine[0].get("control") or {}).get("accepts")
          == ["control.pause", "control.cancel"]
          and (mine[0].get("control") or {}).get("ack_window_ms") == 9_000)

    # close() twice on both surfaces: idempotent, no raise (the duplex sends
    # a best-effort close frame the first time and no-ops after)
    try:
        ctl.close()
        ctl.close()
        tgt.close()
        tgt.close()
        closed_ok = True
    except Exception:  # noqa: BLE001 — idempotence is the contract under test
        closed_ok = False
    tgt_thread.join(2)
    check("sync control: close() is idempotent on client and duplex",
          closed_ok and not tgt_thread.is_alive())

    # ---- tuning knobs: backoff / resume budget / timeouts exposed as kwargs ------
    # A raw listener that ACCEPTS and never answers: connects succeed, requests
    # go unanswered — the failure shape only a timeout knob can escape (a dead
    # port refuses instantly; this one just hangs).
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
                return  # listener closed: these beats are over
            hang_conns.append(conn)  # hold the socket open, never respond

    threading.Thread(target=hang_accept_loop, daemon=True).start()

    # http_sink timeout_ms: the POST would block forever against the hanging
    # listener; the knob bounds it, the sink call itself returns, and the
    # failure flows to on_error (emitters must never crash the host)
    sink_errors: list[Exception] = []
    t_sink = time.monotonic()
    timed_sink = http_sink(hang_url, token=None, on_error=sink_errors.append,
                           timeout_ms=300)
    timed_sink(built[0])
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not sink_errors:
        time.sleep(0.05)
    check("knobs: http_sink timeout_ms bounds a never-answered POST into on_error",
          len(sink_errors) > 0 and (time.monotonic() - t_sink) < 2.0)

    # subscribe backoff knobs, behavioral: a CLOSED port refuses instantly, so
    # the on_error cadence is set by the backoff schedule alone — at 50/100 ms
    # the third refusal lands by ~0.15 s, where the stock 500 ms start would
    # manage only ~2 attempts inside this window. The budget/replay/timeout
    # knobs ride the same call as acceptance.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    dead_knob_port = probe.getsockname()[1]
    probe.close()  # nothing listens here any more
    backoff_errors: list[Exception] = []
    backoff_sub = subscribe(f"http://127.0.0.1:{dead_knob_port}", lambda ev: None,
                            on_error=backoff_errors.append,
                            backoff_initial_ms=50, backoff_max_ms=100,
                            from_budget=6_000, max_replay_chunks=20,
                            timeout_ms=5_000)
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline and len(backoff_errors) < 3:
        time.sleep(0.05)
    check("knobs: backoff_initial_ms/backoff_max_ms speed up reconnects "
          "(3+ refusals inside 1.5 s)",
          len(backoff_errors) >= 3)
    backoff_sub.close()

    # open_duplex connect_timeout_ms, behavioral: the upgrade request is sent
    # but the 101 never comes — the knob bounds the whole handshake and the
    # failure surfaces in the standing taxonomy (a network TransportError),
    # not as a raw socket timeout
    dpx_err: Exception | None = None
    t_dpx = time.monotonic()
    try:
        open_duplex(hang_url, connect_timeout_ms=300)
    except Exception as e:  # noqa: BLE001 — the exception type is the assertion
        dpx_err = e
    check("knobs: open_duplex connect_timeout_ms raises a network "
          "TransportError on a never-answered upgrade",
          isinstance(dpx_err, TransportError) and dpx_err.kind == "network"
          and (time.monotonic() - t_dpx) < 2.0)

    for c in hang_conns:
        try:
            c.close()
        except OSError:
            pass
    hang_srv.close()

    # ControlClient.connect connect_timeout_ms against the real relay: plain
    # acceptance — the knob exists and a healthy handshake completes under it
    # (the hang-path behavior is proven on open_duplex above, which connect()
    # rides)
    knob_ctl = ControlClient(relay, agent="py-knob-console", host="smoke-host",
                             token=token)
    knob_ctl.connect(connect_timeout_ms=5_000)
    knob_ctl.close()
    check("knobs: ControlClient.connect accepts connect_timeout_ms over a "
          "healthy handshake", True)
else:
    print("SKIP  live relay checks (AEP_RELAY unset)")


# ---- resume self-heal: a refused resume set shrinks instead of looping dead ----
class _RefusingFromHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:  # noqa: D401 — keep the smoke output quiet
        pass

    def do_GET(self) -> None:  # noqa: N802 — http.server's naming convention
        if "from=" in self.path:
            body = b"from too large"
            self.send_response(400)
            self.send_header("access-control-allow-origin", "*")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        ev = {
            "aep": "0.1", "id": ulid(), "type": "session.started",
            "time": "2026-01-01T00:00:00.000Z",
            "source": "aep://smoke-host/agent/py-heal", "agent": "py-heal",
            "session": "s_py_heal", "seq": 0, "capture": "metadata",
        }
        self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())


heal_httpd = http.server.HTTPServer(("127.0.0.1", 0), _RefusingFromHandler)
heal_thread = threading.Thread(target=heal_httpd.serve_forever, daemon=True)
heal_thread.start()
heal_errors: list[Exception] = []
heal_events: list[dict] = []
heal_sub = subscribe(f"http://127.0.0.1:{heal_httpd.server_port}", heal_events.append,
                     from_=[{"session": "s_py_heal_x", "epoch": 0, "seq": -1},
                            {"session": "s_py_heal_y", "epoch": 0, "seq": -1}],
                     on_error=heal_errors.append)
deadline = time.monotonic() + 6
while time.monotonic() < deadline and not heal_events:
    time.sleep(0.05)
check("resume self-heal: the refusal surfaced as a readable HTTP 400",
      any(isinstance(e, TransportError) and e.kind == "http" and e.status == 400
          for e in heal_errors))
check("resume self-heal: the stream recovered by shrinking the resume set",
      any(e.get("type") == "session.started" for e in heal_events))
heal_sub.close()
heal_httpd.shutdown()
heal_httpd.server_close()

print("PY SDK SMOKE OK" if failures == 0 else f"PY SDK SMOKE FAILED ({failures})")
sys.exit(0 if failures == 0 else 1)
