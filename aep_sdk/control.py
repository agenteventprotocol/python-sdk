"""Control sending (AEP-0004, ``experimental``), in two shapes. Commands flow
only on authenticated duplex bindings (WebSocket in 0.1, AEP-0004 §4.1); a
bundled stdlib WS transport ships — :func:`open_duplex` / :class:`Duplex`
(a blocking JSON-message WebSocket, the synchronous twin of the ``aio`` pair)
and :class:`ControlClient` (blocking ``send()``/``roster()`` over an owned
duplex). :class:`ControlSender` stays the transport-agnostic builder for
caller-owned wires: inject ``send`` (outbound frames) and feed inbound Events
to ``on_event``."""
from __future__ import annotations

import json
import socket
import ssl as _ssl
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from io import BufferedReader
from typing import Any, Callable

from ._ws import (encode_control_frame, encode_text_frame, expected_accept,
                  handshake_key, upgrade_request)
from .emit import ulid
from .errors import TransportError

NACK_REASONS = ("unsupported", "unauthorized", "busy", "refused", "invalid", "timeout")

# The relay's `error` frame `code` isn't drawn from NACK_REASONS (it covers
# the wider wire protocol, e.g. `limit-exceeded`, `bad-json`); `timeout` is
# excluded too — that reason is only ever synthesized locally (never on the
# wire), matching the TypeScript SDK's normalization set.
_KNOWN_ERROR_CODES = ("unsupported", "unauthorized", "busy", "refused", "invalid")


class NackError(Exception):
    def __init__(self, reason: str, detail: str | None = None, *, synthesized: bool = False) -> None:
        super().__init__(f"command nacked: {reason}" + (f" ({detail})" if detail else ""))
        self.reason = reason
        self.detail = detail
        #: True for the locally synthesized ``timeout`` nack (never on the wire).
        self.synthesized = synthesized


class _Pending:
    def __init__(self, command: dict[str, Any]) -> None:
        self.command = command
        self.settled = threading.Event()
        self.ack: dict[str, Any] | None = None
        self.error: NackError | None = None


class ControlSender:
    """Correlates commands to acks; blocking ``send()`` with retries on the same id."""

    def __init__(self, transport_send: Callable[[dict[str, Any]], None],
                 *, agent: str, host: str) -> None:
        self._send = transport_send
        self.agent = agent
        self.host = host
        self._pending: dict[str, _Pending] = {}
        self._lock = threading.Lock()

    def on_event(self, ev: dict[str, Any]) -> None:
        """Feed every inbound Event here; acks are matched by cause = command id."""
        if ev.get("type") not in ("control.accepted", "control.rejected"):
            return
        cmd_id = ev.get("cause")
        with self._lock:
            p = self._pending.pop(cmd_id, None) if isinstance(cmd_id, str) else None
        if p is None:
            return  # not ours, or already settled
        if ev["type"] == "control.accepted":
            p.ack = ev
        else:
            data = ev.get("data") or {}
            p.error = NackError(data.get("reason", "invalid"), data.get("detail"))
        p.settled.set()

    def build(self, type: str, session: str, *,
              run: str | None = None, subject: str | None = None,
              cause: str | None = None, data: dict[str, Any] | None = None,
              capture: str = "metadata") -> dict[str, Any]:
        """Command envelope: addresses the target session, MUST omit seq (§2.2)."""
        cmd: dict[str, Any] = {
            "aep": "0.1",
            "id": ulid(),  # idempotency key: retries reuse it (§2.3)
            "type": type,
            "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "source": f"aep://{self.host}/agent/{self.agent}",
            "agent": self.agent,
            "session": session,
            "capture": capture,
        }
        if run is not None:
            cmd["run"] = run
        if subject is not None:
            cmd["subject"] = subject
        if cause is not None:
            cmd["cause"] = cause
        if data is not None:
            cmd["data"] = data
        return cmd

    def send(self, type: str, session: str, *,
             ack_window_ms: int = 10_000, retries: int = 2,
             **kwargs: Any) -> dict[str, Any]:
        """Send a command; returns the ``control.accepted`` Event or raises
        NackError — on ``control.rejected``, or with reason ``timeout``
        (synthesized locally, AEP-0004 §3) after the window closes on the last
        retry. Every retry resends the SAME envelope (same ``source`` and
        ``id``): targets dedupe on (source, id) — AEP-0001 §7.4 — and never
        re-execute."""
        cmd = self.build(type, session, **kwargs)
        p = _Pending(cmd)
        with self._lock:
            self._pending[cmd["id"]] = p
        attempts = 1 + max(retries, 0)
        try:
            for _ in range(attempts):
                self._send(cmd)
                if p.settled.wait(ack_window_ms / 1000):
                    break
            else:
                raise NackError("timeout", f"no ack within {ack_window_ms} ms x {attempts}",
                                synthesized=True)
        finally:
            with self._lock:
                self._pending.pop(cmd["id"], None)
        if p.error is not None:
            raise p.error
        assert p.ack is not None
        return p.ack


class ControlTarget:
    """Target-side conformance helper (AEP-0004 §7): dedupe on (source, id) —
    AEP-0001 §7.4, one ack DECISION per deduplicated command with the
    recorded ack re-emitted byte-identically on a retransmit (a lost ack is
    recovered by the retry, §2.3/§3), ``unsupported`` nack for undeclared
    types."""

    def __init__(self, session_emitter: Any, accepts: list[str]) -> None:
        self._emit = session_emitter
        self.accepts = list(accepts)
        # (source, id) -> the ack Event AS EMITTED; retention rides this
        # dedupe memory (§3): a forgotten command reads as new.
        self._seen: dict[tuple[Any, str], dict[str, Any] | None] = {}

    def handle(self, cmd: dict[str, Any],
               execute: Callable[[dict[str, Any]], None]) -> bool:
        """Ack/nack ``cmd``; call ``execute`` at most once per (source, id).
        Returns True when the command was accepted (first delivery). A
        redelivered command gets its recorded ack re-emitted byte-identically
        and returns False — same decision, never a second Event."""
        cmd_id = cmd.get("id")
        # Dedupe scope is (source, id) — AEP-0001 §7.4: two senders may
        # legally mint the same id, and a retried command rides the same
        # source (the sender's identity), so retries still dedupe correctly.
        if not isinstance(cmd_id, str):
            return False
        dedupe_key = (cmd.get("source"), cmd_id)
        if dedupe_key in self._seen:
            # duplicate: MUST NOT re-execute; the recorded ack is re-emitted
            # byte-identically — the §2.3 retry recovers a lost ack (§3)
            recorded = self._seen[dedupe_key]
            redeliver = getattr(self._emit, "redeliver", None)
            if recorded is not None and callable(redeliver):
                redeliver(recorded)
            return False
        self._seen[dedupe_key] = None
        if len(self._seen) > 10_000:
            self._seen.pop(next(iter(self._seen)))
        if cmd.get("type") not in self.accepts:
            self._seen[dedupe_key] = self._emit.emit(
                "control.rejected", {"reason": "unsupported"}, cause=cmd_id)
            return False
        self._seen[dedupe_key] = self._emit.emit("control.accepted", {}, cause=cmd_id)
        execute(cmd)
        return True


# --------------------------------------------------------------------------
# Bundled WebSocket duplex, blocking flavor (control requires an
# authenticated duplex binding, AEP-0004 §4.1; WebSocket in 0.1). Client
# side of RFC 6455 over a plain ``socket``: masked text frames out, unmasked
# frames in. The pure byte layout (handshake key/accept, upgrade request,
# frame encoding) lives in ``aep_sdk._ws``, shared with the asyncio twin in
# ``aep_sdk.aio``; frame *decode* stays here, interleaved with this flavor's
# blocking reads.


class Duplex:
    """A JSON-message WebSocket connection, blocking — the synchronous twin
    of :class:`aep_sdk.aio.Duplex` (see :func:`open_duplex`).

    ``recv_json()`` answers WebSocket pings and the relay's application-level
    ``{"type": "ping"}`` frames itself and returns ``None`` at close. A frame
    whose payload isn't decodable JSON is skipped silently (the connection
    stays open; there's no caller-supplied ``on_error`` at this layer to
    report it to)."""

    def __init__(self, sock: socket.socket, reader: BufferedReader | None = None) -> None:
        self._sock = sock
        # BufferedReader.read(n) returns exactly n bytes until EOF — the
        # blocking equivalent of StreamReader.readexactly. open_duplex hands
        # over the reader it did the upgrade handshake on (the buffer may
        # already hold post-handshake frames).
        self._reader = reader if reader is not None else sock.makefile("rb")
        self._closed = False
        # The aio twin needs no send lock — one event loop serializes every
        # write by construction. Here the caller's send_json and the reader
        # thread's ping replies (recv_json answers pings itself) reach the
        # same socket from different threads, so writes are serialized
        # explicitly: an interleaved sendall would corrupt frame framing.
        self._send_lock = threading.Lock()

    def send_json(self, obj: dict[str, Any]) -> None:
        frame = encode_text_frame(json.dumps(obj).encode("utf-8"))
        with self._send_lock:
            self._sock.sendall(frame)

    def _send_control_frame(self, opcode: int, payload: bytes = b"") -> None:
        frame = encode_control_frame(opcode, payload)
        with self._send_lock:
            self._sock.sendall(frame)

    def _read_exact(self, n: int) -> bytes | None:
        """``n`` bytes, or ``None`` at EOF/reset/close — recv_json turns that
        into its end-of-stream ``None``."""
        try:
            data = self._reader.read(n)
        except (OSError, ValueError):  # ValueError: read on a closed makefile
            return None
        if data is None or len(data) < n:
            return None
        return data

    def recv_json(self) -> dict[str, Any] | None:
        while True:
            head = self._read_exact(2)
            if head is None:
                return None
            opcode = head[0] & 0x0F
            masked = head[1] & 0x80
            length = head[1] & 0x7F
            if length == 126:
                ext = self._read_exact(2)
                if ext is None:
                    return None
                length = int.from_bytes(ext, "big")
            elif length == 127:
                ext = self._read_exact(8)
                if ext is None:
                    return None
                length = int.from_bytes(ext, "big")
            mask = b""
            if masked:
                mask = self._read_exact(4) or b""
                if not mask:
                    return None
            payload = b""
            if length:
                body = self._read_exact(length)
                if body is None:
                    return None
                payload = body
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:  # close
                return None
            if opcode == 0x9:  # WebSocket ping
                try:
                    self._send_control_frame(0xA, payload)
                except OSError:  # can't answer: the connection is dying
                    return None
                continue
            if opcode != 0x1:  # pong / binary / continuation: nothing to hand up
                continue
            try:
                msg = json.loads(payload.decode("utf-8"))
            except ValueError:
                continue
            if isinstance(msg, dict) and msg.get("type") == "ping":
                try:
                    self.send_json({"type": "pong", "t": msg.get("t")})
                except OSError:
                    return None
                continue
            return msg if isinstance(msg, dict) else None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._send_control_frame(0x8)
        except Exception:  # noqa: BLE001 — peer may be gone already
            pass
        # ``makefile`` holds its own reference to the socket, so close()
        # alone would leave a reader thread parked in recv() forever —
        # shutdown() is what actually unblocks it (EOF).
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._reader.close()
        except OSError:
            pass
        self._sock.close()


def open_duplex(url: str, *, token: str | None = None,
                connect_timeout_ms: int = 30_000) -> Duplex:
    """Open the relay's ``/socket`` WebSocket binding and return a blocking
    :class:`Duplex` — the synchronous twin of :func:`aep_sdk.aio.open_duplex`.
    The caller speaks the AEP-0003 duplex protocol itself (hello first);
    :class:`ControlClient` does that for the sender role.

    ``connect_timeout_ms`` bounds the WHOLE open: it is the socket timeout
    through TCP connect, TLS, and every upgrade read, so a peer that accepts
    the socket but never answers the upgrade cannot hang the caller (each
    blocking step is bounded by the budget, not their sum); expiry raises a
    ``network`` :class:`~aep_sdk.errors.TransportError` (same wording as the
    aio twin). The socket reverts to blocking (no timeout) before the
    :class:`Duplex` is returned — the duplex reads for as long as the
    connection lives."""
    parsed = urllib.parse.urlsplit(url if "://" in url else f"http://{url}")
    tls = parsed.scheme in ("https", "wss")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if tls else 80)
    path = parsed.path.rstrip("/") + "/socket"
    if token:
        path += "?" + urllib.parse.urlencode({"access_token": token})
    key = handshake_key()
    timeout_error = TransportError(
        "network", f"WebSocket connect timeout after {connect_timeout_ms} ms")
    try:
        sock = socket.create_connection((host, port),
                                        timeout=connect_timeout_ms / 1000)
    except socket.timeout as e:
        raise timeout_error from e
    reader: BufferedReader | None = None
    try:
        if tls:
            sock = _ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        reader = sock.makefile("rb")
        sock.sendall(upgrade_request(host, port, path, key))
        status_line = reader.readline()
        parts = status_line.decode("latin-1").split(" ", 2)
        if len(parts) < 2 or parts[1] != "101":
            status_text = parts[1] if len(parts) > 1 else "?"
            status = int(status_text) if status_text.isdigit() else None
            raise TransportError("http", f"WebSocket upgrade refused: HTTP {status_text}", status=status)
        headers: dict[str, str] = {}
        while True:
            line = reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        if headers.get("sec-websocket-accept") != expected_accept(key):
            raise TransportError("network", "WebSocket upgrade refused: bad sec-websocket-accept")
    except socket.timeout as e:
        if reader is not None:
            reader.close()
        sock.close()
        raise timeout_error from e
    except BaseException:  # refusals: close before propagating
        if reader is not None:
            reader.close()
        sock.close()
        raise
    sock.settimeout(None)
    return Duplex(sock, reader)


class _Waiter:
    """Settle-once holder for the sync :class:`ControlClient`'s ack and
    roster waits — the :class:`_Pending` pattern (threading.Event), settled
    only by the reader thread."""

    def __init__(self) -> None:
        self.settled = threading.Event()
        self.result: Any = None  # the ack Event dict, or the roster entries list
        self.error: NackError | None = None

    def resolve(self, result: Any) -> None:
        if not self.settled.is_set():
            self.result = result
            self.settled.set()

    def fail(self, error: NackError) -> None:
        if not self.settled.is_set():
            self.error = error
            self.settled.set()


class ControlClient:
    """Blocking control sender over an owned duplex — the synchronous twin of
    :class:`aep_sdk.aio.ControlClient`, mirroring :class:`ControlSender`: the
    same ``send(type, session, *, run, subject, cause, data, capture,
    ack_window_ms, retries)`` semantics, the same envelopes (built by the sync
    builder), and the same :class:`NackError` — on ``control.rejected``, on a
    relay-synthesized error frame (e.g. ``unsupported`` on the target's
    behalf, §4.2), or with reason ``timeout`` (synthesized locally, §3) after
    the window closes on the last retry. Every retry resends the SAME
    envelope: targets dedupe on ``id`` and never re-execute (§2.3). No
    auto-reconnect (aio parity).

    The client subscribes to the target session before the first send — acks
    are ordinary sequenced session Events, and a fresh consumer only receives
    live traffic. Inbound frames are dispatched by a daemon reader thread
    (``aep-control``) started by :meth:`connect`."""

    def __init__(self, url: str, *, agent: str, host: str,
                 token: str | None = None) -> None:
        self.url = url
        self.agent = agent
        self.host = host
        self._token = token
        self._builder = ControlSender(lambda _cmd: None, agent=agent, host=host)
        self._duplex: Duplex | None = None
        self._reader_thread: threading.Thread | None = None
        self._hello = threading.Event()
        self._lock = threading.Lock()  # waiter maps: reader thread vs caller threads
        self._pending: dict[str, _Waiter] = {}
        self._subscribed: dict[str, threading.Event] = {}
        # roster waiters are id-correlated (the §6.5 reply/refusal pattern) —
        # disjoint from the cause-correlated command waiters above
        self._pending_rosters: dict[str, _Waiter] = {}

    def connect(self, *, connect_timeout_ms: int = 30_000) -> None:
        """Open the duplex and complete the hello exchange (consumer role).

        ``connect_timeout_ms`` is one budget over the whole connect: it is
        passed to :func:`open_duplex` (TCP/TLS/upgrade) and the remainder
        bounds the hello wait; expiry raises a ``network``
        :class:`~aep_sdk.errors.TransportError` (same wording as the duplex
        open) with the half-open duplex closed."""
        deadline = time.monotonic() + connect_timeout_ms / 1000
        self._duplex = open_duplex(self.url, token=self._token,
                                   connect_timeout_ms=connect_timeout_ms)
        self._duplex.send_json({
            "type": "hello", "versions": ["0.1"], "role": "consumer",
            "identity": {"agent": self.agent, "instance": self.host},
            "capabilities": {},
        })
        self._reader_thread = threading.Thread(
            target=self._read_loop, name="aep-control", daemon=True)
        self._reader_thread.start()
        if not self._hello.wait(max(deadline - time.monotonic(), 0)):
            self.close()
            raise TransportError(
                "network",
                f"WebSocket connect timeout after {connect_timeout_ms} ms")

    def _read_loop(self) -> None:
        assert self._duplex is not None
        try:
            while True:
                msg = self._duplex.recv_json()
                if msg is None:
                    break
                kind = msg.get("type")
                if kind == "hello":
                    self._hello.set()
                elif kind == "subscribed":
                    ev = self._subscribed.get(str(msg.get("id")))
                    if ev:
                        ev.set()
                elif kind == "roster":
                    # the §4.1 reply: id-echoed, entries relayed verbatim
                    rid = msg.get("id")
                    with self._lock:
                        rw = self._pending_rosters.get(rid) if isinstance(rid, str) else None
                    if rw:
                        entries = msg.get("entries")
                        rw.resolve(entries if isinstance(entries, list) else [])
                elif kind == "error" and isinstance(msg.get("id"), str) \
                        and msg.get("id") in self._pending_rosters:
                    # an id-correlated refusal of one of OUR roster requests
                    with self._lock:
                        rw = self._pending_rosters.get(str(msg.get("id")))
                    if rw:
                        code = str(msg.get("code", "invalid"))
                        reason = code if code in _KNOWN_ERROR_CODES else "invalid"
                        rw.fail(NackError(reason, msg.get("detail")))
                elif kind == "error":
                    cause = msg.get("cause")
                    with self._lock:
                        w = self._pending.get(cause) if isinstance(cause, str) else None
                    if w:
                        code = str(msg.get("code", "invalid"))
                        wire_detail = msg.get("detail")
                        # The relay's error codes aren't drawn from the closed
                        # nack-reason set (they cover the wider wire protocol,
                        # e.g. `limit-exceeded`), so an unrecognized code is
                        # normalized to `invalid` with the raw code preserved
                        # in the detail — never leaked into NackError.reason.
                        reason = code if code in _KNOWN_ERROR_CODES else "invalid"
                        detail = ": ".join(
                            d for d in (code if code != reason else None, wire_detail) if d)
                        w.fail(NackError(reason, detail or None))
                elif kind in ("control.accepted", "control.rejected"):
                    cause = msg.get("cause")
                    with self._lock:
                        w = self._pending.get(cause) if isinstance(cause, str) else None
                    if w:
                        if kind == "control.accepted":
                            w.resolve(msg)
                        else:
                            data = msg.get("data") or {}
                            w.fail(NackError(data.get("reason", "invalid"),
                                             data.get("detail")))
        finally:
            # connection gone: fail whatever is still waiting
            with self._lock:
                waiters = list(self._pending.values()) + list(self._pending_rosters.values())
            for w in waiters:
                w.fail(NackError("timeout", "duplex closed", synthesized=True))

    def _ensure_subscribed(self, session: str) -> None:
        assert self._duplex is not None
        with self._lock:
            ev = self._subscribed.get(session)
            first = ev is None
            if ev is None:
                ev = threading.Event()
                self._subscribed[session] = ev
        if first:
            self._duplex.send_json({
                "type": "subscribe", "id": session, "filter": {"session": session},
            })
        ev.wait()

    def send(self, type: str, session: str, *,
             ack_window_ms: int = 10_000, retries: int = 2,
             **kwargs: Any) -> dict[str, Any]:
        """Send a command; returns the ``control.accepted`` Event or raises
        :class:`NackError` — exactly the :class:`ControlSender` contract."""
        if self._duplex is None:
            raise RuntimeError("ControlClient.send() before connect()")
        self._ensure_subscribed(session)
        cmd = self._builder.build(type, session, **kwargs)
        w = _Waiter()
        with self._lock:
            self._pending[cmd["id"]] = w
        attempts = 1 + max(retries, 0)
        try:
            for _ in range(attempts):
                self._duplex.send_json(cmd)
                if w.settled.wait(ack_window_ms / 1000):
                    break
            else:
                raise NackError("timeout", f"no ack within {ack_window_ms} ms x {attempts}",
                                synthesized=True)
        finally:
            with self._lock:
                self._pending.pop(cmd["id"], None)
        if w.error is not None:
            raise w.error
        assert w.result is not None
        return w.result

    def roster(self, *, timeout_ms: int = 10_000) -> list[dict[str, Any]]:
        """Request the endpoint's live-claim snapshot (the ``roster`` frame,
        AEP-0003 §4.1): returns the entries — declarations relayed verbatim,
        one entry per live claim — or raises :class:`NackError` on the
        in-band refusal or on timeout. Gate on the endpoint advertising
        ``capabilities.roster`` (hello / discovery document): an endpoint
        that does not implement the frame IGNORES it (§2), which this method
        can only report as the synthesized ``timeout``. A roster is a
        snapshot, never a promise — the target's declared ``accepts`` at
        command time stays authoritative (AEP-0004 §4)."""
        if self._duplex is None:
            raise RuntimeError("ControlClient.roster() before connect()")
        rid = f"ro-{ulid()}"
        w = _Waiter()
        with self._lock:
            self._pending_rosters[rid] = w
        try:
            self._duplex.send_json({"type": "roster", "id": rid})
            if not w.settled.wait(timeout_ms / 1000):
                raise NackError(
                    "timeout",
                    f"no roster reply within {timeout_ms} ms "
                    "(did the endpoint advertise capabilities.roster?)",
                    synthesized=True)
        finally:
            with self._lock:
                self._pending_rosters.pop(rid, None)
        if w.error is not None:
            raise w.error
        assert w.result is not None
        return w.result

    def close(self) -> None:
        """Close the duplex; pending sends settle as synthesized timeouts.
        Idempotent — safe to call more than once."""
        if self._duplex is not None:
            self._duplex.close()
