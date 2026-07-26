"""Asyncio consumer and control sender: SSE subscription mirroring
:mod:`aep_sdk.consume`'s surface (filter, capture, token, ``from_`` resume,
id-dedupe, reconnect with backoff) as an async iterable, and a
:class:`ControlClient` mirroring :class:`aep_sdk.control.ControlSender`'s
``send()`` semantics over an owned WebSocket duplex. Standard library only —
the SSE transport is ``asyncio.open_connection`` with a hand-rolled HTTP/1.1
GET, chunked transfer decoding, and SSE line parsing; the duplex is the same
``open_connection`` with the RFC 6455 client handshake and masked text
frames (:func:`open_duplex`; byte layout shared with the sync twin in
:mod:`aep_sdk._ws`)."""
from __future__ import annotations

import asyncio
import json
import ssl as _ssl
import urllib.parse
from collections import OrderedDict
from typing import Any, AsyncIterator, Callable

from ._ws import (encode_control_frame, encode_text_frame, expected_accept,
                  handshake_key, upgrade_request)
from .control import _KNOWN_ERROR_CODES, ControlSender, NackError
from .consume import (_FROM_BUDGET, _MAX_REPLAY_CHUNKS, _as_transport_error,
                      _split_from)
from .emit import ulid
from .errors import TransportError


class AsyncSubscription:
    """Async-iterable of event dicts; ``positions()``/``close()`` mirror the
    sync :class:`aep_sdk.consume.Subscription`."""

    def __init__(self) -> None:
        self._positions: dict[str, dict[str, Any]] = {}
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._closed = False
        self._task: asyncio.Task[None] | None = None

    def close(self) -> None:
        """Stop the subscription; a pending ``async for`` ends promptly."""
        if self._closed:
            return
        self._closed = True
        if self._task:
            self._task.cancel()
        self._queue.put_nowait(None)

    def positions(self) -> list[dict[str, Any]]:
        """Latest per-session positions observed — persist these to resume later."""
        return list(self._positions.values())

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        ev = await self._queue.get()
        if ev is None:
            raise StopAsyncIteration
        return ev


async def _read_http_body(reader: asyncio.StreamReader,
                          on_text: Callable[[str], None]) -> None:
    """Read one HTTP/1.1 response and stream its body text to ``on_text``.

    Raises :class:`~aep_sdk.errors.TransportError` (kind ``"http"``, with
    ``status`` set) on a non-200 status, and kind ``"network"`` on bad chunk
    framing. Handles chunked transfer encoding (what a streaming SSE response
    normally uses), Content-Length, and read-to-EOF bodies."""
    status_line = await reader.readline()
    parts = status_line.decode("latin-1").split(" ", 2)
    if len(parts) < 2 or parts[1] != "200":
        status_text = parts[1] if len(parts) > 1 else "?"
        status = int(status_text) if status_text.isdigit() else None
        raise TransportError("http", f"SSE HTTP {status_text}", status=status)
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, _, value = line.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()

    if headers.get("transfer-encoding", "").lower() == "chunked":
        while True:
            size_line = await reader.readline()
            try:
                size = int(size_line.strip().split(b";")[0], 16)
            except ValueError as e:
                raise TransportError("network", "bad chunk framing") from e
            if size == 0:
                return
            body = await reader.readexactly(size)
            await reader.readexactly(2)  # trailing CRLF
            on_text(body.decode("utf-8", "replace"))
    elif "content-length" in headers:
        remaining = int(headers["content-length"])
        while remaining:
            chunk = await reader.read(min(remaining, 65536))
            if not chunk:
                return
            remaining -= len(chunk)
            on_text(chunk.decode("utf-8", "replace"))
    else:
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return
            on_text(chunk.decode("utf-8", "replace"))


async def subscribe(url: str,
                    *,
                    filter: dict[str, Any] | None = None,
                    capture: str | None = None,
                    token: str | None = None,
                    from_: list[dict[str, Any]] | str | None = None,
                    live: bool = True,
                    on_comment: Callable[[str], None] | None = None,
                    on_error: Callable[[Exception], None] | None = None,
                    backoff_initial_ms: int = 500,
                    backoff_max_ms: int = 10_000,
                    from_budget: int = _FROM_BUDGET,
                    max_replay_chunks: int = _MAX_REPLAY_CHUNKS,
                    connect_timeout_ms: int = 30_000) -> AsyncSubscription:
    """Subscribe over SSE and return an :class:`AsyncSubscription` to iterate.

    Mirrors :func:`aep_sdk.consume.subscribe` — including ``from_="all"``
    (replay-all, AEP-0003 §5: the first request replays every session the
    endpoint holds; positions govern reconnects; a 4xx refusal reports via
    ``on_error`` and falls back to live-only). Returns once the stream is
    established. With ``live=False`` the relay serves buffered history only
    and iteration ends by itself at the ``replay-complete`` marker.

    The tuning knobs mirror the sync surface:
    ``backoff_initial_ms``/``backoff_max_ms`` set the reconnect schedule,
    ``from_budget`` caps the encoded ``from`` parameter on the live request,
    and ``max_replay_chunks`` bounds the ``live=0`` drain connections per
    reconnect. ``connect_timeout_ms`` bounds the connection establishment
    (``open_connection`` through the GET write) of each attempt and surfaces
    expiry as a ``network`` :class:`~aep_sdk.errors.TransportError` on the
    normal retry path; body reads stay unbounded — an SSE stream must idle
    freely."""
    sub = AsyncSubscription()
    pending_all = from_ == "all"
    if isinstance(from_, list):
        for p in from_:
            sub._positions[p["session"]] = p
    seen: OrderedDict[tuple[Any, str], None] = OrderedDict()
    parsed = urllib.parse.urlsplit(url if "://" in url else f"http://{url}")
    tls = parsed.scheme == "https"
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if tls else 80)
    base_path = parsed.path.rstrip("/")
    connected = asyncio.Event()
    done = False

    def deliver(text: str) -> None:
        nonlocal done
        try:
            ev = json.loads(text)
        except ValueError as e:
            if on_error:
                on_error(TransportError("parse", f"undecodable SSE payload: {e}"))
            return
        ev_id = ev.get("id")
        if not isinstance(ev_id, str):
            return
        key = (ev.get("source"), ev_id)  # dedupe on (source, id) — AEP-0001 §7.4
        if key in seen:
            return
        seen[key] = None
        if len(seen) > 50_000:
            seen.popitem(last=False)
        if ev.get("session") is not None and ev.get("seq") is not None:
            sub._positions[ev["session"]] = {
                "session": ev["session"], "epoch": ev.get("epoch", 0), "seq": ev["seq"],
            }
        sub._queue.put_nowait(ev)

    buf = ""
    data_lines: list[str] = []
    terminal_conn = False

    def on_text(text: str) -> None:
        nonlocal buf, done
        buf += text
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            line = line.rstrip("\r")
            if line == "":
                if data_lines:
                    deliver("\n".join(data_lines))
                    data_lines.clear()
                continue
            if line.startswith(":"):
                comment = line[1:].strip()
                if on_comment:
                    on_comment(comment)
                # only the FINAL (subscriber-facing) replay connection ends the
                # subscription — internal drain chunks end at their own marker
                # without touching it
                if terminal_conn and comment.startswith("replay-complete"):
                    done = True
                continue
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))

    async def connect_once(from_list: list[dict[str, Any]] | str, live_conn: bool) -> None:
        nonlocal terminal_conn
        terminal_conn = live_conn is live and not live  # the subscriber-facing replay pass
        params = [("filter", json.dumps(filter or {}))]
        if capture:
            params.append(("capture", capture))
        if token:
            params.append(("access_token", token))
        if not live_conn:
            params.append(("live", "0"))
        if from_list == "all":
            params.append(("from", "all"))
        elif from_list:
            params.append(("from", json.dumps(from_list)))
        path = f"{base_path}/stream?{urllib.parse.urlencode(params)}"
        ctx = _ssl.create_default_context() if tls else None

        async def establish() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            reader, writer = await asyncio.open_connection(host, port, ssl=ctx)
            try:
                writer.write((f"GET {path} HTTP/1.1\r\n"
                              f"host: {host}:{port}\r\n"
                              "accept: text/event-stream\r\n"
                              "connection: close\r\n\r\n").encode("latin-1"))
                await writer.drain()
            except BaseException:  # incl. the wait_for cancellation below
                writer.close()
                raise
            return reader, writer

        # connect_timeout_ms bounds establishment only (open_connection
        # through the GET write); the body read below is unbounded — an SSE
        # stream must be free to idle between events.
        try:
            reader, writer = await asyncio.wait_for(
                establish(), connect_timeout_ms / 1000)
        except asyncio.TimeoutError:
            raise TransportError(
                "network",
                f"connect timeout after {connect_timeout_ms} ms") from None
        try:
            connected.set()
            await _read_http_body(reader, on_text)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass

    async def loop() -> None:
        nonlocal pending_all
        backoff = backoff_initial_ms / 1000
        from_cap: int | None = None  # shrinks when a resume-carrying request is refused
        while not sub._closed and not done:
            sent_from = 0
            sent_all = False
            try:
                # replay-all rides the first request only (§5): positions
                # govern every reconnect once events flow or it was refused.
                use_all = pending_all and not sub._positions
                sent_all = use_all
                if use_all:
                    head, rest = [], []
                else:
                    head, rest = _split_from(list(sub._positions.values()), from_cap,
                                             from_budget)
                sent_from = len(head)
                # positions beyond the budget drain through bounded replay-only
                # connections first; a failed chunk costs history, never the pipe
                chunks = 0
                while rest and chunks < max_replay_chunks and not sub._closed:
                    chunk, rest = _split_from(rest, None, from_budget)
                    try:
                        await connect_once(chunk, False)
                    except asyncio.CancelledError:
                        return
                    except Exception as e:  # noqa: BLE001 — reported, then dropped
                        if not sub._closed and on_error:
                            on_error(_as_transport_error(e))
                        break
                    chunks += 1
                await connect_once("all" if use_all else head, live)
                pending_all = False
                backoff = backoff_initial_ms / 1000
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001 — surfaced via on_error, then retried
                if not sub._closed:
                    err = _as_transport_error(e)
                    if on_error:
                        on_error(err)
                    # the endpoint refused from=all (deterministically, §5 —
                    # it does not implement the value): fall back to live-only
                    if (err.kind == "http" and err.status is not None
                            and 400 <= err.status < 500 and sent_all):
                        pending_all = False
                        await asyncio.sleep(0.15)
                        continue
                    # self-heal: a 4xx on a resume-carrying request shrinks the
                    # set instead of looping dead; network errors keep it — the
                    # relay may be down, and resume must survive its return
                    if (err.kind == "http" and err.status is not None
                            and 400 <= err.status < 500 and sent_from > 0):
                        from_cap = sent_from // 2
                        await asyncio.sleep(0.15)
                        continue
            if done or not live or sub._closed:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max_ms / 1000)
        sub._queue.put_nowait(None)  # end iteration

    sub._task = asyncio.ensure_future(loop())
    await connected.wait()
    return sub


# --------------------------------------------------------------------------
# WebSocket duplex (control requires an authenticated duplex binding,
# AEP-0004 §4.1; WebSocket in 0.1). Client side of RFC 6455: handshake over
# ``asyncio.open_connection``, masked text frames out, unmasked frames in.
# The pure byte layout (handshake key/accept, upgrade request, frame
# encoding) lives in ``aep_sdk._ws``, shared with the sync twin in
# ``aep_sdk.control``; frame *decode* stays here, interleaved with this
# flavor's reads.


class Duplex:
    """A JSON-message WebSocket connection (see :func:`open_duplex`).

    ``recv_json()`` answers WebSocket pings and the relay's application-level
    ``{"type": "ping"}`` frames itself and returns ``None`` at close. A frame
    whose payload isn't decodable JSON is skipped silently (the connection
    stays open; there's no caller-supplied ``on_error`` at this layer to
    report it to)."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def send_json(self, obj: dict[str, Any]) -> None:
        self._writer.write(encode_text_frame(json.dumps(obj).encode("utf-8")))
        await self._writer.drain()

    async def _send_control_frame(self, opcode: int, payload: bytes = b"") -> None:
        self._writer.write(encode_control_frame(opcode, payload))
        await self._writer.drain()

    async def recv_json(self) -> dict[str, Any] | None:
        while True:
            try:
                head = await self._reader.readexactly(2)
            except (asyncio.IncompleteReadError, ConnectionError):
                return None
            opcode = head[0] & 0x0F
            masked = head[1] & 0x80
            length = head[1] & 0x7F
            if length == 126:
                length = int.from_bytes(await self._reader.readexactly(2), "big")
            elif length == 127:
                length = int.from_bytes(await self._reader.readexactly(8), "big")
            mask = await self._reader.readexactly(4) if masked else b""
            payload = await self._reader.readexactly(length) if length else b""
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:  # close
                return None
            if opcode == 0x9:  # WebSocket ping
                await self._send_control_frame(0xA, payload)
                continue
            if opcode != 0x1:  # pong / binary / continuation: nothing to hand up
                continue
            try:
                msg = json.loads(payload.decode("utf-8"))
            except ValueError:
                continue
            if isinstance(msg, dict) and msg.get("type") == "ping":
                await self.send_json({"type": "pong", "t": msg.get("t")})
                continue
            return msg if isinstance(msg, dict) else None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._send_control_frame(0x8)
        except Exception:  # noqa: BLE001 — peer may be gone already
            pass
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except Exception:  # noqa: BLE001 — best-effort teardown
            pass


async def open_duplex(url: str, *, token: str | None = None,
                      connect_timeout_ms: int = 30_000) -> Duplex:
    """Open the relay's ``/socket`` WebSocket binding and return a
    :class:`Duplex`. The caller speaks the AEP-0003 duplex protocol itself
    (hello first); :class:`ControlClient` does that for the sender role.

    ``connect_timeout_ms`` bounds the WHOLE open — TCP connect, TLS, the
    upgrade request, and the 101/``sec-websocket-accept`` verification — so a
    peer that accepts the socket but never answers the upgrade cannot hang
    the caller; expiry raises a ``network``
    :class:`~aep_sdk.errors.TransportError`. The returned duplex itself reads
    unbounded (it lives as long as the connection)."""
    parsed = urllib.parse.urlsplit(url if "://" in url else f"http://{url}")
    tls = parsed.scheme in ("https", "wss")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if tls else 80)
    path = parsed.path.rstrip("/") + "/socket"
    if token:
        path += "?" + urllib.parse.urlencode({"access_token": token})
    key = handshake_key()
    ctx = _ssl.create_default_context() if tls else None

    async def establish() -> Duplex:
        reader, writer = await asyncio.open_connection(host, port, ssl=ctx)
        try:
            writer.write(upgrade_request(host, port, path, key))
            await writer.drain()
            status_line = await reader.readline()
            parts = status_line.decode("latin-1").split(" ", 2)
            if len(parts) < 2 or parts[1] != "101":
                status_text = parts[1] if len(parts) > 1 else "?"
                status = int(status_text) if status_text.isdigit() else None
                raise TransportError(
                    "http", f"WebSocket upgrade refused: HTTP {status_text}", status=status)
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin-1").partition(":")
                headers[name.strip().lower()] = value.strip()
            if headers.get("sec-websocket-accept") != expected_accept(key):
                raise TransportError(
                    "network", "WebSocket upgrade refused: bad sec-websocket-accept")
        except BaseException:  # refusals + the wait_for cancellation below
            writer.close()
            raise
        return Duplex(reader, writer)

    try:
        return await asyncio.wait_for(establish(), connect_timeout_ms / 1000)
    except asyncio.TimeoutError:
        raise TransportError(
            "network",
            f"WebSocket connect timeout after {connect_timeout_ms} ms") from None


# --------------------------------------------------------------------------
# Async control sender (AEP-0004), mirroring the sync surface.

class ControlClient:
    """Async control sender over an owned duplex, mirroring
    :class:`aep_sdk.control.ControlSender`: the same ``send(type, session, *,
    run, subject, cause, data, capture, ack_window_ms, retries)`` semantics,
    the same envelopes (built by the sync builder), and the same
    :class:`~aep_sdk.control.NackError` — on ``control.rejected``, on a
    relay-synthesized error frame (e.g. ``unsupported`` on the target's
    behalf, §4.2), or with reason ``timeout`` (synthesized locally, §3) after
    the window closes on the last retry. Every retry resends the SAME
    envelope: targets dedupe on ``id`` and never re-execute (§2.3).

    The client subscribes to the target session before the first send — acks
    are ordinary sequenced session Events, and a fresh consumer only
    receives live traffic."""

    def __init__(self, url: str, *, agent: str, host: str,
                 token: str | None = None) -> None:
        self.url = url
        self.agent = agent
        self.host = host
        self._token = token
        self._builder = ControlSender(lambda _cmd: None, agent=agent, host=host)
        self._duplex: Duplex | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._hello: asyncio.Event = asyncio.Event()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._subscribed: dict[str, asyncio.Event] = {}
        # roster waiters are id-correlated (the §6.5 reply/refusal pattern) —
        # disjoint from the cause-correlated command waiters above
        self._pending_rosters: dict[str, asyncio.Future[list[dict[str, Any]]]] = {}

    async def connect(self, *, connect_timeout_ms: int = 30_000) -> None:
        """Open the duplex and complete the hello exchange (consumer role).

        ``connect_timeout_ms`` is one budget over the whole connect: it is
        passed to :func:`open_duplex` (TCP/TLS/upgrade) and the remainder
        bounds the hello wait; expiry raises a ``network``
        :class:`~aep_sdk.errors.TransportError` (same wording as the duplex
        open) with the half-open duplex closed."""
        deadline = asyncio.get_running_loop().time() + connect_timeout_ms / 1000
        self._duplex = await open_duplex(self.url, token=self._token,
                                         connect_timeout_ms=connect_timeout_ms)
        await self._duplex.send_json({
            "type": "hello", "versions": ["0.1"], "role": "consumer",
            "identity": {"agent": self.agent, "instance": self.host},
            "capabilities": {},
        })
        self._reader_task = asyncio.ensure_future(self._read_loop())
        remaining = deadline - asyncio.get_running_loop().time()
        try:
            await asyncio.wait_for(self._hello.wait(), max(remaining, 0))
        except asyncio.TimeoutError:
            await self.close()
            raise TransportError(
                "network",
                f"WebSocket connect timeout after {connect_timeout_ms} ms") from None

    async def _read_loop(self) -> None:
        assert self._duplex is not None
        while True:
            msg = await self._duplex.recv_json()
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
                rfut = self._pending_rosters.get(rid) if isinstance(rid, str) else None
                if rfut and not rfut.done():
                    entries = msg.get("entries")
                    rfut.set_result(entries if isinstance(entries, list) else [])
            elif kind == "error" and isinstance(msg.get("id"), str) \
                    and msg.get("id") in self._pending_rosters:
                # an id-correlated refusal of one of OUR roster requests
                rfut = self._pending_rosters[str(msg.get("id"))]
                if not rfut.done():
                    code = str(msg.get("code", "invalid"))
                    reason = code if code in _KNOWN_ERROR_CODES else "invalid"
                    rfut.set_exception(NackError(reason, msg.get("detail")))
            elif kind == "error":
                cause = msg.get("cause")
                fut = self._pending.get(cause) if isinstance(cause, str) else None
                if fut and not fut.done():
                    code = str(msg.get("code", "invalid"))
                    wire_detail = msg.get("detail")
                    # The relay's error codes aren't drawn from the closed
                    # nack-reason set (they cover the wider wire protocol, e.g.
                    # `limit-exceeded`), so an unrecognized code is normalized
                    # to `invalid` with the raw code preserved in the detail —
                    # never leaked into NackError.reason.
                    reason = code if code in _KNOWN_ERROR_CODES else "invalid"
                    detail = ": ".join(
                        d for d in (code if code != reason else None, wire_detail) if d)
                    fut.set_exception(NackError(reason, detail or None))
            elif kind in ("control.accepted", "control.rejected"):
                cause = msg.get("cause")
                fut = self._pending.get(cause) if isinstance(cause, str) else None
                if fut and not fut.done():
                    if kind == "control.accepted":
                        fut.set_result(msg)
                    else:
                        data = msg.get("data") or {}
                        fut.set_exception(NackError(data.get("reason", "invalid"),
                                                    data.get("detail")))
        # connection gone: fail whatever is still waiting
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(NackError("timeout", "duplex closed", synthesized=True))
        for rfut in self._pending_rosters.values():
            if not rfut.done():
                rfut.set_exception(NackError("timeout", "duplex closed", synthesized=True))

    async def _ensure_subscribed(self, session: str) -> None:
        assert self._duplex is not None
        ev = self._subscribed.get(session)
        if ev is None:
            ev = asyncio.Event()
            self._subscribed[session] = ev
            await self._duplex.send_json({
                "type": "subscribe", "id": session, "filter": {"session": session},
            })
        await ev.wait()

    async def send(self, type: str, session: str, *,
                   ack_window_ms: int = 10_000, retries: int = 2,
                   **kwargs: Any) -> dict[str, Any]:
        """Send a command; returns the ``control.accepted`` Event or raises
        :class:`~aep_sdk.control.NackError` — exactly the sync contract."""
        if self._duplex is None:
            raise RuntimeError("ControlClient.send() before connect()")
        await self._ensure_subscribed(session)
        cmd = self._builder.build(type, session, **kwargs)
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[cmd["id"]] = fut
        attempts = 1 + max(retries, 0)
        try:
            for _ in range(attempts):
                await self._duplex.send_json(cmd)
                try:
                    return await asyncio.wait_for(asyncio.shield(fut), ack_window_ms / 1000)
                except asyncio.TimeoutError:
                    continue
            raise NackError("timeout", f"no ack within {ack_window_ms} ms x {attempts}",
                            synthesized=True)
        finally:
            self._pending.pop(cmd["id"], None)
            if not fut.done():
                fut.cancel()
            elif not fut.cancelled():
                fut.exception()  # a late nack must not warn as never-retrieved

    async def roster(self, *, timeout_ms: int = 10_000) -> list[dict[str, Any]]:
        """Request the endpoint's live-claim snapshot (the ``roster`` frame,
        AEP-0003 §4.1): returns the entries — declarations relayed verbatim,
        one entry per live claim — or raises
        :class:`~aep_sdk.control.NackError` on the in-band refusal or on
        timeout. Gate on the endpoint advertising ``capabilities.roster``
        (hello / discovery document): an endpoint that does not implement the
        frame IGNORES it (§2), which this method can only report as the
        synthesized ``timeout``. A roster is a snapshot, never a promise —
        the target's declared ``accepts`` at command time stays
        authoritative (AEP-0004 §4)."""
        if self._duplex is None:
            raise RuntimeError("ControlClient.roster() before connect()")
        rid = f"ro-{ulid()}"
        fut: asyncio.Future[list[dict[str, Any]]] = asyncio.get_running_loop().create_future()
        self._pending_rosters[rid] = fut
        try:
            await self._duplex.send_json({"type": "roster", "id": rid})
            try:
                return await asyncio.wait_for(asyncio.shield(fut), timeout_ms / 1000)
            except asyncio.TimeoutError:
                raise NackError(
                    "timeout",
                    f"no roster reply within {timeout_ms} ms "
                    "(did the endpoint advertise capabilities.roster?)",
                    synthesized=True) from None
        finally:
            self._pending_rosters.pop(rid, None)
            if not fut.done():
                fut.cancel()
            elif not fut.cancelled():
                fut.exception()  # a late refusal must not warn as never-retrieved

    async def close(self) -> None:
        """Close the duplex; pending sends settle as synthesized timeouts."""
        if self._reader_task:
            self._reader_task.cancel()
        if self._duplex is not None:
            await self._duplex.close()
