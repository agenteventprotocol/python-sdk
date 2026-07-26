"""Emit helpers: envelope construction with per-session (epoch, seq) ownership
(AEP-0001 §5/§7) and capture-ceiling stamping (§8.2)."""
from __future__ import annotations

import json
import math
import re
import secrets
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

Sink = Callable[[dict[str, Any]], None]

_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# The envelope's reserved attribute names (AEP-0001 §5.1) — the exact property
# set of the envelope schema. Extension attributes (§5.4) MUST NOT collide with
# any of them; on the wire a clobbered core attribute is indistinguishable from
# the attribute itself, so the collision can only be caught here, at the
# producing boundary. Mirror of ``aep-event.schema.json`` and the TypeScript SDK.
_RESERVED_ATTRS = frozenset({
    "aep", "agent", "capture", "cause", "data", "epoch", "id", "run", "seq",
    "session", "severity", "source", "step", "subject", "time", "traceparent", "type",
})
_EXT_NAME = re.compile(r"[a-z0-9]{1,20}\Z")


def _check_extensions(extensions: dict[str, Any]) -> dict[str, Any]:
    """Enforce AEP-0001 §5.4 at the producing boundary: an extension attribute
    whose name collides with a core attribute, is ill-formed, or carries a
    non-scalar value is refused (``ValueError``), never silently merged into the
    envelope. Returns the map unchanged when every entry is conformant."""
    for name, value in extensions.items():
        if name in _RESERVED_ATTRS:
            raise ValueError(
                f"extension attribute {name!r} collides with a reserved envelope attribute (AEP-0001 §5.4)")
        if not _EXT_NAME.match(name):
            raise ValueError(
                f"extension attribute name {name!r} must match [a-z0-9]{{1,20}} (AEP-0001 §5.4)")
        # bool is an int subclass — check it first so True/False stay legal,
        # then integers; floats and everything else are refused.
        if not (isinstance(value, (bool, str)) or (isinstance(value, int) and not isinstance(value, bool))):
            raise ValueError(
                f"extension attribute {name!r} must be a string, integer, or boolean (AEP-0001 §5.4)")
    return extensions


def ulid(now_ms: int | None = None) -> str:
    """ULID (Crockford base32; 48-bit ms time + 80-bit random)."""
    t = now_ms if now_ms is not None else time.time_ns() // 1_000_000
    out = ""
    for _ in range(10):
        out = _B32[t % 32] + out
        t = math.floor(t / 32)
    rnd = secrets.token_bytes(16)
    return out + "".join(_B32[b % 32] for b in rnd)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Emitter:
    """Builds valid envelopes for one producer; hand out one session() per session.

    epoch (AEP-0001 §7) MUST increase every time the emitter restarts without
    durable seq state; the caller owns persistence (default 0 suits single-shot
    processes and tests).
    """

    def __init__(self, agent: str, host: str, sink: Sink, *,
                 epoch: int = 0, capture: str = "metadata") -> None:
        self.agent = agent
        self.host = host
        self.epoch = epoch
        self.capture = capture
        self._sink = sink

    def emit(self, type: str, data: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        """Agent-scoped event (``agent.*`` and other session-less types)."""
        ev = self.build(type, data, opts)
        self._sink(ev)
        return ev

    def session(self, session_id: str) -> "SessionEmitter":
        return SessionEmitter(self, session_id)

    def build(self, type: str, data: dict[str, Any] | None,
              opts: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        ev: dict[str, Any] = {
            "aep": "0.1",
            "id": ulid(),
            "type": type,
            "time": _now_iso(),
            "source": f"aep://{self.host}/agent/{self.agent}"
                      + (f"/session/{session_id}" if session_id else ""),
            "agent": self.agent,
        }
        # Extensions are validated at the producing boundary (mirrors the
        # TypeScript SDK): a key that collides with a core attribute, is
        # ill-formed, or is non-scalar is refused (AEP-0001 §5.4) rather than
        # merged — a collision is invisible once on the wire. Conformant
        # extensions merge in before the option fields below, which are applied
        # after and always win, same as `capture` and `data`.
        ev.update(_check_extensions(opts.get("extensions") or {}))
        for key in ("subject", "run", "step", "cause", "severity", "traceparent"):
            if opts.get(key) is not None:
                ev[key] = opts[key]
        ev["capture"] = opts.get("capture") or self.capture
        if data:
            ev["data"] = data
        return ev


class SessionEmitter:
    """Session-scoped emission: stamps session, epoch, and a monotonic seq."""

    def __init__(self, emitter: Emitter, session_id: str) -> None:
        self._emitter = emitter
        self.session_id = session_id
        self._seq = 0

    def emit(self, type: str, data: dict[str, Any] | None = None, **opts: Any) -> dict[str, Any]:
        ev = self._emitter.build(type, data, opts, self.session_id)
        ev["session"] = self.session_id
        ev["epoch"] = self._emitter.epoch
        ev["seq"] = self._seq
        self._seq += 1
        self._emitter._sink(ev)
        return ev

    def redeliver(self, ev: dict[str, Any]) -> None:
        """Re-deliver an already-emitted Event verbatim — same ``id``, same
        ``(epoch, seq)`` position: legal at-least-once redelivery that
        consumer dedupe collapses (AEP-0001 §7.4). This is the transport for
        AEP-0004 §3's re-acknowledgement of a retransmitted command; the
        counter is untouched."""
        self._emitter._sink(ev)


def http_sink(url: str, token: str | None = None,
              on_error: Callable[[Exception], None] | None = None,
              *, timeout_ms: int = 10_000) -> Sink:
    """POST each event to the relay's HTTP ingest (AEP-Version header included).

    ``timeout_ms`` is the per-POST ``urlopen`` timeout (urllib semantics: it
    covers the connect and each socket read); expiry flows to ``on_error``
    like any other delivery failure."""
    endpoint = url.rstrip("/") + "/events"

    def sink(ev: dict[str, Any]) -> None:
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(ev).encode(),
            headers={"content-type": "application/json", "AEP-Version": "0.1",
                     **({"authorization": f"Bearer {token}"} if token else {})},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=timeout_ms / 1000).read()
        except Exception as e:  # noqa: BLE001 — emitters must never crash the host
            if on_error:
                on_error(e)

    return sink


def jsonl_sink(path: str) -> Sink:
    """Append events to a JSONL file (the stdio/file binding, AEP-0003 §3.1)."""

    def sink(ev: dict[str, Any]) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, separators=(",", ":")) + "\n")

    return sink
