"""Exported test doubles — relay-free. ``MemorySink`` records what an
:class:`~aep_sdk.emit.Emitter` produces, ``ScriptedSource`` scripts a whole
emitter (real envelopes: per-session ``(epoch, seq)`` ownership per AEP-0001
§7, ``aep://`` source URIs per §5.2), and ``ControlStub`` covers both sides
of the control plane (AEP-0004): a recording target emitter for
:class:`~aep_sdk.control.ControlTarget`, and a capturing transport plus
ack/nack minting for :class:`~aep_sdk.control.ControlSender`. Everything is
built on the SDK's own envelope machinery — no envelope is hand-forked, so
what a test observes is what production code emits.
"""
from __future__ import annotations

from typing import Any, Callable

from .control import ControlSender
from .emit import Emitter, SessionEmitter
from .projection import _COMMAND_TYPE


class MemorySink:
    """A ``Sink`` that records: pass the instance wherever a sink callable is
    expected (``Emitter(agent, host, MemorySink())``); read ``.events``."""

    def __init__(self) -> None:
        #: the recorded Events, in emit order (the exact emitted dicts)
        self.events: list[dict[str, Any]] = []

    def __call__(self, ev: dict[str, Any]) -> None:
        self.events.append(ev)

    def clear(self) -> None:
        """Empty the record in place."""
        self.events.clear()


class ScriptedSource:
    """Scripts one event source: real :class:`SessionEmitter` instances over
    one shared in-memory recorder, so every scripted event is a well-formed
    envelope (fresh ULID ids, ``session``/``epoch``/``seq`` stamped, seq
    contiguous from 0 per session).

    ``source`` overrides the recorded envelopes' ``source`` URI (useful for
    pinning corpus-style identities); by default the emitter-built
    ``aep://<host>/agent/<agent>/...`` URI stands.
    """

    def __init__(self, *, source: str | None = None, agent: str = "scripted",
                 host: str = "scripted-host") -> None:
        self.agent = agent
        self.host = host
        self._source = source
        self._sink = MemorySink()

    def _record(self, ev: dict[str, Any]) -> None:
        if self._source is not None:
            ev["source"] = self._source
        self._sink(ev)

    def session(self, session_id: str, *, epoch: int = 0) -> SessionEmitter:
        """A real :class:`SessionEmitter` for ``session_id`` at ``epoch``.
        Each call mints a fresh internal :class:`Emitter` (epoch is
        emitter-wide, AEP-0001 §7) over the shared recorder."""
        return Emitter(self.agent, self.host, self._record,
                       epoch=epoch).session(session_id)

    @property
    def events(self) -> list[dict[str, Any]]:
        """Everything emitted so far, across sessions, in emit order."""
        return list(self._sink.events)

    def play(self, on_event: Callable[[dict[str, Any]], None]) -> None:
        """Deliver the recorded events to ``on_event`` in emit order — feed a
        consumer (e.g. a :class:`~aep_sdk.projection.StateProjection`)."""
        for ev in list(self._sink.events):
            on_event(ev)


class ControlStub:
    """Both sides of the control plane (AEP-0004), no relay required.

    Target side: ``.emitter`` is a real :class:`SessionEmitter` over an
    internal recorder — hand it to :class:`ControlTarget`; every ack it emits
    (including byte-identical re-acks via ``redeliver``) lands in ``.acks``.
    ``.command()`` mints well-formed command frames to drive the target with
    (built by the SDK's own command builder: fresh ULID id, the target
    ``session``, **no** ``seq`` — AEP-0004 §2.2).

    Client side: pass ``.send`` to :class:`ControlSender` as its transport;
    outbound frames are captured in ``.sent``, and ``.accept()`` /
    ``.reject()`` mint the matching ``control.accepted`` /
    ``control.rejected`` acks (``cause`` = the command's id) to feed back
    through ``sender.on_event()``.
    """

    def __init__(self, *, source: str | None = None, agent: str = "control-stub",
                 host: str = "stub-host", session: str = "stub-target") -> None:
        self.agent = agent
        self.host = host
        self._source = source
        self._recorder = MemorySink()
        self._emitter = Emitter(agent, host, self._record)
        self._sessions: dict[str, SessionEmitter] = {}
        #: target-side session emitter (real ``emit`` + ``redeliver`` — the
        #: exact duck surface ControlTarget uses)
        self.emitter: SessionEmitter = self._session(session)
        #: client side: outbound command frames captured by ``.send``
        self.sent: list[dict[str, Any]] = []
        # command frames are built, never hand-forked: reuse the sender's
        # envelope builder (its transport is never invoked by build())
        self._builder = ControlSender(self.send, agent=agent, host=host)

    def _record(self, ev: dict[str, Any]) -> None:
        if self._source is not None:
            ev["source"] = self._source
        self._recorder(ev)

    def _session(self, session_id: str) -> SessionEmitter:
        emitter = self._sessions.get(session_id)
        if emitter is None:
            emitter = self._emitter.session(session_id)
            self._sessions[session_id] = emitter
        return emitter

    @property
    def acks(self) -> list[dict[str, Any]]:
        """Every Event the stub's emitters produced, in emit order (acks,
        nacks, and byte-identical re-acks included)."""
        return list(self._recorder.events)

    def command(self, type: str, session: str, *, run: str | None = None,
                subject: str | None = None,
                data: dict[str, Any] | None = None) -> dict[str, Any]:
        """A well-formed command frame addressed to ``session`` (AEP-0004
        §2.2: fresh id, sender ``source``/``agent``, ``time``, no ``seq``).
        Refuses a ``type`` outside the command-frame class."""
        if not _COMMAND_TYPE.match(type):
            raise ValueError(
                f"{type!r} is not a command type (AEP-0004 §2.2: control.* "
                "verbs or x.<vendor>.control.*)")
        cmd = self._builder.build(type, session, run=run, subject=subject,
                                  data=data)
        if self._source is not None:
            cmd["source"] = self._source
        return cmd

    def send(self, frame: dict[str, Any]) -> None:
        """Client-side transport: capture an outbound frame (pass this as
        ``ControlSender``'s ``transport_send``)."""
        self.sent.append(frame)

    def accept(self, cmd: dict[str, Any]) -> dict[str, Any]:
        """Mint the ``control.accepted`` ack for ``cmd`` (``cause`` = the
        command's id), emitted on the command's target session; feed it to
        ``sender.on_event()``."""
        return self._session(cmd["session"]).emit(
            "control.accepted", {}, cause=cmd["id"])

    def reject(self, cmd: dict[str, Any], *, reason: str = "refused",
               detail: str | None = None) -> dict[str, Any]:
        """Mint the ``control.rejected`` nack for ``cmd`` (``cause`` = the
        command's id) with the given ``reason`` (AEP-0004 §3) and optional
        ``detail``; feed it to ``sender.on_event()``."""
        data: dict[str, Any] = {"reason": reason}
        if detail is not None:
            data["detail"] = detail
        return self._session(cmd["session"]).emit(
            "control.rejected", data, cause=cmd["id"])
