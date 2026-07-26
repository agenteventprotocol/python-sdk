"""Transport-error taxonomy for the emit/consume/control transports.

``NackError`` (in :mod:`aep_sdk.control`) is protocol-level: a command was
delivered and a peer answered, or the relay answered on the target's behalf.
Everything here is a level below that — the delivery itself didn't complete
as a well-formed protocol exchange. A nack is never a :class:`TransportError`
and a :class:`TransportError` is never a nack.
"""
from __future__ import annotations


class TransportError(ConnectionError):
    """Raised (or, for the ``parse`` kind, reported to ``on_error``) by the
    SSE and WebSocket transports in :mod:`aep_sdk.consume` and
    :mod:`aep_sdk.aio`.

    Subclasses ``ConnectionError`` on purpose: a call site that catches
    ``ConnectionError`` (or the broader ``OSError``) treats SDK transport
    failures like any other connection failure, with no extra handling.

    ``kind`` is one of:

    - ``"network"`` — a transport-level failure (connect/read/TLS/WebSocket
      framing/a handshake that lies).
    - ``"http"`` — the relay ANSWERED with a refusing HTTP status (non-2xx
      ingest/SSE, non-101 upgrade; carries ``status``).
    - ``"parse"`` — a payload arrived but its JSON is undecodable — the frame
      is still dropped, and the drop is reported.
    """

    def __init__(self, kind: str, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.kind = kind
        #: The HTTP status code when ``kind == "http"``; ``None`` otherwise
        #: (including when the status line itself couldn't be parsed).
        self.status = status
