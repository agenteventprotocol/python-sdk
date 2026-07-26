"""Consume helpers: SSE subscription with attr-match filtering, id-dedupe, and
(session, epoch, seq) resume (AEP-0003 §5–7). Consumer conformance — dedupe on
(source, id), tolerate unknown types — is built in, not left to the caller."""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from typing import Any, Callable

from .errors import TransportError

# The resume set is BOUNDED on the wire: an unbounded known-session set would
# grow a single `from` query param past the server's HTTP header limit (Node
# caps the request line + headers at 16 KB), and every reconnect would then be
# refused before any relay code runs: a permanent, opaque failure. The bound
# keeps reconnects valid: newest positions ride the live request within this
# budget of encoded characters; older ones drain through bounded `live=0`
# replay requests whose overlap the (source, id) dedupe absorbs.
_FROM_BUDGET = 6000
_MAX_REPLAY_CHUNKS = 20


def _encoded_size(position: dict[str, Any]) -> int:
    return len(urllib.parse.quote(json.dumps(position, separators=(",", ":")))) + 3


def _split_from(positions: list[dict[str, Any]],
                cap: int | None,
                budget: int = _FROM_BUDGET) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Newest-within-budget tail of ``positions`` (at most ``cap``) + the older rest."""
    head: list[dict[str, Any]] = []
    size = 2
    i = len(positions) - 1
    while i >= 0 and (cap is None or len(head) < cap):
        entry_size = _encoded_size(positions[i])
        if head and size + entry_size > budget:
            break
        head.insert(0, positions[i])
        size += entry_size
        i -= 1
    return head, positions[: i + 1]


def _as_transport_error(e: Exception) -> TransportError:
    if isinstance(e, TransportError):
        return e
    if isinstance(e, urllib.error.HTTPError):  # subclasses URLError: check first
        return TransportError("http", f"SSE HTTP {e.code}", status=e.code)
    # catch-all network wrap, like aio's loop: mid-stream failures aren't all
    # OSErrors (http.client raises HTTPException on a truncated read)
    return TransportError("network", str(e))


class Subscription:
    def __init__(self) -> None:
        self._closed = threading.Event()
        self._positions: dict[str, dict[str, Any]] = {}
        self._thread: threading.Thread | None = None

    def close(self) -> None:
        self._closed.set()

    def positions(self) -> list[dict[str, Any]]:
        """Latest per-session positions observed — persist these to resume later."""
        return list(self._positions.values())

    def join(self, timeout: float | None = None) -> None:
        if self._thread:
            self._thread.join(timeout)


def subscribe(url: str,
              on_event: Callable[[dict[str, Any]], None],
              *,
              filter: dict[str, Any] | None = None,
              capture: str | None = None,
              token: str | None = None,
              from_: list[dict[str, Any]] | str | None = None,
              on_comment: Callable[[str], None] | None = None,
              on_error: Callable[[Exception], None] | None = None,
              backoff_initial_ms: int = 500,
              backoff_max_ms: int = 10_000,
              from_budget: int = _FROM_BUDGET,
              max_replay_chunks: int = _MAX_REPLAY_CHUNKS,
              timeout_ms: int = 30_000) -> Subscription:
    """Subscribe over SSE; runs in a daemon thread, reconnects with backoff.

    ``from_`` is a list of resume positions, or the literal string ``"all"``
    (replay-all, AEP-0003 §5): the FIRST request asks the endpoint for every
    session it holds, each from its earliest held position — no known-session
    enumeration; positions build from delivery and govern reconnects as
    usual. Gate on the endpoint advertising ``replay.all`` (or accept the
    deterministic refusal): a 4xx on that first request is reported via
    ``on_error`` and the subscription falls back to a live-only stream.

    The tuning knob defaults:
    ``backoff_initial_ms``/``backoff_max_ms`` set the reconnect schedule
    (exponential doubling between them), ``from_budget`` caps the encoded
    ``from`` parameter riding the live request, ``max_replay_chunks`` bounds
    the ``live=0`` drain connections per reconnect, and ``timeout_ms`` is the
    per-request ``urlopen`` timeout (urllib semantics: it covers the connect
    and each socket read, not the stream's total lifetime)."""
    sub = Subscription()
    pending_all = from_ == "all"
    if isinstance(from_, list):
        for p in from_:
            sub._positions[p["session"]] = p
    # Dedupe scope is (source, id) — AEP-0001 §7.4: two emitters may legally
    # mint the same id, so the key is the tuple, not the bare id.
    seen: OrderedDict[tuple[Any, str], None] = OrderedDict()

    def _stream_once(from_list: list[dict[str, Any]] | str, live: bool,
                     opened: Callable[[], None] | None = None) -> None:
        params = [("filter", json.dumps(filter or {}))]
        if capture:
            params.append(("capture", capture))
        if token:
            params.append(("access_token", token))
        if from_list == "all":
            params.append(("from", "all"))
        elif from_list:
            params.append(("from", json.dumps(from_list)))
        if not live:
            params.append(("live", "0"))
        full = f"{url.rstrip('/')}/stream?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(full, headers={"accept": "text/event-stream"})
        with urllib.request.urlopen(req, timeout=timeout_ms / 1000) as res:
            if opened:
                opened()
            data_lines: list[str] = []
            for raw in res:
                if sub._closed.is_set():
                    return
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line == "":
                    if data_lines:
                        _deliver("\n".join(data_lines))
                        data_lines = []
                    continue
                if line.startswith(":"):
                    if on_comment:
                        on_comment(line[1:].strip())
                    continue
                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip(" "))

    def loop() -> None:
        backoff = backoff_initial_ms / 1000
        from_cap: int | None = None  # shrinks when a resume-carrying request is refused

        def reset_backoff() -> None:
            nonlocal backoff
            backoff = backoff_initial_ms / 1000

        nonlocal pending_all
        while not sub._closed.is_set():
            sent_from = 0
            sent_all = False
            try:
                # replay-all (AEP-0003 §5) rides the first request only: once
                # events flow (or the request was refused), tracked positions
                # govern every reconnect.
                use_all = pending_all and not sub._positions
                sent_all = use_all
                if use_all:
                    head: list[dict[str, Any]] = []
                    rest: list[dict[str, Any]] = []
                else:
                    head, rest = _split_from(list(sub._positions.values()), from_cap,
                                             from_budget)
                sent_from = len(head)
                # Positions beyond the budget drain through bounded replay-only
                # requests before the live connect; a failed chunk costs
                # history, never the pipe (the refusing relay would refuse the
                # rest the same way).
                chunks = 0
                while rest and chunks < max_replay_chunks and not sub._closed.is_set():
                    chunk, rest = _split_from(rest, None, from_budget)
                    try:
                        _stream_once(chunk, live=False)
                    except Exception as e:  # noqa: BLE001 — reported, then dropped
                        if not sub._closed.is_set() and on_error:
                            on_error(_as_transport_error(e))
                        break
                    chunks += 1
                _stream_once("all" if use_all else head, live=True, opened=reset_backoff)
                pending_all = False
            except Exception as e:  # noqa: BLE001
                if not sub._closed.is_set():
                    err = _as_transport_error(e)
                    if on_error:
                        on_error(err)
                    # The endpoint refused `from=all` (deterministically, §5 —
                    # it does not implement the value): fall back to a
                    # live-only stream; the caller heard the refusal above.
                    if (err.kind == "http" and err.status is not None
                            and 400 <= err.status < 500 and sent_all):
                        pending_all = False
                        time.sleep(0.15)
                        continue
                    # Self-heal: a relay may cap `from` (e.g. AEP_MAX_FROM) and
                    # refuse a resume-carrying request with a 4xx. Resume is an
                    # optimization — shrink the set and retry promptly rather
                    # than looping dead on an identical refusal. Network errors
                    # keep the full set: the relay may be down, and resume
                    # must survive its return.
                    if (err.kind == "http" and err.status is not None
                            and 400 <= err.status < 500 and sent_from > 0):
                        from_cap = sent_from // 2
                        time.sleep(0.15)
                        continue
            if not sub._closed.is_set():
                time.sleep(backoff)
                backoff = min(backoff * 2, backoff_max_ms / 1000)

    def _deliver(text: str) -> None:
        try:
            ev = json.loads(text)
        except ValueError as e:
            if on_error:
                on_error(TransportError("parse", f"undecodable SSE payload: {e}"))
            return
        ev_id = ev.get("id")
        if not isinstance(ev_id, str):
            return
        # (source, id) — AEP-0001 §7.4
        dedupe_key = (ev.get("source"), ev_id)
        if dedupe_key in seen:
            return
        seen[dedupe_key] = None
        if len(seen) > 50_000:
            seen.popitem(last=False)
        if ev.get("session") is not None and ev.get("seq") is not None:
            sub._positions[ev["session"]] = {
                "session": ev["session"], "epoch": ev.get("epoch", 0), "seq": ev["seq"],
            }
        on_event(ev)

    t = threading.Thread(target=loop, name="aep-subscribe", daemon=True)
    sub._thread = t
    t.start()
    return sub
