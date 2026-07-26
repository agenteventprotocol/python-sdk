"""State projection: fold AEP Events into current per-session state.

The fold mirrors the reference CLI's read-side disciplines (the `aep`
command-line tool in the reference stack): dedupe on ``(source, id)`` per
AEP-0001 §7.4 with a byte-identical redelivery collapsing silently and a
same-key/different-payload event reported as an ``id-collision``; sessions
keyed ``(source, session)`` per AEP-0001 §5.2 (session identity is
emitter-scoped — two sources sharing a session string are two sessions);
``(epoch, seq)`` ordering per AEP-0001 §7 with regressions reported, never
repaired; run terminal exclusivity per AEP-0002 §2 convention 5; pending
attention per AEP-0002 §7.2, where ``attention.resolved`` or
``attention.timeout`` clears a request and ``attention.answered``
deliberately does not (resolution is the emitter's ``attention.resolved``).

Command frames (AEP-0004) and ``agent.*`` events are deduplicated but never
folded into session state: a command addresses the TARGET's session under
the SENDER's ``source`` and carries no ``seq``, so folding it would
manufacture a phantom session; ``agent.*`` events are session-less liveness.
``control.accepted``/``control.rejected`` are ordinary seq-bearing session
events — they advance the session's position and change no state.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable

# The command-frame class (AEP-0004 §2.2): the four core control verbs plus
# extension commands under x.<vendor>.control.*. Mirrors the reference
# implementation's shared isCommandType predicate.
_COMMAND_TYPE = re.compile(
    r"^(control\.(attention\.respond|cancel|pause|resume)|x\.[a-z0-9_-]+\.control\..+)$")

# run.* terminal verb -> the run's terminal status (AEP-0002 §2 convention 5)
_RUN_TERMINALS = {
    "run.finished": "finished",
    "run.failed": "failed",
    "run.cancelled": "cancelled",
}


def _canonical(obj: Any) -> str:
    """Canonical sorted-keys JSON: byte-identity is what separates a legal
    at-least-once redelivery from a ``(source, id)`` collision."""
    return json.dumps(obj, sort_keys=True)


class StateProjection:
    """Incremental fold: ``apply()`` events in arrival order, read ``state()``.

    Arrival order is folded as-is — an out-of-order arrival is reported as an
    ``epoch-regression``/``seq-regression`` violation, not repaired. For a
    batch that should be repaired by sorting first, use :func:`project_state`
    (the same fold, pre-sorted per session).
    """

    def __init__(self) -> None:
        # (source, id) -> canonical JSON as first seen (AEP-0001 §7.4);
        # projection-global and consulted at apply time.
        self._seen: dict[tuple[Any, str], str] = {}
        # (source, session) -> mutable session record (AEP-0001 §5.2)
        self._sessions: dict[tuple[Any, Any], dict[str, Any]] = {}
        self._violations: list[dict[str, Any]] = []

    def apply(self, ev: dict[str, Any]) -> None:
        """Fold one Event into the projection (dedupe first, then state)."""
        ev_id = ev.get("id")
        if not isinstance(ev_id, str):
            return  # not a well-formed Event; nothing to key dedupe on
        source = ev.get("source")
        canonical = _canonical(ev)
        prior = self._seen.get((source, ev_id))
        if prior is not None:
            if prior == canonical:
                return  # at-least-once redelivery: silent collapse (§7.4)
            # same (source, id), different payload: the first occurrence
            # stays folded, the collider is dropped
            self._violations.append(
                {"rule": "id-collision", "source": source, "id": ev_id})
            return
        self._seen[(source, ev_id)] = canonical

        type_ = ev.get("type")
        session = ev.get("session")
        if (not isinstance(type_, str) or session is None
                or _COMMAND_TYPE.match(type_) or type_.startswith("agent.")):
            return  # deduped, never folded: no state, no position

        rec = self._sessions.get((source, session))
        if rec is None:
            rec = {
                # descriptive, not policed: the first folded event names it
                "agent": ev.get("agent"),
                "started": None,
                "ended": None,
                "position": None,
                "runs": [],       # fold order; entries shared with run_index
                "run_index": {},  # run id -> its entry in runs
                "pending": [],    # fold order (AEP-0002 §7.2)
            }
            self._sessions[(source, session)] = rec

        seq = ev.get("seq")
        if isinstance(seq, int):
            epoch = ev.get("epoch") or 0
            pos = rec["position"]
            if pos is not None:
                # AEP-0001 §7: report the regression, then fold anyway — the
                # position mirrors the last folded event, as the reference
                # CLI's resume positions do.
                if epoch < pos["epoch"]:
                    self._violations.append(
                        {"rule": "epoch-regression", "source": source,
                         "session": session, "id": ev_id})
                elif epoch == pos["epoch"] and seq <= pos["seq"]:
                    self._violations.append(
                        {"rule": "seq-regression", "source": source,
                         "session": session, "id": ev_id})
            rec["position"] = {"epoch": epoch, "seq": seq}

        time = ev.get("time")
        if type_ == "session.started":
            if rec["started"] is None:  # first wins post-dedupe
                rec["started"] = time
        elif type_ == "session.ended":
            if rec["ended"] is None:
                rec["ended"] = time
                if rec["pending"]:  # AEP-0002 §7.2: ended must not strand
                    self._violations.append(
                        {"rule": "ended-with-pending-attention",
                         "source": source, "session": session,
                         "pending": len(rec["pending"])})
            else:
                # second DISTINCT ended (a byte-identical repeat already
                # collapsed at dedupe); the first terminal stays
                self._violations.append(
                    {"rule": "session-terminal-exclusivity",
                     "source": source, "session": session})
        elif type_ == "run.started":
            run = ev.get("run")
            if isinstance(run, str) and run not in rec["run_index"]:
                entry = {"run": run, "status": "running",
                         "started": time, "ended": None}
                rec["run_index"][run] = entry
                rec["runs"].append(entry)
            # a second DISTINCT run.started for a known run: first wins,
            # no violation (pinned edge; no reference rule exists)
        elif type_ in _RUN_TERMINALS:
            run = ev.get("run")
            if isinstance(run, str):
                entry = rec["run_index"].get(run)
                if entry is None:
                    # terminal with no prior run.started: the run is still
                    # recorded (started unknown), no violation invented
                    entry = {"run": run, "status": _RUN_TERMINALS[type_],
                             "started": None, "ended": time}
                    rec["run_index"][run] = entry
                    rec["runs"].append(entry)
                elif entry["status"] == "running":
                    entry["status"] = _RUN_TERMINALS[type_]
                    entry["ended"] = time
                else:
                    # AEP-0002 §2 convention 5: terminals are exclusive;
                    # the first terminal's status stays
                    self._violations.append(
                        {"rule": "run-terminal-exclusivity", "source": source,
                         "session": session, "run": run})
        elif type_ == "attention.requested":
            data = ev.get("data") or {}
            rec["pending"].append(
                {"id": ev_id, "kind": data.get("kind"), "since": time})
        elif type_ in ("attention.resolved", "attention.timeout"):
            # clears the pending request; a subject that is not pending is a
            # silent no-op. attention.answered deliberately does NOT clear —
            # resolution is the emitter's attention.resolved (AEP-0002 §7.2).
            subject = ev.get("subject")
            rec["pending"] = [p for p in rec["pending"] if p["id"] != subject]

    def state(self) -> dict[str, Any]:
        """Snapshot of the projected state (plain dicts, JSON-serializable):
        ``sessions`` sorted by ``(source, session)``, ``violations`` in fold
        order. Unseen timestamps/positions are ``None`` (JSON ``null``)."""
        sessions: list[dict[str, Any]] = []
        for source, session in sorted(
                self._sessions, key=lambda k: (k[0] or "", k[1] or "")):
            rec = self._sessions[(source, session)]
            position = rec["position"]
            sessions.append({
                "source": source,
                "session": session,
                "agent": rec["agent"],
                "started": rec["started"],
                "ended": rec["ended"],
                "position": dict(position) if position is not None else None,
                "runs": [dict(r) for r in rec["runs"]],
                "pending": [dict(p) for p in rec["pending"]],
            })
        return {"sessions": sessions,
                "violations": [dict(v) for v in self._violations]}


def project_state(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Batch projection: group by ``(source, session)``, sort each group by
    ``(epoch, seq)`` stably (absent values sort as 0), then fold the groups in
    sorted key order through one :class:`StateProjection`. The pre-sort
    repairs arrival order, so only violations that survive sorting — two
    distinct events at the same ``(epoch, seq)`` position — are reported."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for ev in events:
        key = (str(ev.get("source") or ""), str(ev.get("session") or ""))
        groups.setdefault(key, []).append(ev)
    projection = StateProjection()
    for key in sorted(groups):
        batch = sorted(groups[key],
                       key=lambda e: (e.get("epoch") or 0, e.get("seq") or 0))
        for ev in batch:
            projection.apply(ev)
    return projection.state()
