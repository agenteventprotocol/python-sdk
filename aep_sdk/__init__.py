"""agenteventprotocol — AEP 0.1 helpers over the generated envelope + payload types.

Types are generated from the protocol's schema registry (AEP-0002) into
``aep_sdk.gen``; everything handwritten lives in the emit/consume/control
modules.
"""
from .gen.aep_types import *  # noqa: F401,F403 — the generated type surface
from .gen import aep_types as types  # noqa: F401
from .emit import Emitter, SessionEmitter, http_sink, jsonl_sink, ulid  # noqa: F401
from .consume import Subscription, subscribe  # noqa: F401
from .control import (NACK_REASONS, ControlClient, ControlSender,  # noqa: F401
                      ControlTarget, Duplex, NackError, open_duplex)
from .errors import TransportError  # noqa: F401
from .projection import StateProjection, project_state  # noqa: F401
from . import aio  # noqa: F401 — asyncio consumer (aio.subscribe / aio.AsyncSubscription)
from . import testing  # noqa: F401 — exported test doubles (testing.MemorySink / ScriptedSource / ControlStub)

__version__ = "0.1.0.dev0"
