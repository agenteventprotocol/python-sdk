#!/usr/bin/env bash
# agenteventprotocol smoke: pydantic env via uv; the vendored relay fixture provides the
# live emit-over-HTTP / consume-over-SSE path. Requires uv and Node >= 22
# (the fixture is the reference relay, which is a Node program).
set -euo pipefail
cd "$(dirname "$0")/.."

# Relay-free first: the exported testing utilities + state projection need no
# transport, so this smoke runs before (and without) the relay fixture.
uv run --no-project --with 'pydantic>=2' python tests/smoke_testing.py

RELAY_PID=""
cleanup() { [ -n "$RELAY_PID" ] && kill "$RELAY_PID" 2>/dev/null || true; }
trap cleanup EXIT

if [ ! -d tests/fixtures/relay/node_modules ]; then
  if [ -f tests/fixtures/relay/package-lock.json ]; then
    npm ci --prefix tests/fixtures/relay >/dev/null
  else
    npm install --prefix tests/fixtures/relay >/dev/null
  fi
fi

PORT="${AEP_SMOKE_PORT:-18788}"
# Control flows only on authenticated bindings (AEP-0004 §4.1): the vendored
# relay is watch-only without a token, so the smokes run fully authenticated.
TOKEN="${AEP_SMOKE_TOKEN:-sdk-smoke-token}"
AEP_PORT="$PORT" AEP_TOKEN="$TOKEN" node tests/fixtures/relay/server.cjs > /dev/null 2>&1 & RELAY_PID=$!
sleep 0.7

AEP_RELAY="http://127.0.0.1:$PORT" AEP_TOKEN="$TOKEN" uv run --no-project --with 'pydantic>=2' python tests/smoke.py
AEP_RELAY="http://127.0.0.1:$PORT" AEP_TOKEN="$TOKEN" uv run --no-project --with 'pydantic>=2' python tests/smoke_aio.py
