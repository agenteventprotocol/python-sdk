# Golden state-projection corpus

Hand-authored fixture for the exported state projection: `events.jsonl` is a
mixed capture (four sessions across three emitters, a command frame, an
agent-scoped liveness event, one byte-identical redelivery, one deliberate
(source, id) collision, one duplicated position); `expected.json` is the
projected state the SDK must produce from it in batch mode.

The same two files are vendored byte-identically in both the TypeScript and
the Python SDK (typescript-sdk and python-sdk in this organization), so both
implementations answer to one definition. Do not edit one copy without the
other; any change must keep the twins identical and re-derive
`expected.json` by hand from the rules.

The projection's fold rules mirror the reference CLI's folds
(github.com/agenteventprotocol/reference, `impl/cli/aep.js` — the validate,
timeline, and subscribe-with-resume disciplines: (source, id) dedupe per
AEP-0001 §7.4, emitter-scoped session identity per AEP-0001 §5.2,
(epoch, seq) ordering per AEP-0001 §7, run terminal exclusivity per AEP-0002
§2, pending attention per AEP-0002 §7.2). The corpus was checked against
that CLI at authoring time: `aep validate` reports exactly the corpus's
intended violations (plus the file-order seq finding its file discipline
adds for the redelivered line, which dedupe-before-sort collapses here), and
`aep timeline` agrees with the batch ordering.
