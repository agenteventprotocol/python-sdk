// AEP shared primitives (AEP-0001/0003 semantics) used by the vendored relay
// fixture (code home: https://github.com/agenteventprotocol/reference).
// Plain Node, no dependencies. Not an SDK — not part of the package.
'use strict';

const SEVERITY = ['debug', 'info', 'notice', 'warning', 'error', 'critical'];
const CAPTURE = ['none', 'metadata', 'redacted', 'full'];

const CORE_ATTRS = new Set([
  'aep', 'id', 'type', 'subject', 'time', 'source', 'agent', 'session', 'run',
  'step', 'seq', 'epoch', 'cause', 'traceparent', 'severity', 'capture', 'data',
]);

// Keys attr-match may reference (AEP-0003 §6.2.1). Extension attrs also legal.
const FILTERABLE = new Set([
  'id', 'type', 'subject', 'source', 'agent', 'session', 'run', 'step', 'epoch',
  'cause', 'severity', 'capture',
]);
const UNFILTERABLE = new Set(['time', 'seq', 'aep', 'data']);

const sevRank = (s) => SEVERITY.indexOf(s ?? 'info');
const capRank = (c) => CAPTURE.indexOf(c ?? 'metadata');

/** Validate an attr-match filter object. Returns null if ok, else an error string.
 *  Fail closed: unknown/disallowed keys are errors (AEP-0003 §6.2.1). */
function validateFilter(filter) {
  if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) {
    return 'filter must be a JSON object';
  }
  for (const [key, pred] of Object.entries(filter)) {
    if (UNFILTERABLE.has(key)) return `attribute not filterable: ${key}`;
    const isExt = /^[a-z0-9]{1,20}$/.test(key) && !CORE_ATTRS.has(key);
    if (!FILTERABLE.has(key) && !isExt) return `unknown filter key: ${key}`;
    if (key === 'severity' && pred !== null && typeof pred === 'object' && !Array.isArray(pred)) {
      for (const k of Object.keys(pred)) {
        if (k !== 'gte' && k !== 'lte') return `severity comparison allows gte/lte only`;
        if (!SEVERITY.includes(pred[k])) return `unknown severity level: ${pred[k]}`;
      }
      continue;
    }
    const scalars = Array.isArray(pred) ? pred : [pred];
    if (scalars.length === 0) return `empty inclusion array on ${key}`;
    for (const v of scalars) {
      const t = typeof v;
      if (t !== 'string' && t !== 'number' && t !== 'boolean') {
        return `predicate values must be scalars (${key})`;
      }
      if (t === 'string' && v.includes('*') && key !== 'type') {
        return `glob patterns are only legal on "type" (${key})`;
      }
      if (key === 'type' && t === 'string' && v.includes('*') && !/^([a-z0-9_-]+\.)+\*$/.test(v)) {
        return `bad type pattern: ${v} (form: prefix.*)`;
      }
    }
  }
  return null;
}

/** attr-match evaluation (AEP-0003 §6). Assumes filter already validated. */
function attrMatch(filter, ev) {
  for (const [key, pred] of Object.entries(filter)) {
    let val = ev[key];
    if (key === 'severity') val = val ?? 'info';
    if (key === 'capture') val = val ?? 'metadata';
    if (key === 'epoch') val = val ?? 0;
    if (key === 'severity' && pred !== null && typeof pred === 'object' && !Array.isArray(pred)) {
      const r = sevRank(val);
      if (pred.gte !== undefined && r < sevRank(pred.gte)) return false;
      if (pred.lte !== undefined && r > sevRank(pred.lte)) return false;
      continue;
    }
    if (val === undefined) return false; // absent attribute never matches (§6.2.5)
    const alts = Array.isArray(pred) ? pred : [pred];
    let hit = false;
    for (const alt of alts) {
      if (key === 'type' && typeof alt === 'string' && alt.endsWith('.*')) {
        if (String(val).startsWith(alt.slice(0, -1))) { hit = true; break; }
      } else if (val === alt) { hit = true; break; }
    }
    if (!hit) return false;
  }
  return true;
}

/** Down-level an event to a capture ceiling (AEP-0001 §8.2 / AEP-0002 §5.1).
 *  annotations: { [type]: { [dottedFieldPath]: minLevel } } from the schema
 *  registry (nested paths use `.` and `[]`, e.g. `answer.text`,
 *  `options[].label`); without annotations for a type, strips `data`
 *  wholesale below `full` — the conservative (privacy-safe) direction.
 *  Never up-levels, and never mints the `redacted` provenance claim
 *  (§8.2e): this is a mechanical hop, so a `redacted` ceiling on a `full`
 *  event rewrites to `metadata` — redacted-gated fields at capture:full may
 *  be verbatim and dropping fields cannot establish the §9.4 pipeline claim.
 *  The rewritten event MUST honor its new level (§8.2a: `metadata` means no
 *  free text), so nested annotated fields are pruned and the AEP-0002 §5.3
 *  [metadata*] exception fields are degraded structurally below `redacted`
 *  (see degradeCaptureExceptions). */
function downlevel(ev, ceiling, annotations) {
  const evCap = ev.capture ?? 'metadata';
  if (capRank(ceiling) >= capRank(evCap)) return ev; // ceiling not lower: unchanged
  // AEP-0001 §8.2(e): `redacted` is a provenance claim (the text passed the
  // §9.4 pipeline) that a mechanical hop can never mint — a redacted ceiling
  // on a full event lands on metadata. Only a redacting re-emitter (which
  // runs the pipeline itself) may rewrite full -> redacted.
  const target = ceiling === 'redacted' && evCap === 'full' ? 'metadata' : ceiling;
  const out = { ...ev, capture: target };
  if (target === 'none' || !ev.data) { delete out.data; return out; }
  const ann = annotations && annotations[ev.type];
  if (!ann) { delete out.data; return out; }
  const rank = capRank(target);
  const prune = (val, path) => {
    if (Array.isArray(val)) return val.map((x) => prune(x, path + '[]'));
    if (val === null || typeof val !== 'object') return val;
    const kept = {};
    for (const [k, v] of Object.entries(val)) {
      const p = `${path}.${k}`;
      if (capRank(ann[p] ?? 'metadata') > rank) continue;
      kept[k] = prune(v, p);
    }
    return kept;
  };
  const keep = {};
  for (const [k, v] of Object.entries(ev.data)) {
    if (capRank(ann[k] ?? 'metadata') > rank) continue;
    keep[k] = prune(v, k);
  }
  out.data = degradeCaptureExceptions(ev, keep, rank);
  return out;
}

/** AEP-0002 §5.3 marks prompt / options[].label / field_spec.label /
 *  choices[].label / answer.values as [metadata*]: schema annotation
 *  `metadata`, real content allowed at redacted+. A hop rewriting capture
 *  below `redacted` must not keep that content, and cannot run the emitter's
 *  synthesis or per-field classification (no source context) — so it
 *  degrades structurally: prompt → the AEP-0003 §9.2 synthesis shape from
 *  envelope attributes, labels → their structural ids, values → number/
 *  boolean entries only (a string may be free text and an array's ids can't
 *  be verified without the request's field_spec; dropping more than required
 *  is always legal, §8.2b). KNOWN LOSSY CASE, accepted deliberately: a
 *  closed multi-select answer (values entry is an array of choices[].id,
 *  genuinely structural) is ALSO dropped here, unlike gateFormValues (the
 *  emitter, which has the field_spec and keeps it) — a hop has no field_spec
 *  to confirm every array entry is actually a choice id rather than
 *  free-text-shaped-as-an-array, so it conservatively drops the whole entry.
 *  A metadata-ceiling fan-out consumer therefore sees fewer structural
 *  answers than the emitter itself did; see conformance/fixtures/downlevel/
 *  case DL1 for the pinned example. */
function degradeCaptureExceptions(ev, data, rank) {
  if (rank >= capRank('redacted')) return data;
  if (ev.type === 'attention.requested') {
    const out = { ...data };
    if (typeof out.prompt === 'string') {
      out.prompt = `${ev.agent} requests ${out.kind ?? 'attention'}${ev.subject ? `: ${ev.subject}` : ''}`;
    }
    if (Array.isArray(out.options)) out.options = out.options.map((o) => ({ ...o, label: String(o.id) }));
    if (Array.isArray(out.fields)) {
      out.fields = out.fields.map((f) => ({
        ...f,
        label: String(f.id),
        ...(Array.isArray(f.choices)
          ? { choices: f.choices.map((c) => ({ ...c, label: String(c.id) })) } : {}),
      }));
    }
    return out;
  }
  if ((ev.type === 'attention.answered' || ev.type === 'control.attention.respond') &&
      data.answer && typeof data.answer === 'object' &&
      data.answer.values && typeof data.answer.values === 'object') {
    const values = {};
    for (const [k, v] of Object.entries(data.answer.values)) {
      if (typeof v === 'number' || typeof v === 'boolean') values[k] = v;
    }
    return { ...data, answer: { ...data.answer, values } };
  }
  return data;
}

/** Load { type: { dottedFieldPath: minLevel } } from a schemas/types
 *  directory — nested object properties as `a.b`, array items as `a[].b`,
 *  so downlevel can prune below-top-level annotations (answer.text,
 *  error.message, …). */
function loadCaptureAnnotations(typesDir) {
  const fs = require('fs');
  const path = require('path');
  const out = {};
  for (const f of fs.readdirSync(typesDir)) {
    if (!f.endsWith('.schema.json')) continue;
    const sch = JSON.parse(fs.readFileSync(path.join(typesDir, f), 'utf8'));
    const type = f.replace(/\.schema\.json$/, '');
    const ann = {};
    const walk = (node, base) => {
      for (const [prop, psch] of Object.entries(node.properties ?? {})) {
        const p = base ? `${base}.${prop}` : prop;
        ann[p] = psch['x-aep-capture'] ?? 'metadata';
        walk(psch, p);
        if (psch.items) walk(psch.items, p + '[]');
      }
    };
    walk(sch, '');
    out[type] = ann;
  }
  return out;
}

// ---- filename safety for wire-derived identifiers --------------------------
/** Map a wire-derived identifier (envelope `session`, a vendor conversation /
 *  task id) to a single safe path segment for a per-session file. Attacker
 *  input reaches a filename here, so a `../` or an absolute-looking name must
 *  never escape the capture directory. Legitimate identifiers — ULIDs, UUIDs,
 *  the vendored `s_k1`-style test names — match the safe grammar and pass
 *  through UNCHANGED (existing capture files keep their names, smokes keep
 *  their filenames). Anything else is percent-encoded: every byte outside
 *  `[A-Za-z0-9._-]` becomes `%XX` (uppercase hex), with `%` itself encoded
 *  first so the transform is unambiguous, and a leading `.` encoded so no
 *  result begins with a dot (blocks `.`, `..`, and dotfiles). The result is a
 *  deterministic, collision-free function of the input (distinct names never
 *  collide: the only characters that survive verbatim are the safe set, and
 *  `%` is reserved for the escape), and contains no `/`, `\`, or NUL. */
function sessionFilename(name) {
  const s = String(name);
  if (s !== '.' && s !== '..' && /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(s)) return s;
  const bytes = Buffer.from(s, 'utf8');
  let out = '';
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i];
    const isSafe =
      (b >= 0x30 && b <= 0x39) || // 0-9
      (b >= 0x41 && b <= 0x5a) || // A-Z
      (b >= 0x61 && b <= 0x7a) || // a-z
      b === 0x2e || b === 0x5f || b === 0x2d; // . _ -
    // encode '%' (0x25) too, so an encoded byte is never confused with a
    // literal percent in the source name; encode a leading '.' so no result
    // starts with a dot
    if (isSafe && b !== 0x25 && !(i === 0 && b === 0x2e)) out += String.fromCharCode(b);
    else out += '%' + b.toString(16).toUpperCase().padStart(2, '0');
  }
  return out;
}

// ---- ULID (Crockford base32; 48-bit ms time + 80-bit random) ---------------
const B32 = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
function ulid(now = Date.now()) {
  const { randomBytes } = require('crypto');
  let t = now, time = '';
  for (let i = 0; i < 10; i++) { time = B32[t % 32] + time; t = Math.floor(t / 32); }
  const rnd = randomBytes(16);
  let rand = '';
  for (let i = 0; i < 16; i++) rand += B32[rnd[i] % 32];
  return time + rand;
}

/** Canonical JSON (sorted keys, no whitespace) for digests and identity checks. */
function canonicalJson(v) {
  if (Array.isArray(v)) return '[' + v.map(canonicalJson).join(',') + ']';
  if (v !== null && typeof v === 'object') {
    return '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonicalJson(v[k])).join(',') + '}';
  }
  return JSON.stringify(v);
}

function sha256Digest16(v) {
  const { createHash } = require('crypto');
  return createHash('sha256').update(canonicalJson(v)).digest('hex').slice(0, 32);
}

/** Lightweight structural envelope check (relay-grade; full validation = `aep validate`). */
function envelopeError(ev) {
  if (ev === null || typeof ev !== 'object' || Array.isArray(ev)) return 'not an object';
  if (ev.aep !== '0.1') return `unsupported aep version: ${ev.aep}`;
  for (const k of ['id', 'type', 'time', 'source', 'agent']) {
    if (typeof ev[k] !== 'string' || !ev[k]) return `missing/invalid ${k}`;
  }
  const t = ev.type;
  if (!/^(x\.[a-z0-9_-]+(\.[a-z0-9_]+){2,}|[a-z0-9_]+(\.[a-z0-9_]+)+)$/.test(t)) return `bad type syntax: ${t}`;
  const agentScoped = t.startsWith('agent.');
  const isCommand = /^(control\.(attention\.respond|cancel|pause|resume)|x\.[a-z0-9_-]+\.control\..+)$/.test(t);
  if (agentScoped) {
    if (ev.session !== undefined || ev.seq !== undefined) return 'agent-scoped events omit session/seq';
  } else if (isCommand) {
    if (typeof ev.session !== 'string') return 'command events require session (target)';
    if (ev.seq !== undefined) return 'command events omit seq';
  } else {
    if (typeof ev.session !== 'string') return 'session required';
    if (!Number.isInteger(ev.seq) || ev.seq < 0) return 'seq required (integer >= 0)';
  }
  if (ev.severity !== undefined && !SEVERITY.includes(ev.severity)) return `bad severity`;
  if (ev.capture !== undefined && !CAPTURE.includes(ev.capture)) return `bad capture`;
  return null;
}

const isCommandType = (t) =>
  /^(control\.(attention\.respond|cancel|pause|resume)|x\.[a-z0-9_-]+\.control\..+)$/.test(t);

// ---- kind:"form" structured input (AEP-0006 / AEP-0002 §5.3) ----------------

const choiceIds = (field) => new Set((field.choices ?? []).map((c) => c.id));

/** The MUST-reject rule for a control.attention.respond answering a form
 *  (AEP-0002 §5.3 attention.answered): values must cover every required
 *  field_spec, and a select answer must be a choices[].id unless the field
 *  opts into free text with other:true. Returns null if ok, else an error
 *  string (safe for a capture-gated control.rejected detail). */
function formValuesError(fields, values) {
  if (values === null || typeof values !== 'object' || Array.isArray(values)) {
    return 'answer.values must be an object for a kind:"form" request';
  }
  for (const f of fields ?? []) {
    if (f.required && !(f.id in values)) return `missing required field: ${f.id}`;
    if (!(f.id in values) || f.type !== 'select') continue;
    const ids = choiceIds(f);
    const v = values[f.id];
    const entries = Array.isArray(v) ? v : [v];
    for (const e of entries) {
      if (ids.has(e)) continue;
      if (typeof e === 'string' && f.other) continue; // sanctioned free-text escape
      return `not a choices[].id for select field ${f.id} (other:${f.other === true})`;
    }
  }
  return null;
}

/** Type-dependent capture gating for attention.answered.answer.values
 *  (AEP-0002 §5.3): number/boolean/closed-select values are structural and
 *  survive at metadata; string-typed values and `other` free text follow
 *  answer.text's rule (redacted minimum, through the §9.4 pipeline via
 *  `redact`). Keys without a matching field_spec are dropped (conservative). */
function gateFormValues(fields, values, ceiling, redact = (s) => s) {
  const contentOk = capRank(ceiling) >= capRank('redacted');
  const byId = new Map((fields ?? []).map((f) => [f.id, f]));
  const out = {};
  for (const [id, v] of Object.entries(values ?? {})) {
    const f = byId.get(id);
    if (!f) continue;
    if (f.type === 'number' || f.type === 'boolean') { out[id] = v; continue; }
    if (f.type === 'select') {
      const ids = choiceIds(f);
      const entries = Array.isArray(v) ? v : [v];
      if (entries.every((e) => ids.has(e))) { out[id] = v; continue; } // closed choice: structural
      if (contentOk) out[id] = typeof v === 'string' ? redact(v) : v; // other free text
      continue;
    }
    if (contentOk) out[id] = typeof v === 'string' ? redact(v) : v; // string field: free text
  }
  return out;
}

module.exports = {
  SEVERITY, CAPTURE, CORE_ATTRS, sevRank, capRank,
  validateFilter, attrMatch, downlevel, loadCaptureAnnotations,
  ulid, canonicalJson, sha256Digest16, envelopeError, isCommandType,
  formValuesError, gateFormValues, sessionFilename,
};
