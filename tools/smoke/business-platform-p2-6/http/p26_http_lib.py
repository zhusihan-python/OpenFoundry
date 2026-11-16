#!/usr/bin/env python3
"""P2.6 W7 shared harness primitives (pure Python stdlib, no sockets).

Owned by W7 (tools/smoke/business-platform-p2-6/http). This module only
provides MECHANICS: append-only evidence, secret redaction, bounded
waits, a release-token gate for the (later) live serve phases, and an
injectable HTTP client wrapper. Nothing here binds a port, runs docker,
or performs I/O against any service.

Evidence discipline (mirrors the P25/W6 family):
  * every attempt step writes <name>.cmd (redacted, BEFORE execution),
    <name>.rc (real return code), <name>.raw (real stdout/stderr bytes),
    <name>.meta (structured facts) — append-only, originals never
    overwritten;
  * Authorization headers, JWTs, DSNs and raw publish bodies NEVER enter
    any evidence file or ledger (redact() is applied to every cmd/meta).
"""

import hashlib
import json
import os
import re
import subprocess  # mechanics only: selftest spawns pure python
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True

W7_HTTP_DIR = Path(__file__).resolve().parent
P26_ROOT = W7_HTTP_DIR.parent
RESULTS_W7 = P26_ROOT / "results" / "w7"
FIXTURE_DIR = P26_ROOT / "fixture"

DEFAULT_RELEASE_FILE = os.environ.get(
    "P26_RELEASE_FILE", "/private/tmp/openfoundry-p26-release.json")

# Release phases this track may eventually ask Root to release. Serve
# mains refuse to bind unless the release file lists their phase.
RELEASE_PHASES = {
    "fixture": "http-fixture-serve",      # bind p26 fixture server
    "proxy": "http-proxy-serve",          # bind p26 dataset fault proxy
    "live": "http-live-scenarios",        # run scenario functions live
}

MAX_BARRIER_MS = 180_000                   # hard bound for every barrier
# (r3 memo: owned in-flight replay tests may need ≥60s lease + slack;
# fixture-level hold knobs stay clamped tighter by the fixture itself)
MAX_HOLD_JOIN_S = 10.0


class ScenarioAssertionError(Exception):
    """A scenario assertion failed. Message must stay secret-free."""


# --------------------------------------------------------------------------
# small utilities
# --------------------------------------------------------------------------

def utcnow_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def utcstamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data or b"").hexdigest()


def key_hash(key: str) -> str:
    """Non-reversible, ledger-safe digest of an idempotency key."""
    return hashlib.sha256((key or "").encode("utf-8")).hexdigest()[:16]


_SECRET_PATTERNS = [
    (re.compile(r"(Authorization\s*:\s*)(\S+(?:\s+\S+)?)", re.I), r"\1<redacted>"),
    (re.compile(r"(Bearer\s+)([A-Za-z0-9._~+/=-]+)", re.I), r"\1<redacted>"),
    (re.compile(r"(postgres(?:ql)?://[^\s:]+:)([^\s@]+)@", re.I), r"\1<redacted>@"),
    (re.compile(r"(OPENFOUNDRY_[A-Z0-9_]*JWT[A-Z0-9_]*=)(\S+)"), r"\1<redacted>"),
    (re.compile(r"(X-P26-Admin-Token\s*:\s*)(\S+)", re.I), r"\1<redacted>"),
]

# Opaque positional values (vendor checkpoint tokens, pagination cursors,
# presign signatures) never enter evidence raw: redact() rewrites their
# query-serialized form to sha256-16 + length so continuity stays
# provable without disclosure. Expiry values are whitelisted (not here).
_OPAQUE_QUERY_PARAMS = ("since_token", "cursor", "sig", "signature",
                        "hmac", "x-amz-signature", "x-goog-signature")


def opaque_digest(value):
    """Ledger/evidence-safe digest of an opaque positional value."""
    return f"sha256-{sha256_hex(value.encode('utf-8'))[:16]}:len={len(value)}"


def _scrub_opaque_query(text):
    def repl(match):
        return f"{match.group(1)}{opaque_digest(match.group(2))}"
    for param in _OPAQUE_QUERY_PARAMS:
        text = re.sub(rf"([?&]{param}=)([^&\s]+)", repl, text)
    return text


def redact(text):
    """Redact credential material and opaque positional values from any
    string bound for evidence (cmd lines, error messages, metas)."""
    if text is None:
        return None
    out = str(text)
    for pat, repl in _SECRET_PATTERNS:
        out = pat.sub(repl, out)
    return _scrub_opaque_query(out)


# EXACT credential-ish key names (case-insensitive full match). R2 item 5:
# substring matching wrongly ate validated SAFE digests under keys like
# "last_two_tokens"; only these exact names are blanket-redacted, and
# every remaining string still flows through pattern redact() (headers,
# bearer material, opaque query values).
_SENSITIVE_EXACT_KEYS = frozenset({
    "authorization", "jwt", "token", "access_token", "refresh_token",
    "id_token", "password", "secret", "dsn", "credential", "credentials",
    "api_key", "admin_token", "x-p26-admin-token", "auth",
})


def redact_json(obj):
    """Deep redaction for structured meta before it is written. Exact
    sensitive keys are replaced; validated safe digests (sha256-…:len=N)
    under ordinary keys survive; all strings still pass pattern redact."""
    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            if str(k).strip().lower() in _SENSITIVE_EXACT_KEYS:
                clean[k] = "<redacted>"
            else:
                clean[k] = redact_json(v)
        return clean
    if isinstance(obj, (list, tuple)):
        return [redact_json(v) for v in obj]
    if isinstance(obj, str):
        return redact(obj)
    return obj


# Evidence-only projection; never changes protocol bytes used by assertions.
_EVIDENCE_OMIT = frozenset({"config", "source_config", "credentials",
    "credential", "secret_ciphertext", "authorization", "auth", "password",
    "publish_body", "raw_body", "serialized_body", "content_base64"})
_EVIDENCE_OPAQUE = frozenset({"completed_token", "terminal_token",
    "completed_checkpoint", "cp_base_value", "candidate", "checkpoint",
    "since_token", "next_cursor", "cursor", "lease_token", "lease_holder"})


def safe_http_evidence(value):
    """Safe complete public JSON apart from secret/config/opaque fields.

    Opaque values become digest+length. Private publish material, Source
    configuration and credentials are never copied into evidence. Known
    in-memory credentials are also removed from arbitrary error strings.
    """
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            low = str(key).lower()
            if low in _EVIDENCE_OMIT or low in _SENSITIVE_EXACT_KEYS:
                result[key] = "<redacted>"
            elif low in _EVIDENCE_OPAQUE and item is not None:
                result[key] = opaque_digest(str(item))
            else:
                result[key] = safe_http_evidence(item)
        return result
    if isinstance(value, (list, tuple)):
        return [safe_http_evidence(item) for item in value]
    if isinstance(value, str):
        for name in ("OPENFOUNDRY_SMOKE_JWT", "P26_HTTP_ADMIN_TOKEN",
                     "P26_W7_JWT_WRITER", "P26_W7_JWT_READONLY", "P26_W7_JWT_FOREIGN"):
            secret = os.environ.get(name)
            if secret:
                value = value.replace(secret, "<redacted>")
                if secret.startswith("Bearer "):
                    value = value.replace(secret[7:], "<redacted>")
        return redact(value)
    return value


# --- live origin binding (R2 item 3) ---------------------------------------
# The four live bases are bound to origins the ROOT RELEASE FILE
# explicitly approves ("w7_bases"); CLI flags must match exactly.
W7_BASE_KEYS = ("fixture", "proxy_admin", "dvs", "connector")
# Old substrate ports that can never appear in an approved origin.
FORBIDDEN_PORTS = frozenset({17090, 17091, 17092, 17093, 17094})


def validate_origin(url):
    """Returns (ok, reason). Structural rules for an approved origin:
    http(s) scheme, host[:port] only — no userinfo, query, fragment,
    path with segments, or trailing slash; no old-substrate port."""
    try:
        u = urllib.parse.urlparse(url)
    except (ValueError, AttributeError):
        return False, "unparseable url"
    if u.scheme not in ("http", "https"):
        return False, f"scheme {u.scheme!r} not http(s)"
    if not u.hostname:
        return False, "no host"
    if u.username or u.password or u.query or u.fragment:
        return False, "userinfo/query/fragment forbidden in origin"
    if u.path not in ("", "/"):
        return False, f"path {u.path!r} forbidden in origin"
    if u.port is not None and u.port in FORBIDDEN_PORTS:
        return False, f"port {u.port} is old P25/q3 substrate"
    return True, "ok"


def resolve_bases(flag_bases, release_doc):
    """Bind the four live bases to Root-approved origins.

    release_doc["w7_bases"] must carry all four approved origins
    (W6 deployment facts). CLI flags are optional but, when present,
    must EQUAL the approved origin exactly — a flag can never widen the
    credential destination. Returns (bases, error)."""
    approved = (release_doc or {}).get("w7_bases")
    if not isinstance(approved, dict):
        return None, "release file lacks w7_bases (Root-approved origins)"
    for key in W7_BASE_KEYS:
        if key not in approved:
            return None, f"w7_bases missing {key!r}"
    bases = {}
    for key in W7_BASE_KEYS:
        origin = approved[key]
        ok, why = validate_origin(origin)
        if not ok:
            return None, f"w7_bases[{key}]={origin!r}: {why}"
        bases[key] = str(origin).rstrip("/")
    # optional FIFTH origin: the Dataset FILES base for the presigned
    # download two-step (q3 DATASET_FILES_BASE_URL, e.g.
    # http://127.0.0.1:17078). Defaults to the dvs origin.
    files_origin = approved.get("dvs_files")
    if files_origin is not None:
        ok, why = validate_origin(files_origin)
        if not ok:
            return None, f"w7_bases[dvs_files]={files_origin!r}: {why}"
        bases["dvs_files"] = str(files_origin).rstrip("/")
    # optional SIXTH origin: the SECOND owned aux connector (instance
    # B) — r4: never silently dropped; when approved it must be a
    # DISTINCT origin from instance A (two-instance scenarios gate on
    # this; absent means B-requiring scenarios refuse, not fall back).
    b_origin = approved.get("connector_b")
    if b_origin is not None:
        ok, why = validate_origin(b_origin)
        if not ok:
            return None, f"w7_bases[connector_b]={b_origin!r}: {why}"
        b = str(b_origin).rstrip("/")
        if b == bases["connector"]:
            return None, ("w7_bases[connector_b] must DIFFER from the "
                          "connector (A) origin")
        bases["connector_b"] = b
    for key, flag in flag_bases.items():
        if flag is None:
            continue
        if str(flag).rstrip("/") != bases[key]:
            return None, (f"--{key}-base {flag!r} does not equal the "
                          f"Root-approved origin {bases[key]!r}")
    return bases, None


# --- presigned download two-step (first-live protocol, R2 appendix) -------
LOCAL_FS_PREFIX = "/v1/_internal/local-fs/"
EXPIRY_PARAM_WHITELIST = ("expires", "x-amz-expires")


def validated_download_location(location, approved_origin):
    """Validate a DownloadFile 302 Location against the approved Dataset
    FILES origin: exact scheme/host/port equality, no userinfo, path
    under /v1/_internal/local-fs/. Returns (safe_facts, error); the
    presign query signature is NEVER returned — only digest/length,
    path, and whitelisted expiry."""
    try:
        u = urllib.parse.urlparse(location)
        a = urllib.parse.urlparse(approved_origin)
    except (ValueError, AttributeError, TypeError):
        return None, "unparseable location/origin"
    if u.scheme not in ("http", "https"):
        return None, "location scheme not http(s)"
    if u.username or u.password:
        return None, "userinfo forbidden in location"
    if u.hostname != a.hostname or u.port != a.port \
            or u.scheme != a.scheme:
        return None, (f"location origin {u.scheme}://{u.netloc!r} != "
                      f"approved files origin")
    if not u.path.startswith(LOCAL_FS_PREFIX):
        return None, f"location path not under {LOCAL_FS_PREFIX}"
    if u.fragment:
        return None, "fragment forbidden in location"
    query = urllib.parse.parse_qs(u.query, keep_blank_values=True)
    expiry = None
    for key in EXPIRY_PARAM_WHITELIST:
        if key in query and query[key]:
            expiry = query[key][0]
            break
    return {"origin": f"{u.scheme}://{u.netloc}", "path": u.path,
            "query_sha256": sha256_hex(u.query.encode())[:16],
            "query_len": len(u.query), "expiry": expiry}, None


# --------------------------------------------------------------------------
# release gate (serve refusal without Root release)
# --------------------------------------------------------------------------

def read_release(path=DEFAULT_RELEASE_FILE):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict):
        return None
    return doc


def release_allows(phase, path=None):
    if path is None:
        path = DEFAULT_RELEASE_FILE
    doc = read_release(path)
    if doc is None:
        return False, "no release file"
    phases = doc.get("released_for")
    if not isinstance(phases, list) or phase not in phases:
        return False, f"phase {phase!r} not in released_for"
    exp = str(doc.get("expires_utc", ""))
    if not re.fullmatch(r"\d{8}T\d{6}Z", exp):
        return False, "bad expires_utc"
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if now > exp:
        return False, "release expired"
    return True, "released"


def refuse_serve(component, phase, argv):
    """Print a refusal (never bind) and return the refusal exit code."""
    ok, why = release_allows(phase)
    print(f"[{component}] REFUSING to bind: {why}", file=sys.stderr)
    print(f"[{component}] argv would be: {redact(' '.join(argv))}",
          file=sys.stderr)
    return 3


# --------------------------------------------------------------------------
# bounded waits / barriers
# --------------------------------------------------------------------------

class BoundedWait:
    """Poll predicate with a hard deadline; never sleeps past `timeout_s`."""

    def __init__(self, timeout_s, interval_s=0.2, clock=time.monotonic):
        self.timeout_s = float(timeout_s)
        self.interval_s = float(interval_s)
        self.clock = clock

    def until(self, predicate):
        deadline = self.clock() + self.timeout_s
        last = None
        while True:
            last = predicate()
            if last:
                return True, last
            if self.clock() >= deadline:
                return False, last
            time.sleep(min(self.interval_s,
                           max(0.0, deadline - self.clock())))


class BoundedEvent:
    """Event + deadline pair. wait() ALWAYS returns within the bound;
    release() is idempotent and safe from any thread. Used by fixture /
    proxy barriers so a crashed assertion path can never wedge a server
    thread forever."""

    def __init__(self, max_ms, clock=time.monotonic):
        max_ms = min(int(max_ms), MAX_BARRIER_MS)
        self.max_ms = max(0, max_ms)
        self.event = threading.Event()
        self.created_mono = clock()
        self.deadline_mono = self.created_mono + self.max_ms / 1000.0
        self.released_by = None
        self._clock = clock

    def release(self, by="manual"):
        self.released_by = by
        self.event.set()

    def wait(self):
        """Returns (released: bool, waited_ms: int)."""
        remaining = self.deadline_mono - self._clock()
        signaled = self.event.wait(max(0.0, remaining))
        waited = int((self._clock() - self.created_mono) * 1000)
        if not signaled:
            self.released_by = "timeout"
        return bool(signaled), waited

    @property
    def expired(self):
        return self._clock() >= self.deadline_mono

    def state(self):
        return {
            "max_ms": self.max_ms,
            "released": self.event.is_set(),
            "released_by": self.released_by,
            "expired": self.expired,
        }


# --------------------------------------------------------------------------
# injectable HTTP client (live transport; tests inject fakes)
# --------------------------------------------------------------------------

class HTTPResponse:
    __slots__ = ("status", "body", "err", "duration_ms", "headers")

    def __init__(self, status=None, body=b"", err=None, duration_ms=None,
                 headers=None):
        self.status = status
        self.body = body or b""
        self.err = err
        self.duration_ms = duration_ms
        self.headers = headers or {}

    def json(self):
        try:
            return json.loads(self.body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    @property
    def ok(self):
        return self.err is None and 200 <= (self.status or 0) < 300

    def describe(self):
        """Secret-free one-line description for ledgers/evidence."""
        if self.err is not None:
            return f"err:{type(self.err).__name__}"
        return f"status:{self.status} bytes:{len(self.body)}"


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """3xx is returned as-is (never followed): Authorization must never
    travel to a redirect target (auth-not-forwarded-to-arbitrary-URL)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)


class UrllibTransport:
    """Real live transport. Authorization passes through to the target
    ONLY (never logged, never followed across redirects). Instantiated
    solely by the live scenario path."""

    def request(self, method, url, headers=None, body=None, timeout_s=30.0):
        started = time.monotonic()
        req = urllib.request.Request(
            url, data=body if body is not None else None, method=method)
        for k, v in (headers or {}).items():
            if v is not None:
                req.add_header(k, v)
        try:
            with _NO_REDIRECT_OPENER.open(req, timeout=timeout_s) as resp:
                return HTTPResponse(
                    status=resp.status, body=resp.read(),
                    duration_ms=int((time.monotonic() - started) * 1000),
                    headers=dict(resp.headers))
        except urllib.error.HTTPError as exc:
            return HTTPResponse(
                status=exc.code, body=exc.read(),
                duration_ms=int((time.monotonic() - started) * 1000),
                headers=dict(exc.headers or {}))
        except Exception as exc:            # transport failure (honest)
            return HTTPResponse(
                err=exc,
                duration_ms=int((time.monotonic() - started) * 1000))


class Client:
    """Thin scenario-facing client. `auth` is held in memory only and is
    attached to outgoing headers; it never reaches ledgers or evidence
    (describe()/evidence paths use HTTPResponse.describe())."""

    def __init__(self, transport, base, auth=None, timeout_s=30.0,
                 extra_headers=None):
        self.transport = transport
        self.base = base.rstrip("/")
        self.auth = auth
        self.timeout_s = timeout_s
        # fixed per-client headers (e.g. the local admin credential);
        # values live in memory only and are never part of evidence
        self.extra_headers = dict(extra_headers or {})

    def request(self, method, path, json_body=None, raw_body=None,
                extra_headers=None, timeout_s=None):
        headers = dict(self.extra_headers)
        headers.update(extra_headers or {})
        if self.auth:
            headers.setdefault("Authorization", self.auth)
        body = None
        if json_body is not None:
            body = json.dumps(json_body, sort_keys=True).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        elif raw_body is not None:
            body = raw_body
        url = self.base + path if path else self.base
        return self.transport.request(
            method, url, headers=headers, body=body,
            timeout_s=timeout_s or self.timeout_s)

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, **kw):
        return self.request("POST", path, **kw)


# --------------------------------------------------------------------------
# append-only evidence writer
# --------------------------------------------------------------------------

class EvidenceDir:
    """Append-only attempt directory under results/w7/.

    step() returns a StepRecorder; a second recorder for the same step
    name is refused (append-only, originals never overwritten)."""

    def __init__(self, root: Path, label: str):
        self.path = Path(root) / f"{label}-{utcstamp()}"
        self.path.mkdir(parents=True, exist_ok=False)
        self._used = set()

    def step(self, name):
        if name in self._used:
            raise RuntimeError(f"evidence step {name!r} already written "
                               f"(append-only)")
        self._used.add(name)
        return StepRecorder(self.path, name)


class StepRecorder:
    """One attempt step: cmd (redacted) first, then raw/rc/meta."""

    def __init__(self, directory: Path, name: str):
        self.dir = directory
        self.name = name
        (self.dir / f"{name}.cmd").write_text("", encoding="utf-8")

    def cmd(self, argv, env_keys=None, note=None):
        text = redact(" ".join(str(a) for a in argv))
        if env_keys:
            text += "\n# env keys (values never stored): " + \
                ",".join(sorted(env_keys))
        if note:
            text += "\n# " + redact(note)
        (self.dir / f"{self.name}.cmd").write_text(text + "\n",
                                                  encoding="utf-8")
        return self

    def finish(self, rc, raw=b"", meta=None):
        (self.dir / f"{self.name}.rc").write_text(f"{rc}\n", encoding="utf-8")
        (self.dir / f"{self.name}.raw").write_bytes(raw or b"")
        if meta is not None:
            (self.dir / f"{self.name}.meta").write_text(
                json.dumps(redact_json(meta), indent=1, sort_keys=True)
                + "\n", encoding="utf-8")

    def result(self, resp: HTTPResponse, meta=None, artifact_bytes=False):
        # .rc keeps the historical normalized harness convention; it is
        # NOT the original HTTP status, nor an OS child return code.
        rc = 0 if resp.ok else (resp.status or 1)
        details = dict(safe_http_evidence(meta or {}))
        details.update({"rc_kind": "normalized_harness",
                        "original_http_status": resp.status,
                        "original_child_rc": "unavailable",
                        "transport_exception": type(resp.err).__name__ if resp.err else None,
                        "response_bytes": len(resp.body),
                        "response_sha256": sha256_hex(resp.body),
                        "duration_ms": resp.duration_ms})
        doc = resp.json()
        raw = (resp.describe() + "\n").encode()
        if doc is not None:
            safe = safe_http_evidence(doc)
            raw += (json.dumps(safe, indent=1, sort_keys=True) + "\n").encode()
            details["safe_response"] = safe
        else:
            # No unstructured/binary response text, HTML or presign body
            # is copied. Explicit synthetic file downloads have a separate
            # artifact so their bytes/hash/rows can be recomputed.
            details["response_projection"] = "non-JSON omitted; size/hash retained"
        if resp.err is not None:
            raw += f"error={type(resp.err).__name__}\n".encode()
            rc = 1
        if artifact_bytes and resp.status == 200 and resp.err is None:
            artifact = self.dir / f"{self.name}.download.bin"
            artifact.write_bytes(resp.body)
            details["download_artifact"] = artifact.name
        self.finish(rc, raw, details)
        return resp

    def subprocess_run(self, argv, timeout_s=60.0, env=None):
        """Run a bounded local subprocess (pure python tooling only) and
        record the REAL rc + raw output. Refuses obviously dangerous
        shells; this path is only used for selftest mechanics."""
        self.cmd(argv)
        try:
            cp = subprocess.run(
                [str(a) for a in argv], capture_output=True, text=True,
                timeout=timeout_s, env=env)
            rc, out = cp.returncode, (cp.stdout or "") + (cp.stderr or "")
        except subprocess.TimeoutExpired as exc:
            rc, out = 124, f"TIMEOUT after {timeout_s}s: {exc}"
        self.finish(rc, out.encode("utf-8"))
        return rc, out
