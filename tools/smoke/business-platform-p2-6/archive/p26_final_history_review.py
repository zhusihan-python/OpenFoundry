"""Independent final history review. Reads only explicit nonsecret pinned originals.

p.refs contains config/inventory/catalog {path,sha256}. Config extends the existing
history_epoch inventory config with schema, final_cutoff, required_known_families
(family -> nonempty member-path list) and permitted_issues (exact metadata list).
Config may explicitly list fixture_literal_members/authentic_rc_paths. Only a
dedicated history-test-fixture-literal cohort can retain intentional fake RC/meta
literals with kind fixture-literal/disposition test-fixture/Unix null. Known real
failure family members and actual outer RCs cannot use that exception.
Catalog contains schema, final_cutoff, config/inventory refs, root_certificate,
members, families, issues, rc_dispositions, no_process_events. Member catalog
rows contain classification, epoch and eligible_for_S2_formal:false. RC rows
contain raw_value (stripped literal), kind, disposition, original_unix_rc, and
optional event (a no_process_events key). Event rows contain path/json_pointer;
the original JSON object must explicitly record a no-process/null Unix refusal
(legacy original_child_rc/decision or native original_process_rc/status/error).
http-observation rows add http_status/transport_outcome (literal rc unavailable);
normalized-harness rows add http_status/transport_exception (literal harness rc). No historical tool executes.
retained_zero_literal_paths is an exact no-companion zero roster, with null Unix
and retained-no-service-green disposition. recorded_child_rc_paths has an exact
recorded_child_rc_sources map ({producer:{path,sha256},rule}); only inventoried
source snapshots plus native integer/command evidence support those child rows.
reported_literal_paths/rules preserve finite recorder reports without claiming
an actual process or response. Original metadata/command must agree and closed
limitations/counts remain explicit in both config and catalog.
This reader never creates a final inventory, certificate or catalog.
Finite retained_unattributed_originals binds every original literal and existing
companion SHA/size with an explicit reason. It never supplies Unix, HTTP status,
transport or service success; incomplete and contradictory observations stay
explicit. Recoverable consistent current facts cannot use a legacy exception.
"""
import datetime
import hashlib
import json
import re
import shlex

OLD_S = 'b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
REQUIRED_KNOWN_FAMILIES = frozenset({
    'oldS', 'focused-red', 'S2partialunit', 'P24first', 'P24cleanupRO',
    'P24retry1', 'h9failed', 'h10failed', 'quiesceISO',
    'receiptapprovalblocked', 'postABC-BCsnapshot-v3-failed', 'postABC-BCsnapshot-v4-failed',
})
PREFIX = 'tools/smoke/business-platform-p2-6/results/'


def need(ok, why):
    if not ok:
        raise ValueError('final failed history NOTREADY: ' + why)


def local(path):
    need(isinstance(path, str) and path.startswith(PREFIX)
         and '\\' not in path and all(x not in ('', '.', '..') for x in path.split('/')),
         'explicit local results path required')
    need(not any(x.lower().startswith('.env') or x.lower().endswith('.env')
                 or '.env.' in x.lower() or x.lower().endswith('.body')
                 for x in path.split('/')), 'private/body file read forbidden')
    return path


def pinned(r, entry, label):
    need(isinstance(entry, dict) and set(entry) == {'path', 'sha256'}, 'explicit path/SHA ref required')
    path = entry['path']
    need(isinstance(path, str), 'literal ref path required')
    path = local(path[len(label) + 1:] if path.startswith(label + '/') else path)
    need(isinstance(entry['sha256'], str) and re.fullmatch('[0-9a-f]{64}', entry['sha256']), 'malformed SHA')
    need(path.endswith('.json'), 'JSON input ref only; never private body/command')
    raw = r.read(path)
    need(hashlib.sha256(raw).hexdigest() == entry['sha256'], 'pinned original changed')
    return json.loads(raw)


def pointer(doc, ptr):
    need(isinstance(ptr, str) and (not ptr or ptr.startswith('/')), 'literal JSON pointer required')
    for part in ptr.split('/')[1:] if ptr else []:
        key = part.replace('~1', '/').replace('~0', '~')
        doc = doc[int(key)] if isinstance(doc, list) else doc[key]
    return doc


def no_process_event(observed):
    need(isinstance(observed, dict) and observed.get('process_started') is False,
         'explicit no-process original required')
    if 'original_process_rc' in observed or str(observed.get('status', '')).startswith('AUTOMATIC_APPROVAL_REJECTED'):
        need('original_process_rc' in observed and observed['original_process_rc'] is None
             and observed.get('status') == 'AUTOMATIC_APPROVAL_REJECTED_BEFORE_PROCESS_CREATION; not a business verdict'
             and type(observed.get('private_SELECT_count')) is int and observed['private_SELECT_count'] == 0
             and type(observed.get('POST_count')) is int and observed['POST_count'] == 0
             and observed.get('actual_launch_directory_exists') is False
             and isinstance(observed.get('raw_tool_error'), str)
             and 'CreateProcess' in observed['raw_tool_error'] and 'Rejected' in observed['raw_tool_error'],
             'native approval rejection must prove no process/SELECT/POST/launch')
    else:
        need('original_child_rc' in observed and observed['original_child_rc'] is None
             and observed.get('decision') == 'approval-refused', 'explicit no-process/null Unix refusal')


def command_doc(r, path, projections=None):
    if path in PRIVATE_COMMAND_PATHS:
        need(projections is not None and path in projections, 'private command requires approved projection, never original body')
        return 'GET ' + projections[path]['url_without_query_fragment']
    raw = r.read(local(path))
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        return raw.decode().strip() if path.endswith('.cmd') else None


REPORT_LIMITS = {
    'legacy-recorder-report': 'actual-process-unclaimed; no-Unix; no-service-green',
    'early-request-report': 'producer-version-unavailable; no-HTTP-status; no-transport; no-Unix; no-service-green',
}


def check_reported_literal(value, meta, cmd, rule):
    need(rule in REPORT_LIMITS and isinstance(meta, dict)
         and not any(k in meta for k in ('kind', 'original_rc', 'original_child_rc', 'http_status', 'original_http_status', 'status', 'transport_outcome', 'rc_kind', 'failure_kind')),
         'finite reported literal cannot override original child/HTTP/failed spawn evidence')
    if rule == 'legacy-recorder-report':
        need({'cmd', 'rc', 'secret_scrubbed_occurrences', 'utc'} <= set(meta)
             and re.fullmatch('-?[0-9]+', value) and type(meta['rc']) is int and value == str(meta['rc'])
             and isinstance(cmd, str) and bool(cmd) and cmd == meta['cmd']
             and type(meta['secret_scrubbed_occurrences']) is int and meta['secret_scrubbed_occurrences'] >= 0,
             'reported legacy integer/command must preserve exact original agreement')
    else:
        lines = cmd.splitlines() if isinstance(cmd, str) else []
        first = lines[0].split() if lines else []
        need(set(meta) == {'request'} and (meta['request'] is None or isinstance(meta['request'], dict))
             and (value == '0' or value.isdigit() and 100 <= int(value) <= 599)
             and len(first) == 2 and first[0] in {'GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'}
             and (first[1].startswith('/') or first[1].startswith(('http://', 'https://')))
             and len(lines) in (1, 2) and (len(lines) == 1 or lines[1].startswith(('# base=http://', '# base=https://'))),
             'reported request literal has no original response/transport/Unix verdict')


def recorded_process(value, meta, cmd, rule, source, path=None):
    rules = {
        'private-projection-child': ('cp = (runner or subprocess.run)(argv,', "'original_child_rc': cp.returncode", "kind='private-projection'"),
        'recorded-call-child': ('cp=self.real', 'cp.returncode', "'original_rc':cp.returncode"),
        'delegated-call-child': ('cp=self.real(new,*args,**kwargs)', "{'original':old,'replacement':new}", "att.step_finish(kind,receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr})"),
        'h-command-child': ('try: cp=runner(cmd)', "att.step_finish('command',path,cp.stdout,cp.returncode,{'stderr':cp.stderr})"),
        'ownedops-child': ('cp = subprocess.run(argv, capture_output=True, text=True,', 'rc, out, err = cp.returncode, cp.stdout, cp.stderr', '{"original_rc": rc, "failure": failure,'),
    }
    need(rule in rules and all(token in source for token in rules[rule]), 'recorded process closed actual producer mechanism')
    fixed_sources = {'h-command-child': '17ab1e438ba318e51b224702846a447cbda31804d7f81c2af381098ed29b8f7f',
                     'ownedops-child': '621b10a86662fbc0c70c8821c9b87ff518d38a16a978fab87aa279e11b4feeeb'}
    if rule in fixed_sources:
        need(hashlib.sha256(source.encode()).hexdigest() == fixed_sources[rule], 'exact supported actual producer source version')
    def argv(v):
        return isinstance(v, list) and bool(v) and all(isinstance(x, str) and bool(x) for x in v)
    need(isinstance(meta, dict) and re.fullmatch('-?[0-9]+', value)
         and not any(k in meta for k in ('status', 'http_status', 'transport_outcome', 'rc_kind')), 'recorded process cannot absorb HTTP/noninteger literal')
    need(all(type(meta[k]) is int and str(meta[k]) == value for k in ('original_child_rc', 'original_rc') if k in meta),
         'all recorded original process rc fields must agree')
    if rule == 'private-projection-child':
        need(argv(cmd) and meta.get('kind') == 'private-projection'
             and type(meta.get('original_child_rc')) is int and value == str(meta['original_child_rc'])
             and all(type(meta.get(k)) is int and meta[k] >= 0 for k in ('stdout_bytes', 'stderr_bytes'))
             and all(isinstance(meta.get(k), str) and re.fullmatch('[a-f0-9]{64}', meta[k]) for k in ('stdout_sha256', 'stderr_sha256')),
             'recorded private projection integer/hash/argv binding')
    elif rule == 'recorded-call-child':
        need(argv(cmd) and meta.get('kind') == 'call' and type(meta.get('original_rc')) is int
             and value == str(meta['original_rc']) and 'stderr' in meta, 'recorded real-runner integer/argv binding')
    elif rule == 'delegated-call-child':
        need(isinstance(cmd, dict) and set(cmd) == {'original', 'replacement'}
             and argv(cmd['original']) and argv(cmd['replacement'])
             and meta.get('kind') == 'call' and set(meta) == {'kind', 'scrub_hits', 'stderr', 'utc'}, 'delegated real replacement command/native metadata')
    elif rule == 'h-command-child':
        need(isinstance(path, str) and re.search(r'/[0-9]+-command\.rc$', path)
             and argv(cmd) and meta.get('kind') == 'call'
             and set(meta) == {'kind', 'scrub_hits', 'stderr', 'utc'}, 'exact actual H command/native metadata; HTTP and fixture excluded')
    else:
        try:
            parsed = shlex.split(cmd) if isinstance(cmd, str) else None
        except ValueError:
            parsed = None
        need(argv(parsed) and set(meta) == {'duration_ms', 'failure', 'method', 'original_rc', 'timeout_s'}
             and meta['failure'] is None and type(meta['original_rc']) is int and value == str(meta['original_rc'])
             and isinstance(meta['method'], str) and bool(meta['method'])
             and type(meta['duration_ms']) is int and meta['duration_ms'] >= 0
             and type(meta['timeout_s']) in (int, float) and 0 < meta['timeout_s'] <= 30
             and parsed[:2] == ['docker', {'inspect-identity': 'inspect', 'kill': 'kill', 'start': 'start', 'lease-observe': 'exec', 'pregate-observe': 'exec'}.get(meta['method'])],
             'actual ownedops original integer and exact shellargv metadata')


UNATTRIBUTED_LIMITS = 'origin-unattributed; no-Unix; no-HTTP-status; no-transport; no-service-green; formaleligiblefalse'
# Historical attribution can lack a version, an original path pairing, or an
# unambiguous recorder convention; this does not assert the source never existed.
UNATTRIBUTED_REASONS = {'historical-process-attribution-insufficient', 'local-derived-no-process-attestation', 'pure-simulation-process-unclaimed', 'explicit-incomplete-observation', 'contradictory-metadata-retained'}


def unattributed_companion_paths(path):
    stem = path[:-3] if path.endswith('.rc') else None
    return ((stem + '.meta.json', stem + '.meta', stem + '.cmd.json', stem + '.cmd') if stem else
            tuple(path.rsplit('/', 1)[0] + '/' + v for v in ('meta.json', 'meta', 'cmd.json', 'cmd')))


def current_unattributed_claim(value, meta, cmd):
    """Mirror only recoverable current native domains; historical reports stay reports."""
    if not isinstance(meta, dict):
        return None
    status_ok = lambda x: type(x) is int and 100 <= x <= 599
    url = cmd.get('url', cmd.get('path')) if isinstance(cmd, dict) else None
    if isinstance(url, dict) and set(url) == {'url', 'query_digest'}:
        if url['query_digest'] is None or isinstance(url['query_digest'], str) and re.fullmatch('[a-f0-9]{64}', url['query_digest']):
            url = url['url']
    is_http = isinstance(cmd, dict) and cmd.get('method') in {'GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'} and isinstance(url, str) and bool(url)
    if meta.get('kind') == 'call' and is_http and 'transport_outcome' not in meta:
        status = meta.get('http_status', meta.get('status'))
        sentinel = set(meta) == {'kind', 'scrub_hits', 'status', 'utc'} and value == '0'
        nullable = 'original_rc' in meta and meta['original_rc'] is None and value == 'unavailable'
        if status_ok(status) and (sentinel or nullable) and 'original_child_rc' not in meta and ('status' not in meta or meta['status'] == status):
            return ('http-status-observation', status)
        return None
    if meta.get('rc_kind') == 'normalized_harness':
        status, error = meta.get('original_http_status'), meta.get('transport_exception')
        if (meta.get('original_child_rc') == 'unavailable' and 'original_http_status' in meta and 'transport_exception' in meta
                and (error is None or isinstance(error, str) and bool(error))
                and (status_ok(status) or status is None and error is not None)):
            expected = 0 if error is None and status is not None and 200 <= status < 300 else (status or 1)
            if value == str(expected):
                return ('normalized-harness', status, error)
        return None
    if 'transport_outcome' in meta:
        outcome, status = meta['transport_outcome'], meta.get('http_status', meta.get('status'))
        if (type(outcome) is int and outcome in (0, 1) and 'original_child_rc' in meta and meta['original_child_rc'] is None
                and (status_ok(status) or outcome == 1 and status is None)
                and ('status' not in meta or meta['status'] == status) and value in (str(outcome), 'unavailable')):
            return ('http-transport-observation', outcome, status)
        return None
    if (meta.get('kind') == 'http' or 'http_status' in meta or 'original_http_status' in meta
            or 'status' in meta and meta.get('original_child_rc', meta.get('original_rc')) is None):
        status = meta.get('http_status', meta.get('original_http_status', meta.get('status')))
        if status_ok(status) and value == str(status) and meta.get('original_child_rc', meta.get('original_rc')) is None:
            return ('http-status', status)
        return None
    if meta.get('kind') in {'argv', 'Unix-child', 'unix-child', 'Unix-subprocess'}:
        rc = meta.get('original_child_rc', meta.get('original_rc'))
        if type(rc) is int and value == str(rc):
            return ('unix', rc)
    return None


def check_unattributed(value, blobs, entry, projected_commands=None):
    need(set(entry) == {'raw_value', 'companions', 'reason', 'limitations'}
         and entry['raw_value'] == value and entry['reason'] in UNATTRIBUTED_REASONS
         and entry['limitations'] == UNATTRIBUTED_LIMITS, 'finite unattributed literal/reason/limits')
    projected_commands = projected_commands or {}
    need(set(blobs) | set(projected_commands) == set(entry['companions']) and not set(blobs) & set(projected_commands), 'unattributed complete original companion or exact safe projection coverage')
    metas = []; commands = []
    for path, row in projected_commands.items():
        need(row['original_file'] == entry['companions'][path] and row['method'] == 'GET', 'unattributed command projection original pin/method')
        commands.append('GET ' + row['url_without_query_fragment'])
    for path, raw in blobs.items():
        if path.endswith(('.meta', '.meta.json')) or path.rsplit('/', 1)[-1] in {'meta', 'meta.json'}:
            try:
                parsed = json.loads(raw)
            except (ValueError, UnicodeError):
                parsed = None
            metas.append(parsed)
        else:
            try:
                parsed = json.loads(raw)
            except (ValueError, UnicodeError):
                parsed = raw.decode().strip() if path.endswith('.cmd') or path.rsplit('/', 1)[-1] == 'cmd' else None
            commands.append(parsed)
    command_conflict = bool(commands) and not all(v == commands[0] for v in commands)
    cmd = commands[0] if commands and not command_conflict else None
    claims = [current_unattributed_claim(value, meta, cmd) for meta in metas]
    known = [v for v in claims if v is not None]
    incomplete = value in {'unavailable', 'pending', ''} and any(isinstance(meta, dict) and (
        any(k in meta and meta[k] is None for k in ('original_rc', 'original_child_rc'))
        or any(isinstance(meta.get(k), str) and bool(meta[k]) for k in ('failure', 'failure_kind', 'error', 'cause'))
    ) for meta in metas)
    pure = any(isinstance(meta, dict) and (meta.get('pure_only') is True or type(meta.get('actual_service_operations')) is int and meta['actual_service_operations'] == 0) for meta in metas)
    local = any(isinstance(meta, dict) and (meta.get('kind') in {'local-derived-proof', 'local-derived', 'gate', 'expected-facts'}
        or {'bytes', 'ndjson_sha256', 'pages', 'rows', 'src', 'terminal_token_digest', 'window'} <= set(meta)
        or {'expected_ndjson_sha256', 'expected_rows', 'file_download_bytes', 'file_download_sha256', 'file_id', 'receipt', 'version_count', 'version_ids'} <= set(meta)) for meta in metas)
    contradiction = command_conflict or bool(known) and (len(known) != len(metas) or not all(v == known[0] for v in known))
    numeric = int(value) if re.fullmatch('-?[0-9]+', value) else None
    for meta in metas:
        if not isinstance(meta, dict):
            continue
        rcs = [meta[k] for k in ('original_child_rc', 'original_rc', 'rc') if type(meta.get(k)) is int]
        contradiction = contradiction or len(set(rcs)) > 1 or numeric is not None and any(v != numeric for v in rcs)
        statuses = [meta[k] for k in ('http_status', 'original_http_status', 'status') if type(meta.get(k)) is int]
        contradiction = contradiction or len(set(statuses)) > 1
        if type(meta.get('transport_outcome')) is int and meta['transport_outcome'] in (0, 1) and value in {'0', '1'}:
            contradiction = contradiction or str(meta['transport_outcome']) != value
        if meta.get('rc_kind') == 'normalized_harness' and 'transport_exception' in meta:
            status, error = meta.get('original_http_status'), meta['transport_exception']
            if (type(status) is int and 100 <= status <= 599) or (status is None and isinstance(error, str) and bool(error)):
                expected = 0 if error is None and status is not None and 200 <= status < 300 else (status or 1)
                contradiction = contradiction or value != str(expected)
    reason = entry['reason']
    if contradiction:
        need(reason == 'contradictory-metadata-retained', 'typed/companion contradiction must remain explicit, never origin-unavailable')
    elif reason == 'contradictory-metadata-retained':
        need(False, 'contradiction disposition needs original conflicting observations')
    if reason == 'explicit-incomplete-observation':
        need(incomplete and not contradiction, 'explicit incomplete literal/null/failure observation required')
    elif incomplete and not contradiction:
        need(False, 'incomplete observation must retain explicit incomplete disposition')
    if reason == 'pure-simulation-process-unclaimed':
        need(pure, 'pure simulation requires explicit metadata, never dirname')
    if reason == 'local-derived-no-process-attestation':
        need(local, 'local derived reason needs explicit original facts')
    if known and len(known) == len(metas) and all(v == known[0] for v in known) and not contradiction:
        need(pure and reason == 'pure-simulation-process-unclaimed', 'recoverable consistent current proof must keep its actual domain')
    disposition = ('incomplete' if reason == 'explicit-incomplete-observation' else
                   'contradiction' if reason == 'contradictory-metadata-retained' else 'reviewed-attribution-insufficient')
    return {'raw_value': value, 'kind': 'retained-unattributed-original', 'disposition': disposition, 'original_unix_rc': None}


OMISSION_LIMITS = 'registered secret no-match does not establish public fixture or secret clean; local original bodies remain unchanged; omitted bytes not verified from package'


def private_body_omissions(r, config, inventory, inventory_ref, source, label, require_approval):
    """Read only explicit metadata; never open any of the147 original bodies."""
    ref = config.get('private_body_omission_addendum')
    if ref is None:
        return {}, None, set()
    cache = {}; omitted = set()
    def rel(path):
        need(isinstance(path, str), 'omission literal metadata path')
        if path.startswith(label + '/'):
            path = path[len(label) + 1:]
        need(path.startswith(PREFIX) and '\\' not in path and all(v not in ('', '.', '..') for v in path.split('/'))
             and not any(v.lower().startswith('.env') or v.lower().endswith(('.env', '.body')) or '.env.' in v.lower() for v in path.split('/')),
             'omission metadata path must be explicit nonprivate results')
        return path
    def raw(pin, suffix=None):
        need(isinstance(pin, dict) and set(pin) in ({'path', 'sha256'}, {'path', 'sha256', 'size'})
             and isinstance(pin['sha256'], str) and re.fullmatch('[a-f0-9]{64}', pin['sha256']), 'omission exact metadata ref')
        path = rel(pin['path']);need(path not in omitted and (suffix is None or path.endswith(suffix)), 'omitted body or wrong metadata type cannot be opened')
        if path not in cache:
            cache[path] = r.read(path)
        data = cache[path]
        need(hashlib.sha256(data).hexdigest() == pin['sha256'] and ('size' not in pin or type(pin['size']) is int and len(data) == pin['size']), 'omission metadata SHA/size drift')
        return data
    def doc(pin):
        return json.loads(raw(pin, '.json'))
    add = doc(ref)
    need(set(add) == {'schema', 'active_S2', 'root_accepted', 'scope', 'inventory', 'omissions', 'diagnostic', 'formal_intersection', 'limits'}
         and add['schema'] == 'p26-history-private-body-omission-v1' and add['active_S2'] == source
         and type(add['root_accepted']) is bool and (add['root_accepted'] or not require_approval)
         and add['scope'] == 'history-only-exact147-private-body-omission' and add['limits'] == OMISSION_LIMITS,
         'Root exact147 history-only omission approval/scope')
    need(rel(add['inventory']['path']) == rel(inventory_ref['path']) and add['inventory']['sha256'] == inventory_ref['sha256'], 'omission original inventory ref unchanged')
    raw(add['inventory'], '.json')
    rows = add['omissions'];members = inventory['members']
    need(isinstance(rows, dict) and len(rows) == 147 and set(rows) <= set(members), 'only exact147 inventoried private bodies')
    omitted = set(rows)
    for path, pin in rows.items():
        rel(path)
        need(path.endswith('.raw') and set(pin) == {'sha256', 'size', 'reason', 'local_original_preserved', 'package_bytes_omitted', 'eligible_for_S2_formal'}
             and type(pin['size']) is int and {k: pin[k] for k in ('sha256', 'size')} == {k: members[path][k] for k in ('sha256', 'size')}
             and pin['reason'] == 'unknown-or-unregistered-opaque-credential-potentially-private'
             and pin['local_original_preserved'] is True and pin['package_bytes_omitted'] is True
             and pin['eligible_for_S2_formal'] is False and members[path]['eligible_for_S2_formal'] is False,
             'omission preserves original pin/local body, never redaction or formal green')
    diag = add['diagnostic'];formal = add['formal_intersection']
    need(set(diag) == {'report', 'tool', 'original_rc', 'audit'} and set(formal) == {'report', 'formal_paths'}, 'complete original diagnostic/formal intersection refs')
    report = doc(diag['report']);tool = raw(diag['tool'], '.py');need(bool(tool), 'diagnostic tool bytes required')
    need(raw(diag['original_rc'], '.rc').strip() == b'0', 'diagnostic original Unixrc0 required')
    audit = doc(diag['audit'])
    need(audit['schema'] == 'p26-opaque-dsn-memory-diagnostic-original-audit-v1' and type(audit['original_child_rc']) is int and audit['original_child_rc'] == 0
         and audit['no_match_does_not_release_opaque_files'] is True and audit['diagnostic_not_secretclean'] is True
         and audit['private_value_bytes_or_hashes_persisted'] is False, 'original diagnostic is not secret-clean/public-fixture proof')
    auditbase = rel(diag['audit']['path']).rsplit('/', 1)[0]
    expected_names = {'RESULT.json', 'Root-release.snapshot.json', 'after.json', 'before.json', 'command.json', 'execute-once.py', 'original-child.rc', 'stderr.raw', 'stdout.raw'}
    auditpins = {rel(path): {'path': rel(path), 'sha256': pin['sha256'], 'size': pin['bytes']} for path, pin in audit['files'].items()}
    need(set(auditpins) == {auditbase + '/' + v for v in expected_names} | {rel(diag['report']['path'])}, 'exact10 diagnostic originals, no private reference following')
    audata = {path: raw(pin) for path, pin in auditpins.items()}
    for field in ('report', 'original_rc'):
        path = rel(diag[field]['path']);need(path in auditpins and auditpins[path]['sha256'] == diag[field]['sha256'], 'diagnostic report/rc exact original audit binding')
    need(audata[auditbase + '/stderr.raw'] == b'' and audata[auditbase + '/original-child.rc'].strip() == b'0', 'diagnostic stderr/originalrc')
    before, after, release, result, command = [json.loads(audata[auditbase + '/' + v]) for v in ('before.json', 'after.json', 'Root-release.snapshot.json', 'RESULT.json', 'command.json')]
    need({k: v for k, v in before.items() if k != 'utc'} == {k: v for k, v in after.items() if k != 'utc'}
         and before['tool_sha256'] == release['tool_sha256'] == diag['tool']['sha256']
         and before['release_sha256'] == auditpins[auditbase + '/Root-release.snapshot.json']['sha256'], 'actual diagnostic source/release stable before-after')
    need(release['schema'] == 'p26-opaque-dsn-memory-diagnostic-release-v1' and release['phase'] == 'opaque-dsn-memory-diagnostic'
         and release['root_accepted'] is True and release['released_by'] == 'Root' and release['source_sha256'] == source
         and release['workspace_root'] == label and rel(release['output_path']) == rel(diag['report']['path']), 'original Root diagnostic phase/source/output')
    need(command[:3] == ['python3', '-I', '-B'] and len(command) == 6
         and command[3] == label + '/tools/smoke/business-platform-p2-6/archive/p26_opaque_dsn_memory_diagnostic.py'
         and command[4] == '--release-file' and type(result['original_child_rc']) is int and result['original_child_rc'] == 0
         and result['no_match_does_not_release_opaque_files'] is True and result['diagnostic_rc0_is_not_secret_clean'] is True and result['report_exists'] is True,
         'original actual diagnostic invocation/termination; never execute it here')
    manifest = doc(release['path_manifest'])
    need(manifest['schema'] == 'p26-opaque-dsn-exact147-path-manifest-v1' and type(manifest['count']) is int and manifest['count'] == 147
         and manifest['no_credential_bytes_or_value_hashes'] is True and set(manifest['members']) == omitted
         and before['path_manifest_sha256'] == release['path_manifest']['sha256']
         and rel(manifest['inventory']['path']) == rel(inventory_ref['path']) and manifest['inventory']['sha256'] == inventory_ref['sha256'],
         'actual exact147 path manifest/original inventory binding')
    need(all(manifest['members'][path] == {k: rows[path][k] for k in ('sha256', 'size')} for path in omitted), 'actual diagnostic147 original pins match omissions')
    need(isinstance(report, list) and len(report) == 296 and all(set(v) == {'path', 'kind', 'count', 'input-id'} and type(v['count']) is int and v['count'] >= 0 and v['input-id'] == '' for v in report), 'strict safe diagnostic report shape, no value/hash/sample fields')
    expected_report = {(path, kind, count) for path in omitted for kind, count in (('opaque-dsn-observation-count', 1), ('no-registered-match-not-a-public-fixture-proof', 0))}
    expected_report |= {('', 'loaded-registered-input-count', 22), ('', 'verified-exact-file-count', 147)}
    need({(v['path'], v['kind'], v['count']) for v in report} == expected_report, 'actual147/22 diagnostic facts, no registered hit or public-fixture inference')
    stdout = json.loads(audata[auditbase + '/stdout.raw'])
    need(stdout == [{'path': rel(diag['report']['path']), 'kind': 'memory-comparison-complete-not-secret-clean', 'count': 147, 'input-id': ''}], 'original safe stdout report binding')
    intersection, formalpaths = doc(formal['report']), doc(formal['formal_paths'])
    need(isinstance(formalpaths, dict) and len(formalpaths) == 31232
         and all(isinstance(path, str) and isinstance(pin, dict) and set(pin) == {'sha256', 'size'} and type(pin['size']) is int
                 and isinstance(pin['sha256'], str) and re.fullmatch('[a-f0-9]{64}', pin['sha256']) for path, pin in formalpaths.items()), 'exact31232 strong formal original paths/pins')
    need(not omitted & set(formalpaths) and type(intersection['opaque_candidates']) is int and intersection['opaque_candidates'] == 147
         and type(intersection['formal_union_count']) is int and intersection['formal_union_count'] == 31232
         and type(intersection['intersection_count']) is int and intersection['intersection_count'] == 0
         and intersection['intersection'] == {} and intersection['private_values_read'] is False, 'history-only omission must have actual strict formal intersection0')
    return rows, ref, set(cache)


EXTRA_REPORT_PINS = {'tools/smoke/business-platform-p2-6/results/w2/attempt-1/INDEX.md': {'sha256': '4d701ae412aeb721347488d0fb61feb2bcebd35f327634903b2aaf2bf31e0c5b', 'size': 1892}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-1/secret-scan-20261115T013814Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-10/secret-scan-20261115T044007Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-11/secret-scan-20261115T050352Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-12/secret-scan-20261115T050858Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-13/secret-scan-20261115T051649Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-2/secret-scan-20261115T015523Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-3/secret-scan-20261115T021621Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-4/secret-scan-20261115T021939Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-5/secret-scan-20261115T023019Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-6/secret-scan-20261115T025633Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-7/secret-scan-20261115T033409Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}, 'tools/smoke/business-platform-p2-6/results/w2/attempt-9/secret-scan-20261115T043647Z.txt': {'sha256': 'bab6fb4a9dfcdff5d6d67ef4952d6b589ac3bd01407003b98b8a3cda4329fc61', 'size': 153}}

EXTRA_REPORT_LIMITS = 'conservative potential-private history body omission; not a finding that these bodies are secrets; originals preserved local; omitted body bytes not verified from package; no fixture, Unix-success, service-success or secret-clean claim'


def extra_report_omissions(r, config, inventory, inventory_ref, source, label, require_approval):
    """Only the13 reviewed metadata pins; never read an omitted report body."""
    ref = config.get('private_extra_report_omission_addendum')
    if ref is None:
        return {}, None, set()
    cache = {}
    def rel(path):
        need(isinstance(path, str), 'extra13 metadata path')
        if path.startswith(label + '/'):
            path = path[len(label) + 1:]
        need(path.startswith(PREFIX) and '\\' not in path
             and all(x not in ('', '.', '..') for x in path.split('/'))
             and not any(x.lower().startswith('.env') or x.lower().endswith(('.env', '.body')) or '.env.' in x.lower() for x in path.split('/')),
             'extra13 explicit nonprivate result metadata path')
        return path
    def raw(pin, suffix=None):
        need(isinstance(pin, dict) and set(pin) in ({'path', 'sha256'}, {'path', 'sha256', 'size'})
             and isinstance(pin['sha256'], str) and re.fullmatch('[a-f0-9]{64}', pin['sha256']), 'extra13 metadata ref shape')
        path = rel(pin['path'])
        need(path not in EXTRA_REPORT_PINS and path not in PRIVATE_COMMAND_PATHS
             and (suffix is None or path.endswith(suffix)), 'extra13 private original or wrong metadata type blocked before read')
        if path not in cache:
            cache[path] = r.read(path)
        data = cache[path]
        need(hashlib.sha256(data).hexdigest() == pin['sha256']
             and ('size' not in pin or type(pin['size']) is int and len(data) == pin['size']), 'extra13 metadata SHA/size')
        return data
    def doc(pin):
        return json.loads(raw(pin, '.json'))
    add = doc(ref)
    need(set(add) == {'schema', 'active_S2', 'root_accepted', 'scope', 'inventory', 'omissions', 'diagnostic', 'structure_proof', 'formal_intersection', 'limits'}
         and add['schema'] == 'p26-history-extra-opaque-report-omission-v1'
         and add['active_S2'] == source and type(add['root_accepted']) is bool
         and (add['root_accepted'] or not require_approval)
         and add['scope'] == 'history-only-exact13-opaque-report-body-omission'
         and add['limits'] == EXTRA_REPORT_LIMITS, 'Root exact13 potential-private history-only scope')
    need(rel(add['inventory']['path']) == rel(inventory_ref['path'])
         and add['inventory']['sha256'] == inventory_ref['sha256'] == '42bd171b599d44ecd82b4e89b1a88f3d219c2d7274d7ac5f6c9fee08bab3ff0d', 'extra13 original inventory binding')
    raw(add['inventory'], '.json')
    rows = add['omissions']; members = inventory['members']
    need(isinstance(rows, dict) and set(rows) == set(EXTRA_REPORT_PINS) and len(rows) == 13, 'only exact13 original reports, never a14th')
    for path, pin in rows.items():
        rel(path)
        need(set(pin) == {'sha256', 'size', 'reason', 'local_original_preserved', 'package_bytes_omitted', 'eligible_for_S2_formal'}
             and type(pin['size']) is int and {k: pin[k] for k in ('sha256', 'size')} == EXTRA_REPORT_PINS[path]
             == {k: members[path][k] for k in ('sha256', 'size')}
             and pin['reason'] == 'unresolved-opaque-historical-report-potentially-private'
             and pin['local_original_preserved'] is True and pin['package_bytes_omitted'] is True
             and pin['eligible_for_S2_formal'] is False and members[path]['eligible_for_S2_formal'] is False,
             'extra13 immutable original pins/local preservation/nonformal boundary')
    diag = add['diagnostic']; formal = add['formal_intersection']
    need(set(diag) == {'report', 'original_rc', 'audit', 'tool'} and set(formal) == {'report', 'formal_paths'}, 'extra13 complete independent diagnostic/formal refs')
    report = doc(diag['report']); audit = doc(diag['audit'])
    need(raw(diag['original_rc'], '.rc').strip() == b'0'
         and type(audit['original_child_rc']) is int and audit['original_child_rc'] == 0
         and audit['schema'] == 'p26-final-scan1-failure-diagnostic-original-audit-v1'
         and audit['diagnostic_only_not_official_scan1_retry'] is True
         and audit['private_values_or_value_hashes_persisted'] is False, 'extra13 diagnostic rc0 is not scan1/clean')
    base = rel(diag['audit']['path']).rsplit('/', 1)[0]
    need(base == PREFIX + 'w6/S2-readiness/final-memory-scan1-failure-diagnostic-outer-attempt1', 'extra13 original diagnostic outer')
    names = {'RESULT.json', 'Root-release.snapshot.json', 'after.json', 'before.json', 'command.json', 'original-child.rc', 'stderr.raw', 'stdout.raw'}
    pins = {rel(p): dict(v, path=rel(p)) for p, v in audit['files'].items()}
    need(set(pins) == {base + '/' + n for n in names} | {rel(diag['report']['path'])}
         and all(set(v) == {'path', 'sha256', 'size'} and type(v['size']) is int for v in pins.values()), 'extra13 exact9 safe diagnostic originals')
    data = {p: raw(pin) for p, pin in pins.items()}
    need(data[base + '/stderr.raw'] == b'' and data[base + '/original-child.rc'].strip() == b'0'
         and pins[rel(diag['report']['path'])]['sha256'] == diag['report']['sha256']
         and pins[rel(diag['original_rc']['path'])]['sha256'] == diag['original_rc']['sha256'], 'extra13 original report/rc/audit equality')
    before, after, release, result = [json.loads(data[base + '/' + p]) for p in ('before.json', 'after.json', 'Root-release.snapshot.json', 'RESULT.json')]
    need({k:v for k,v in before.items() if k != 'utc'} == {k:v for k,v in after.items() if k != 'utc'}
         and before['release_sha256'] == pins[base + '/Root-release.snapshot.json']['sha256'], 'extra13 diagnostic original pins stable')
    tool = raw(diag['tool'], '.py')
    need(tool and release['schema'] == 'p26-final-scan1-failure-diagnostic-release-v1'
         and release['phase'] == 'p26-final-scan1-memory-failure-diagnostic'
         and release['root_accepted'] is True and release['source_sha256'] == source and release['workspace_root'] == label
         and release['tool_sha256'] == diag['tool']['sha256']
         and type(result['original_child_rc']) is int and result['original_child_rc'] == 0
         and result['before_after_pins_stable'] is True and result['official_scan1_failure_unchanged'] is True
         and result['retry_performed'] is False, 'extra13 diagnostic provenance, not service result')
    manifest = doc(release['package_manifest'])
    need(manifest['schema'] == 'p26-secret-scan-members-v1' and manifest['source_sha256'] == source
         and before['manifest_sha256'] == release['package_manifest']['sha256']
         and all(manifest['members'][p] == EXTRA_REPORT_PINS[p] for p in rows), 'extra13 original staged bytes metadata binding')
    need(report['diagnostic_complete'] is True and report['values_persisted'] is False
         and report['scanner_CLI_success_claimed'] is False and report['secret_clean_claimed'] is False
         and type(report['official_scan1_original_rc']) is int and report['official_scan1_original_rc'] == 1
         and type(report['members']) is int and report['members'] == len(manifest['members']) == 138610
         and type(report['registered_value_count']) is int and report['registered_value_count'] == 22,
         'extra13 diagnostic complete does not relabel failed original scan1')
    findings = report['findings']
    need(isinstance(findings, list) and all(set(x) == {'kind','path','line','classification','count'}
         and type(x['line']) is int and type(x['count']) is int and x['count'] > 0 for x in findings), 'extra13 no raw finding value/length/hash/context')
    observations = {x['path'] for x in findings if x['path'] in rows and x['kind'] == 'secret-env-assign' and x['classification'] == 'suspect'}
    need(observations == set(rows), 'extra13 all unresolved report observations retained')
    structure = doc(add['structure_proof'])
    need(structure['private_env_reads_stat_hash'] is False and structure['helpers_stage_unchanged'] is True
         and structure['public_classification_not_assumed'] is True and structure['formal_intersection'] == []
         and {x['path'] for x in structure['rows']} == set(rows)
         and all(x['source_file_pin'] == EXTRA_REPORT_PINS[x['path']] for x in structure['rows']), 'extra13 bounded structure proof original pins, not fixture')
    intersection = doc(formal['report']); paths = doc(formal['formal_paths'])
    need(isinstance(paths, dict) and len(paths) == 31236 and not set(rows) & set(paths)
         and all(set(v) == {'sha256','size'} and type(v['size']) is int for v in paths.values())
         and intersection['schema'] == 'p26-extra13-formal-intersection-v1'
         and type(intersection['candidate_count']) is int and intersection['candidate_count'] == 13
         and type(intersection['formal_union_count']) is int and intersection['formal_union_count'] == 31236
         and type(intersection['intersection_count']) is int and intersection['intersection_count'] == 0
         and type(intersection['source404_intersection_count']) is int and intersection['source404_intersection_count'] == 0
         and intersection['intersection'] == {} and intersection['members'] == EXTRA_REPORT_PINS
         and intersection['formal_paths'] == formal['formal_paths'] and intersection['body_bytes_read'] is False,
         'extra13 independently rebuild exact formal/source intersection0')
    return rows, ref, set(cache)


PRIVATE_COMMAND_LIMITS = 'only method and URL without query/fragment are independently read from new projection; original private command bodies/query bytes not verified from package; unrecorded request headers unclaimed; no credential-clean or business verdict'
PRIVATE_COMMAND_PATHS = frozenset(['tools/smoke/business-platform-p2-6/results/w7/S2-attempts/a_then_b_recover_a_head_unchanged-20261115T111106Z/018-05-a-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/a_then_b_recover_a_head_unchanged-20261115T111106Z/028-07-b-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/budget_exhausted_then_lookup_finalize-20261115T110929Z/077-12-budget-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/dropack_auto_retry_exact_replay-20261115T105934Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/dropack_manual_owned_restart_lookup-20261115T110427Z/075-20-dropack-mr-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/healthy_incremental_epoch_once-20261115T105857Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/healthy_snapshot_and_legacy-20261115T105817Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/inflight_receipt404_samebody_replay-20261115T110605Z/033-15-inflight-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/pregate_no_intent_owned_restart-20261115T110247Z/071-17-pregate-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/twoinstances_exactly_one_claim-20261115T110755Z/041-12-twoinst-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-cross-CURRENT-supplement-20261115T112605Z/009-CURRENT-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-cross-CURRENT-supplement-20261115T112835Z/009-CURRENT-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/a_then_b_recover_a_head_unchanged-20261115T074334Z/018-05-a-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/a_then_b_recover_a_head_unchanged-20261115T074334Z/028-07-b-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/budget_exhausted_then_lookup_finalize-20261115T074158Z/079-12-budget-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/crossscope-current-output-proof-20261115T074928Z/010-current-crossscope-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_auto_retry_exact_replay-20261115T065903Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_auto_retry_exact_replay-20261115T070904Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_manual_owned_restart_lookup-20261115T072721Z/075-20-dropack-mr-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_incremental_epoch_once-20261115T065842Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_incremental_epoch_once-20261115T070846Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_snapshot_and_legacy-20261115T065815Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_snapshot_and_legacy-20261115T070829Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/inflight_receipt404_samebody_replay-20261115T073839Z/033-15-inflight-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/pregate_no_intent_owned_restart-20261115T072854Z/071-17-pregate-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/twoinstances_exactly_one_claim-20261115T074016Z/041-12-twoinst-file-bytes.cmd'])


def private_command_evidence(r, p, source, inventory=None, inventory_ref=None, require_approval=True):
    """Exact26 approved projection. No original private command read or fake bytes."""
    ref = p.get('private_command_omission_addendum')
    if ref is None:
        return {'commands': {}, 'omissions': {}, 'metadata_paths': set(), 'addendum_ref': None}
    need(isinstance(ref, dict) and isinstance(ref.get('path'), str) and isinstance(ref.get('sha256'), str), 'literal command projection ref')
    label = p['canonical_root_label']
    canonical_ref_path = lambda value: value[len(label)+1:] if value.startswith(label+'/') else value
    key = (canonical_ref_path(ref['path']), ref['sha256'], label, source, require_approval, canonical_ref_path(inventory_ref['path']) if inventory_ref else None, inventory_ref['sha256'] if inventory_ref else None)
    cached = getattr(r, '_p26_exact26_projection', {})
    if key in cached and isinstance(getattr(r, 'files', None), dict) and all(r.files.get(path) == pin for path, pin in cached[key]['metadata_file_pins'].items()):
        result = cached[key]
        if inventory is not None:
            need(all({k: inventory['members'][path][k] for k in ('sha256', 'size')} == {k: pin[k] for k in ('sha256', 'size')} for path, pin in result['omissions'].items()), 'command omission original inventory pins')
        return result
    cache = {}
    def rel(path):
        need(isinstance(path, str), 'command projection literal ref')
        if path.startswith(label + '/'):
            path = path[len(label) + 1:]
        need(path.startswith(PREFIX) and '\\' not in path and all(v not in ('', '.', '..') for v in path.split('/'))
             and not any(v.lower().startswith('.env') or v.lower().endswith(('.env', '.body')) or '.env.' in v.lower() for v in path.split('/')), 'command projection explicit nonprivate metadata path')
        return path
    def raw(pin, suffix=None):
        need(isinstance(pin, dict) and set(pin) in ({'path', 'sha256'}, {'path', 'sha256', 'size'}) and isinstance(pin['sha256'], str) and re.fullmatch('[a-f0-9]{64}', pin['sha256']), 'command exact metadata ref')
        path = rel(pin['path']);need(path not in PRIVATE_COMMAND_PATHS and (suffix is None or path.endswith(suffix)), 'original private command cannot be read as evidence')
        if path not in cache:cache[path] = r.read(path)
        data = cache[path];need(hashlib.sha256(data).hexdigest() == pin['sha256'] and ('size' not in pin or type(pin['size']) is int and len(data) == pin['size']), 'command evidence SHA/size drift')
        return data
    def doc(pin):return json.loads(raw(pin, '.json'))
    add = doc(ref)
    need(set(add) == {'schema', 'active_S2', 'root_accepted', 'scope', 'inventory', 'omissions', 'projection', 'producer', 'formal_intersection', 'limits'}
         and add['schema'] == 'p26-history-private-command-omission-v1' and add['active_S2'] == source
         and type(add['root_accepted']) is bool and (add['root_accepted'] or not require_approval)
         and add['scope'] == 'exact26-private-command-body-omission-with-safe-GET-projection' and add['limits'] == PRIVATE_COMMAND_LIMITS, 'Root exact26 private command projection approval')
    inv = doc(add['inventory']) if inventory is None else inventory
    if inventory_ref is not None:
        need(rel(add['inventory']['path']) == rel(inventory_ref['path']) and add['inventory']['sha256'] == inventory_ref['sha256'], 'command original inventory binding')
    rows = add['omissions'];need(isinstance(rows, dict) and set(rows) == PRIVATE_COMMAND_PATHS, 'only exact26 Rootreviewed private command paths')
    for path, pin in rows.items():
        need(set(pin) == {'sha256', 'size', 'reason', 'local_original_preserved', 'package_bytes_omitted', 'eligible_for_S2_formal'}
             and type(pin['size']) is int and {k: pin[k] for k in ('sha256', 'size')} == {k: inv['members'][path][k] for k in ('sha256', 'size')}
             and pin['reason'] == 'opaque-signed-query-private-command-body' and pin['local_original_preserved'] is True
             and pin['package_bytes_omitted'] is True and pin['eligible_for_S2_formal'] is False, 'command original pin/local-only boundary')
    projection = doc(add['projection']);producer = add['producer'];formal = add['formal_intersection']
    need(set(producer) == {'tool', 'original_rc', 'audit'} and set(formal) == {'report', 'formal_paths'}, 'command complete producer/intersection chain')
    tool = raw(producer['tool'], '.py');need(tool and raw(producer['original_rc'], '.rc').strip() == b'0', 'actual command projector original rc0/tool bytes')
    audit = doc(producer['audit']);need(audit['schema'] == 'p26-private-command-projection-original-audit-v1'
        and type(audit['outer_original_child_rc']) is int and audit['outer_original_child_rc'] == 0 and audit['raw_command_bytes_persisted'] is False
        and type(audit['actual_http_operations']) is int and audit['actual_http_operations'] == 0
        and type(audit['actual_private_environment_reads']) is int and audit['actual_private_environment_reads'] == 0, 'command actual no-service/no-env producer audit')
    base = rel(producer['audit']['path']).rsplit('/', 1)[0];out = rel(add['projection']['path']).rsplit('/', 1)[0]
    expected = {base + '/' + v for v in ('producer-tool.snapshot.py', 'stderr.raw', 'Root-release.snapshot.json', 'original-child.rc', 'command.json', 'stdout.raw', 'meta.json')} | {out + '/' + v for v in ('RESULT.json', 'safe-command-projections.json', 'rc')}
    pins = {rel(path): pin for path, pin in audit['files'].items()};need(all(rel(pin['path']) == path for path, pin in pins.items()) and set(pins) == expected, 'exact10 actual projection originals')
    data = {path: raw(pin) for path, pin in pins.items()}
    for pin in (producer['tool'], producer['original_rc'], add['projection']):
        need(rel(pin['path']) in pins and pins[rel(pin['path'])]['sha256'] == pin['sha256'], 'command actual tool/rc/projection original audit binding')
    need(data[base + '/original-child.rc'].strip() == data[out + '/rc'].strip() == b'0'
         and data[base + '/stderr.raw'] == data[base + '/stdout.raw'] == b'', 'command original inner/outer rc and empty capture')
    release, meta, result, command = [json.loads(data[path]) for path in (base + '/Root-release.snapshot.json', base + '/meta.json', out + '/RESULT.json', base + '/command.json')]
    need(release['schema'] == 'p26-private-command-projection-release-v1' and release['phase'] == 'p26-exact26-private-command-safe-projection'
         and release['root_accepted'] is True and release['source_sha256'] == source and release['workspace_root'] == label
         and release['tool_sha256'] == producer['tool']['sha256'] == result['tool_sha256']
         and rel(release['output_dir']) == out, 'command actual Root phase/source/tool/output')
    need(command[:3] == ['python3', '-I', '-B'] and len(command) == 6 and command[3] == label + '/tools/smoke/business-platform-p2-6/archive/p26_private_command_projection.py' and command[4] == '--release-file'
         and meta['kind'] == 'original-producer-process' and type(meta['original_child_rc']) is int and meta['original_child_rc'] == 0 and meta['private_command_bytes_captured'] is False,
         'command original actual producer invocation')
    need(result['schema'] == 'p26-private-command-projection-result-v1' and result['ok'] is True
         and type(result['original_child_rc']) is int and result['original_child_rc'] == 0 and result['private_values_persisted'] is False
         and type(result['services_called']) is int and result['services_called'] == 0 and type(result['input_commands']) is int and result['input_commands'] == 26
         and type(result['projected_commands']) is int and result['projected_commands'] == 26, 'actual exact26 projection result, no business verdict')
    need(set(projection) == {'schema','source_sha256','input_manifest','inventory','original_private_commands_preserved','query_values_hashes_lengths_persisted','unrecorded_request_headers_claimed','commands'} and projection['schema'] == 'p26-private-command-safe-projection-v1' and projection['source_sha256'] == source
         and projection['inventory'] == release['inventory'] == add['inventory'] and projection['input_manifest'] == release['paths_manifest']
         and projection['original_private_commands_preserved'] is True and projection['query_values_hashes_lengths_persisted'] is False
         and projection['unrecorded_request_headers_claimed'] is False, 'projection original inventory/input/scope')
    manifest = doc(projection['input_manifest'])
    need(manifest['schema'] == 'p26-opaque-signed-command-paths-metadata-v1' and manifest['inventory_sha256'] == add['inventory']['sha256']
         and type(manifest['path_count']) is int and manifest['path_count'] == 26 and manifest['token_bytes_or_token_hashes_included'] is False
         and set(manifest['members']) == set(rows), 'Rootreviewed exact26 input path manifest')
    commands = projection['commands'];need(set(commands) == set(rows), 'projection exact26 output commands')
    from urllib.parse import urlsplit
    for path, row in commands.items():
        need(set(row) == {'method', 'url_without_query_fragment', 'query_omitted', 'fragment_omitted', 'anonymous', 'scope', 'original_file'}
             and isinstance(row['url_without_query_fragment'], str) and bool(row['url_without_query_fragment'])
             and row['method'] == 'GET' and row['query_omitted'] is True and row['fragment_omitted'] is True and row['anonymous'] is True
             and row['scope'] == 'command-shape-only; unrecorded-request-headers-unclaimed'
             and row['original_file'] == manifest['members'][path] == {k: rows[path][k] for k in ('sha256', 'size')}, 'safe GET command shape and original pin')
        url = urlsplit(row['url_without_query_fragment'])
        need(url.scheme in {'http', 'https'} and url.hostname and url.username is None and url.password is None
             and not url.query and not url.fragment and '?' not in row['url_without_query_fragment'] and '#' not in row['url_without_query_fragment']
             and '\n' not in row['url_without_query_fragment'] and '\r' not in row['url_without_query_fragment']
             and url.path.startswith('/v1/_internal/local-fs/datasets/') and '/transactions/' in url.path and url.path.endswith('.ndjson')
             and url.scheme + '://' + url.netloc in release['allowed_origins'], 'safe URL has no query/fragment/userinfo or foreign origin')
    intersection, formalpaths = doc(formal['report']), doc(formal['formal_paths'])
    need(isinstance(formalpaths, dict) and len(formalpaths) == 31232, 'command strict31232 formal path scope')
    actual = {path: formalpaths[path] for path in set(rows) & set(formalpaths)}
    need(len(actual) == 12 and intersection['intersection'] == actual and type(intersection['intersection_count']) is int and intersection['intersection_count'] == 12
         and type(intersection['candidate_count']) is int and intersection['candidate_count'] == 26 and type(intersection['formal_count']) is int and intersection['formal_count'] == 31232
         and intersection['remaining_history_only'] == {path: manifest['members'][path] for path in set(rows) - set(actual)}
         and intersection['candidate_manifest'] == projection['input_manifest'] and intersection['formal_union'] == formal['formal_paths'], 'exact12 formal +14 historical command-body boundary')
    result = {'commands': commands, 'omissions': rows, 'metadata_paths': set(cache), 'metadata_file_pins': {path: {'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)} for path, data in cache.items()}, 'addendum_ref': ref}
    cached[key] = result;r._p26_exact26_projection = cached
    r._p26_exact26_projection_validations = getattr(r, '_p26_exact26_projection_validations', 0) + 1
    return result


def private_command_shape(r, p, source, path, info=None, original_pin=None):
    if path not in PRIVATE_COMMAND_PATHS:
        return None
    info = info if info is not None else private_command_evidence(r, p, source)
    need(path in info['commands'], 'private command requires explicit Root exact26 projection; never read old body')
    row = info['commands'][path]
    if original_pin is not None:
        need(row['original_file'] == {'sha256': original_pin['sha256'], 'size': original_pin.get('size', original_pin.get('bytes'))}, 'private command historical ref original SHA/size')
    return row


def validate(r, p, S):
    need(p['kind'] == 'final-whole-failed-history' and p['epoch'] == S, 'kind/source')
    need(isinstance(S, str) and re.fullmatch('[0-9a-f]{64}', S) and S != OLD_S, 'full S2 hash')
    label = p['canonical_root_label']
    need(isinstance(label, str) and label.startswith('/') and label != '/' and not label.endswith('/')
         and all(x not in ('.', '..') for x in label.split('/')), 'explicit canonical workspace label')
    refs = p['refs']
    need(isinstance(refs, dict) and set(refs) == {'config', 'inventory', 'catalog'}, 'three explicit final refs required')
    config, inventory, catalog = [pinned(r, refs[k], label) for k in ('config', 'inventory', 'catalog')]
    if refs['inventory']['sha256'] == '42bd171b599d44ecd82b4e89b1a88f3d219c2d7274d7ac5f6c9fee08bab3ff0d':
        need(config.get('private_body_omission_addendum') and config.get('private_command_omission_addendum') and config.get('private_extra_report_omission_addendum'), 'sensitive original inventory requires exact147, exact26 and exact13 approved omission chains')
    need(config['schema'] == 'p26-final-history-config-v1'
         and catalog['schema'] == 'p26-final-history-catalog-v1', 'final config/catalog schemas')
    cutoff = config['final_cutoff']
    need(cutoff == catalog['final_cutoff'] and cutoff['complete'] is True
         and cutoff['active_S2'] == config['active_S2'] == S
         and isinstance(cutoff['cutoff_id'], str) and bool(cutoff['cutoff_id']), 'same complete S2 cutoff')
    stamp = datetime.datetime.fromisoformat(cutoff['utc'].replace('Z', '+00:00'))
    need(stamp.tzinfo is not None, 'UTC cutoff required')
    need(stamp.utcoffset() == datetime.timedelta(0), 'cutoff must be UTC')
    need(catalog['config'] == refs['config'] and catalog['inventory'] == refs['inventory'], 'catalog final input linkage')
    cert = catalog['root_certificate']
    need(cert['reviewed_by'] == 'Root' and cert['root_accepted'] is True
         and cert['scope'] == 'whole-failed-history-at-final-cutoff'
         and cert['final_cutoff'] == cutoff and cert['config'] == refs['config']
         and cert['inventory'] == refs['inventory'], 'Root final whole-history certificate metadata')
    # Preserve the producer\'s candidate/uncertified flags; Root cert does not rewrite history.
    need(inventory['actual_copy'] is False and inventory['full_failed_history_review_complete'] is False
         and inventory['active_S2'] is None, 'original inventory candidate metadata must remain original')
    roots = config['roots']
    need(isinstance(roots, list) and roots and len(set(roots)) == len(roots)
         and inventory['roots'] == roots, 'complete exact root roster')
    for root in roots:
        local(root)
    cohorts = config['explicit_cohorts']
    need(isinstance(cohorts, list) and cohorts and inventory['explicit_cohorts'] == cohorts, 'exact explicit cohort roster')
    for cohort in cohorts:
        local(cohort['root'])
        need(isinstance(cohort['classification'], str) and bool(cohort['classification'])
             and any(cohort['root'] == root or cohort['root'].startswith(root + '/') for root in roots), 'cohort outside roots')
    members = inventory['members']
    need(isinstance(members, dict) and members and isinstance(catalog['members'], dict)
         and set(members) == set(catalog['members']), 'nonempty full member classification closure')
    if 'member_epochs' in config:
        need(isinstance(config['member_epochs'], dict) and set(config['member_epochs']) == set(members)
             and all(catalog['members'][v].get('epoch') == epoch for v, epoch in config['member_epochs'].items()),
             'complete Rootconfigured member epoch mapping must bind catalog')
    need(set(config['explicit_paths']) <= set(members), 'explicit historical paths absent')
    issues = inventory['issues']
    need(issues == config['permitted_issues'] == catalog['issues'], 'every skipped member metadata explicitly reviewed')
    for issue in issues:
        need(isinstance(issue, dict) and issue['kind'] in {'private-or-body-not-read', 'nonregular-not-read', 'symlink-not-read'}, 'read race/unknown inventory issue')
        path = issue['path']
        need(isinstance(path, str) and any(path.startswith(root + '/') for root in roots)
             and path not in members and '..' not in path.split('/'), 'issue scope/member conflict')
    family = config['required_known_families']
    need(isinstance(family, dict) and REQUIRED_KNOWN_FAMILIES <= set(family)
         and catalog['families'] == family, 'all known failure families required')
    for paths in family.values():
        need(isinstance(paths, list) and paths and len(set(paths)) == len(paths)
             and set(paths) <= set(members), 'family needs complete explicit original member refs')
    fixture_members = config.get('fixture_literal_members', [])
    need(isinstance(fixture_members, list) and len(set(fixture_members)) == len(fixture_members)
         and set(fixture_members) <= set(members), 'explicit fixture literal member list')
    known_actual = {path for paths in family.values() for path in paths}
    authentic = config.get('authentic_rc_paths', [])
    need(isinstance(authentic, list) and set(authentic) <= set(members)
         and not set(fixture_members) & (known_actual | set(authentic)), 'known actual originals cannot be fixture literals')
    retained = config.get('retained_zero_literal_paths', [])
    need(isinstance(retained, list) and len(set(retained)) == len(retained)
         and set(retained) <= set(members)
         and all(v.endswith('.rc') or v.rsplit('/', 1)[-1] == 'rc' for v in retained)
         and not set(retained) & (set(authentic) | set(fixture_members)),
         'explicit retained zero list cannot replace authentic/fixture domains')
    recorded = config.get('recorded_child_rc_paths', [])
    sources = config.get('recorded_child_rc_sources', {})
    need(isinstance(recorded, list) and len(set(recorded)) == len(recorded)
         and set(recorded) <= set(members) and isinstance(sources, dict) and set(sources) == set(recorded)
         and not set(recorded) & (set(retained) | set(fixture_members)), 'exact recorded child path/source roster')
    for path, entry in sources.items():
        need((path.endswith('.rc') or path.rsplit('/', 1)[-1] == 'rc') and set(entry) == {'producer', 'rule'}
             and set(entry['producer']) == {'path', 'sha256'}, 'explicit recorded process source ref')
        producer = entry['producer']; local(producer['path'])
        need(producer['path'] in members and producer['path'].endswith('.py')
             and catalog['members'][producer['path']]['epoch'] != 'testfixture'
             and producer['sha256'] == members[producer['path']]['sha256'], 'inventoried producer source SHA required')
    reported = config.get('reported_literal_paths', [])
    report_rules = config.get('reported_literal_rules', {})
    report_limits = config.get('reported_literal_limitations', {})
    need(isinstance(reported, list) and len(set(reported)) == len(reported) and set(reported) <= set(members)
         and isinstance(report_rules, dict) and set(report_rules) == set(reported)
         and all(v in REPORT_LIMITS for v in report_rules.values())
         and report_limits == {v: REPORT_LIMITS[v] for v in set(report_rules.values())}
         and not set(reported) & (set(recorded) | set(retained) | set(fixture_members))
         and all(v.endswith('.rc') or v.rsplit('/', 1)[-1] == 'rc' for v in reported),
         'finite reported literal roster/source limitations, never actual process or literal exception')
    unattributed = config.get('retained_unattributed_originals', {})
    need(isinstance(unattributed, dict) and set(unattributed) <= set(members)
         and not set(unattributed) & (set(recorded) | set(reported) | set(retained) | set(fixture_members)), 'finite unattributed originals cannot override other domains')
    for path, entry in unattributed.items():
        need((path.endswith('.rc') or path.rsplit('/', 1)[-1] == 'rc')
             and set(entry) == {'raw_value', 'companions', 'reason', 'limitations'}
             and isinstance(entry['raw_value'], str) and entry['reason'] in UNATTRIBUTED_REASONS
             and entry['limitations'] == UNATTRIBUTED_LIMITS, 'finite original literal/reason/limits')
        expected = {q: {k: members[q][k] for k in ('sha256', 'size')} for q in unattributed_companion_paths(path) if q in members}
        need(isinstance(entry['companions'], dict)
             and all(isinstance(pin, dict) and set(pin) == {'sha256', 'size'} and type(pin['size']) is int
                     for pin in entry['companions'].values())
             and entry['companions'] == expected, 'unattributed every existing companion exact SHA/size required')
    for path in fixture_members:
        need(members[path]['classification'] == 'history-test-fixture-literal'
             and (path.rsplit('/', 1)[-1] == 'rc' or path.endswith('.rc'))
             and any(c['classification'] == 'history-test-fixture-literal'
                     and any(c['root'].startswith(root + '/') for root in roots)
                     and (path == c['root'] or path.startswith(c['root'] + '/')) for c in cohorts)
             and not re.search(r'/(?:child/rc|original-child\.rc|outer\.rc|launch\.rc)$', path),
             'fixture exception requires explicit dedicated cohort, never actual outer rc')
    body_omissions, omission_ref, omission_metadata = private_body_omissions(r, config, inventory, refs['inventory'], S, label, True)
    command_info = private_command_evidence(r, dict(config, canonical_root_label=label), S, inventory, refs['inventory'], True)
    need(not set(body_omissions) & set(command_info['omissions']), 'private body/command scopes disjoint')
    extra_omissions, extra_ref, extra_metadata = extra_report_omissions(r, config, inventory, refs['inventory'], S, label, True)
    need(not set(extra_omissions) & (set(body_omissions) | set(command_info['omissions'])), 'extra13 omission scope disjoint')
    omissions = dict(body_omissions, **command_info['omissions'], **extra_omissions);omission_metadata |= command_info['metadata_paths'] | extra_metadata
    if omissions:
        need(isinstance(p.get('original_files'), dict) and not set(omissions) & set(p['original_files'])
             and omission_metadata <= set(p['original_files']), 'omitted raw must not be claimed read; omission metadata must enter original-files closure')
        need(catalog.get('private_body_omission_addendum') == omission_ref
             and type(catalog.get('inventoried_original_member_count')) is int and catalog['inventoried_original_member_count'] == len(members)
             and type(catalog.get('packaged_original_member_count')) is int and catalog['packaged_original_member_count'] == len(members) - len(omissions)
             and type(catalog.get('omitted_private_body_count')) is int and catalog['omitted_private_body_count'] == len(body_omissions)
             and catalog.get('private_command_omission_addendum') == command_info['addendum_ref']
             and catalog.get('private_extra_report_omission_addendum') == extra_ref
             and type(catalog.get('omitted_extra_opaque_report_count')) is int and catalog['omitted_extra_opaque_report_count'] == len(extra_omissions)
             and catalog.get('omitted_private_command_count', 0) == len(command_info['omissions'])
             and catalog.get('omitted_original_member_count', len(omissions)) == len(omissions)
             and catalog.get('omitted_original_bytes_verified_from_package') is False, 'catalog must state exact inventoried/packaged/omitted boundary')
    else:
        need(catalog.get('private_body_omission_addendum') is None and catalog.get('omitted_private_body_count', 0) == 0, 'no unsupported private omission claim')
    raw_rc = {}
    for path, pin in members.items():
        local(path)
        need(any(path.startswith(root + '/') for root in roots), 'member outside declared roots')
        need(set(pin) == {'sha256', 'size', 'classification', 'eligible_for_S2_formal'}
             and pin['eligible_for_S2_formal'] is False and type(pin['size']) is int and pin['size'] >= 0, 'inventory member shape/formal eligibility')
        row = catalog['members'][path]
        need(set(row) == {'classification', 'epoch', 'eligible_for_S2_formal'}
             and row['classification'] == pin['classification'] and row['eligible_for_S2_formal'] is False
             and row['epoch'] in {S, OLD_S, 'intermediate', 'not-applicable', 'testfixture'}, 'unknown epoch/classification/formal history confusion')
        need((row['epoch'] == 'testfixture') == (row['classification'] == 'history-test-fixture-literal'), 'fixture epoch must have explicit fixture classification')
        need(isinstance(pin['classification'], str) and pin['classification'].startswith('history-'), 'history classification only')
        matched = next((c for c in cohorts if path == c['root'] or path.startswith(c['root'] + '/')), None)
        if matched is not None:
            need(pin['classification'] == matched['classification'], 'explicit cohort classification conflict')
        need(path not in PRIVATE_COMMAND_PATHS or path in omissions, 'known private command requires explicit exact26 omission metadata')
        need(path not in EXTRA_REPORT_PINS or path in omissions, 'known extra13 reports require approved omission before filesystem access')
        if path in omissions:
            continue  # Original SHA/size retained in inventory + approved metadata; body is never opened.
        raw = r.read(path)
        need(hashlib.sha256(raw).hexdigest() == pin['sha256'] and len(raw) == pin['size'], 'full original member SHA/size drift')
        if path.rsplit('/', 1)[-1] == 'rc' or path.endswith('.rc'):
            raw_rc[path] = raw.decode('utf-8').strip()
    # Numeric-only observations are source facts, not Unix success claims.
    numeric = {path: int(value) for path, value in raw_rc.items() if value.isdigit() and members[path]['size'] < 20}
    need(all(type(v) is int for v in inventory['original_rc_observations'].values())
         and inventory['original_rc_observations'] == numeric, 'all original numeric rc observations exact')
    dispositions = catalog['rc_dispositions']
    need(isinstance(dispositions, dict) and set(dispositions) == set(raw_rc), 'every rc including zero/unavailable/pending must have a disposition')
    need(all(dispositions[v].get('kind') == 'retained-zero-literal' and raw_rc[v] == '0' for v in retained),
         'retained zero roster must contain only its explicit nonservice zero domain')
    need(all(dispositions[v].get('kind') == 'reported-literal' for v in reported)
         and catalog.get('reported_literal_limitations', {}) == report_limits,
         'reported catalog exact nonservice domain/limitations')
    reported_counts = {v: sum(1 for q in reported if report_rules[q] == v) for v in sorted(report_limits)}
    need(catalog.get('reported_literal_counts', {}) == reported_counts, 'reported domain counts must preserve finite exact roster')
    unattributed_counts = {reason: sum(1 for entry in unattributed.values() if entry['reason'] == reason) for reason in sorted({entry['reason'] for entry in unattributed.values()})}
    need(all(dispositions[path].get('kind') == 'retained-unattributed-original' for path in unattributed)
         and isinstance(catalog.get('retained_unattributed_counts', {}), dict)
         and all(type(v) is int for v in catalog.get('retained_unattributed_counts', {}).values())
         and catalog.get('retained_unattributed_counts', {}) == unattributed_counts
         and catalog.get('retained_unattributed_limits') == (UNATTRIBUTED_LIMITS if unattributed else None),
         'unattributed exact original roster/counts/limits')
    events = catalog['no_process_events']
    need(isinstance(events, dict) and events, 'original no-process approval refusals required')
    for name, event in events.items():
        need(isinstance(name, str) and name and set(event) == {'path', 'json_pointer'}, 'explicit no-process raw event')
        need(event['path'] in members and event['path'] not in omissions, 'no-process event absent/private; never read an omitted body')
        observed = pointer(json.loads(r.read(local(event['path']))), event['json_pointer'])
        no_process_event(observed)
    need(any(e['path'] in family['receiptapprovalblocked'] for e in events.values()), 'known receipt approval refusal original omitted')
    for path, row in dispositions.items():
        base_fields = {'raw_value', 'kind', 'disposition', 'original_unix_rc'}
        domain_fields = {'http-observation': {'http_status', 'transport_outcome'},
                         'http-status-observation': {'http_status'},
                         'normalized-harness': {'http_status', 'transport_exception'}}.get(row.get('kind'), set())
        need(set(row) in (base_fields | domain_fields, base_fields | domain_fields | {'event'})
             and row['raw_value'] == raw_rc[path], 'literal rc/disposition metadata conflict')
        value, kind, disposition = raw_rc[path], row['kind'], row['disposition']
        # Existing raw companion metadata determines the domain; catalog cannot
        # relabel an HTTP normalized zero as a successful Unix child.
        stem = path[:-3] if path.endswith('.rc') else None
        metas = [q for q in ((stem + '.meta.json', stem + '.meta') if stem else
                            (path.rsplit('/', 1)[0] + '/meta.json', path.rsplit('/', 1)[0] + '/meta')) if q in members]
        if kind == 'retained-zero-literal':
            need(path in retained and value == '0' and not metas
                 and disposition == 'retained-no-service-green', 'retained zero must be explicit unaccompanied literal, never service success')
        if path in sources:
            need(kind == 'unix' and metas and catalog['members'][path]['epoch'] != 'testfixture', 'recorded actual process must not be a fixture/literal domain')
            command_paths = ((stem + '.cmd.json', stem + '.cmd') if stem else
                             (path.rsplit('/', 1)[0] + '/cmd.json', path.rsplit('/', 1)[0] + '/cmd'))
            commands = [command_doc(r, v, command_info['commands']) for v in command_paths if v in members]
            need(commands and all(c == commands[0] for c in commands), 'exact recorded process command attestation')
            entry = sources[path]; source = r.read(local(entry['producer']['path'])).decode()
            for q in metas:
                recorded_process(value, json.loads(r.read(local(q))), commands[0], entry['rule'], source, path)
        if kind == 'reported-literal':
            need(path in report_rules and metas and row['original_unix_rc'] is None
                 and disposition == 'retained-no-service-green', 'reported original needs exact nonservice roster and metadata')
            command_paths = ((stem + '.cmd.json', stem + '.cmd') if stem else
                             (path.rsplit('/', 1)[0] + '/cmd.json', path.rsplit('/', 1)[0] + '/cmd'))
            commands = [command_doc(r, v, command_info['commands']) for v in command_paths if v in members]
            need(commands and all(c == commands[0] for c in commands), 'reported exact original command required')
            for q in metas:
                check_reported_literal(value, json.loads(r.read(local(q))), commands[0], report_rules[path])
        if kind == 'retained-unattributed-original':
            need(path in unattributed, 'unattributed is exact Root finite scope, never automatic fallback')
            expected_row = check_unattributed(value, {q: r.read(local(q)) for q in unattributed[path]['companions'] if q not in command_info['commands']}, unattributed[path], {q: command_info['commands'][q] for q in unattributed[path]['companions'] if q in command_info['commands']})
            need(row == expected_row, 'unattributed literal/disposition cannot become Unix/HTTP/success')
        bound_domain = False
        for q in (() if kind == 'fixture-literal' and path in fixture_members or kind == 'retained-unattributed-original' else metas):
            rawmeta = r.read(local(q))
            try:
                meta = json.loads(rawmeta)
            except (ValueError, UnicodeError):
                continue  # Verbatim historical key=value metadata remains a member.
            if not isinstance(meta, dict):
                continue
            if kind == 'unix' and meta.get('kind') not in {'argv', 'Unix-child', 'unix-child', 'Unix-subprocess'}:
                need(path in sources, 'shared/normalized recorder needs exact real-process source pairing, never schema-only Unix')
            if meta.get('kind') == 'call' and ('http_status' in meta or type(meta.get('status')) is int) and 'transport_outcome' not in meta:
                command_paths = ((stem + '.cmd.json', stem + '.cmd') if stem else
                                 (path.rsplit('/', 1)[0] + '/cmd.json', path.rsplit('/', 1)[0] + '/cmd'))
                commands = [command_doc(r, v, command_info['commands']) for v in command_paths if v in members]
                cmd = commands[0] if commands and all(c == commands[0] for c in commands) else None
                url = cmd.get('url', cmd.get('path')) if isinstance(cmd, dict) else None
                if isinstance(url, dict):
                    need(set(url) == {'url', 'query_digest'}
                         and (url['query_digest'] is None or isinstance(url['query_digest'], str)
                              and re.fullmatch('[a-f0-9]{64}', url['query_digest'])), 'exact sanitized HTTP URL projection')
                    url = url['url']
                status = meta.get('http_status', meta.get('status'))
                sentinel = set(meta) == {'kind', 'scrub_hits', 'status', 'utc'} and value == '0'
                nullable = 'original_rc' in meta and meta['original_rc'] is None and value == 'unavailable'
                need(kind == 'http-status-observation' and isinstance(cmd, dict)
                     and cmd.get('method') in {'GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'}
                     and isinstance(url, str) and bool(url)
                     and type(status) is int and 100 <= status <= 599
                     and ('status' not in meta or meta['status'] == status)
                     and 'original_child_rc' not in meta and (sentinel or nullable)
                     and row['http_status'] == status, 'native HTTP observation/sentinel original binding')
                bound_domain = True
            elif meta.get('rc_kind') == 'normalized_harness':
                need(kind == 'normalized-harness' and 'original_child_rc' in meta
                     and meta['original_child_rc'] == 'unavailable'
                     and 'original_http_status' in meta and 'transport_exception' in meta
                     and row['http_status'] == meta['original_http_status']
                     and row['transport_exception'] == meta['transport_exception'], 'normalized harness is not a Unix child')
                bound_domain = True
            elif 'transport_outcome' in meta:
                need(type(meta['transport_outcome']) is int and meta['transport_outcome'] in (0, 1)
                     and 'original_child_rc' in meta and meta['original_child_rc'] is None, 'HTTP metadata cannot invent Unix child')
                if kind == 'http-observation':
                    status = meta.get('http_status', meta.get('status'))
                    need(value == 'unavailable' and row['transport_outcome'] == meta['transport_outcome']
                         and row['http_status'] == status
                         and ('status' not in meta or meta['status'] == status), 'nullable HTTP observation metadata conflict')
                    bound_domain = True
                else:
                    need(kind == 'http-transport' and str(meta['transport_outcome']) == value, 'HTTP metadata cannot become Unix success')
            elif meta.get('kind') == 'http' or 'http_status' in meta or ('status' in meta and meta.get('original_child_rc', meta.get('original_rc')) is None):
                status = meta.get('http_status', meta.get('status'))
                need(row['original_unix_rc'] is None and kind in {'http-transport', 'http-status'}, 'HTTP status metadata cannot become Unix/no-process')
                if kind == 'http-status':
                    need(type(status) is int and value == str(status), 'original HTTP status conflict')
            elif 'original_child_rc' in meta or 'original_rc' in meta:
                rc = meta.get('original_child_rc', meta.get('original_rc'))
                if type(rc) is int:
                    need(kind == 'unix' and row['original_unix_rc'] == rc and value == str(rc), 'Unix companion metadata conflict')
                else:
                    need(rc is None and row['original_unix_rc'] is None and kind in {'no-process', 'incomplete'}, 'unavailable companion metadata conflict')
        if kind in {'http-observation', 'http-status-observation', 'normalized-harness'}:
            need(bound_domain, 'native HTTP/harness requires actual companion metadata')
        if kind == 'unix':
            need(re.fullmatch('-?[0-9]+', value) and type(row['original_unix_rc']) is int
                 and row['original_unix_rc'] == int(value), 'Unix rc must be actual integer, never bool/status/null')
            need(disposition in ({'success', 'expected-negative', 'test-fixture'} if int(value) == 0
                                  else {'failed', 'expected-negative', 'test-fixture', 'incomplete'}), 'nonzero Unix cannot be success')
        else:
            need(row['original_unix_rc'] is None, 'HTTP/incomplete/no-process must not invent Unix rc')
            if kind == 'fixture-literal':
                need(path in fixture_members and disposition == 'test-fixture', 'only explicit test-fixture literal may bypass child metadata')
            elif kind == 'reported-literal':
                need(path in report_rules and disposition == 'retained-no-service-green', 'reported literal never supplies a process or service result')
            elif kind == 'retained-unattributed-original':
                need(path in unattributed and row == check_unattributed(value, {q: r.read(local(q)) for q in unattributed[path]['companions'] if q not in command_info['commands']}, unattributed[path], {q: command_info['commands'][q] for q in unattributed[path]['companions'] if q in command_info['commands']}), 'unattributed original remains nonservice, explicit incomplete/contradiction')
            elif kind == 'retained-zero-literal':
                need(path in retained and value == '0' and not metas
                     and disposition == 'retained-no-service-green', 'retained zero has no service/Unix verdict')
            elif kind == 'http-status-observation':
                need(value in {'0', 'unavailable'} and type(row['http_status']) is int
                     and 100 <= row['http_status'] <= 599 and disposition == 'http-observed',
                     'HTTP status observation must not invent transport or Unix success')
            elif kind == 'http-observation':
                outcome, status = row['transport_outcome'], row['http_status']
                need(value == 'unavailable' and type(outcome) is int and outcome in (0, 1)
                     and ((type(status) is int and 100 <= status <= 599) or (status is None and outcome == 1))
                     and disposition == ('http-observed' if outcome == 0 else 'transport-failed'), 'nullable HTTP observation semantics')
            elif kind == 'normalized-harness':
                status, exception = row['http_status'], row['transport_exception']
                need((exception is None or (isinstance(exception, str) and bool(exception)))
                     and ((type(status) is int and 100 <= status <= 599) or (status is None and exception is not None)),
                     'normalized harness original HTTP status/exception required')
                # p26_http_lib.HTTPResponse.ok and Evidence.result convention:
                # even a transport exception with a known status retains that
                # status as the harness literal. This is never an OS exit code.
                normalized = 0 if exception is None and status is not None and 200 <= status < 300 else (status or 1)
                need(value == str(normalized)
                     and disposition == ('http-observed' if exception is None else 'transport-failed'), 'normalized harness literal differs from original status/exception')
            elif kind == 'http-transport':
                need(value in {'0', '1'} and disposition == ('transport-completed' if value == '0' else 'transport-failed'), 'HTTP transport outcome semantics')
            elif kind == 'http-status':
                need(value.isdigit() and 100 <= int(value) <= 599 and disposition in {'http-response', 'expected-negative'}, 'HTTP status semantics')
            elif kind == 'no-process':
                need(value == 'unavailable' and disposition == 'approval-refused'
                     and row.get('event') in events, 'no-process must retain refusal and no Unix rc')
            else:
                need(kind == 'incomplete' and value in {'unavailable', 'pending', ''}
                     and disposition == 'incomplete', 'unknown rc semantics cannot close history')
    return {'kind': p['kind'], 'source_sha256': S, 'final_cutoff': cutoff,
            'whole_failed_history_review_complete': True, 'member_count': len(members),
            'inventoried_original_member_count': len(members),
            'packaged_original_member_count': len(members) - len(omissions),
            'omitted_private_body_count': len(body_omissions),
            'omitted_private_command_count': len(command_info['omissions']),
            'omitted_extra_opaque_report_count': len(extra_omissions),
            'omitted_original_member_count': len(omissions),
            'private_command_projection_metadata_validations': getattr(r, '_p26_exact26_projection_validations', 0),
            'omitted_original_bytes_verified_from_package': False,
            'all_inventoried_member_bytes_verified': not bool(omissions),
            'rc_count': len(raw_rc), 'no_process_events': len(events),
            'known_families': sorted(family), 'formal_success_eligible': False,
            'reported_literal_counts': reported_counts, 'reported_literal_limitations': report_limits,
            'retained_unattributed_counts': unattributed_counts,
            'retained_unattributed_limits': UNATTRIBUTED_LIMITS if unattributed else None,
            'actual_copy': False, 'private_values_read': False}
