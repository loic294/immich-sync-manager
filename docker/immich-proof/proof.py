#!/usr/bin/env python3
"""Empirical proof of ISM's safety assumptions against a disposable stock Immich v3.4.1.

Driven by run.sh, which brings the compose stack up first. Uses only the Python standard library,
`docker compose exec database psql` for SQL, and synthetic data. Every check prints PASS/FAIL with
evidence; the exit code is the number of failed checks.
"""

import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(HERE, ".work")
EXT = os.path.join(WORK, "ext")  # mounted at /mnt/ext (library import path)
TRASH = os.path.join(WORK, "ism-trash")  # mounted at /mnt/ism-trash (outside import paths)
IMPORT = "/mnt/ext"
EMAIL = "admin@ism.example"
PASSWORD = "proof-password-not-secret"

# Mirrors crates/ism-server/src/immich/{api.rs,mod.rs}.
ADMIN_PERMISSIONS = [
    "asset.read", "asset.update", "asset.delete", "asset.view", "asset.download",
    "library.read", "library.update", "library.statistics", "adminConfig.read",
    "adminConfig.update", "queue.read", "queue.update", "server.about", "user.read",
]
SUPPORTED_MIGRATION = "1792568637987-AddAssetOwnerIdUpdateIdIndex"
INTERFERING_QUEUES = ["library", "sidecar", "metadataExtraction", "storageTemplateMigration"]
SCAN_QUEUES = ["library", "sidecar", "metadataExtraction"]

HOLD_SECONDS = int(os.environ.get("PROOF_HOLD_SECONDS", "60"))
NO_AUTO_SECONDS = int(os.environ.get("PROOF_NO_AUTO_SECONDS", "180"))

results = []


def log(msg=""):
    print(msg, flush=True)


def report(check, ok, evidence):
    results.append((check, ok))
    log(f"[{'PASS' if ok else 'FAIL'}] {check}")
    for line in evidence:
        log(f"       {line}")


def compose(*args, stdin=None):
    return subprocess.run(
        ["docker", "compose", *args], cwd=HERE, input=stdin, capture_output=True, text=True
    )


BASE = "http://" + compose("port", "server", "2283").stdout.strip()


def http(method, path, body=None, key=None, token=None, ok=True):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + "/api" + path, data=data, method=method)
    if data is not None:
        req.add_header("content-type", "application/json")
    if key:
        req.add_header("x-api-key", key)
    if token:
        req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    value = json.loads(raw) if raw else None
    if ok and not 200 <= status < 300:
        raise RuntimeError(f"{method} {path} -> {status}: {value}")
    return status, value


def psql(script, **variables):
    args = ["exec", "-T", "database", "psql", "-U", "ismtest", "-d", "ismtest", "-At", "-F", "|",
            "-v", "ON_ERROR_STOP=1"]
    for k, v in variables.items():
        args += ["-v", f"{k}={v}"]
    r = compose(*args, stdin=script)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr.strip()}")
    return r.stdout.strip()


def sql_rows(query):
    out = psql(query + "\n")
    return [line.split("|") for line in out.splitlines() if line]


ASSET_SQL = (
    'SELECT id, "originalPath", "originalFileName", "isOffline", ("deletedAt" IS NOT NULL), status, '
    '"isFavorite", "checksumAlgorithm", encode(checksum, \'hex\') FROM asset '
)


def asset_rows(where):
    rows = sql_rows(ASSET_SQL + "WHERE " + where + " ORDER BY \"originalPath\"")
    return [
        dict(id=r[0], path=r[1], name=r[2], offline=r[3] == "t", trashed=r[4] == "t",
             status=r[5], favorite=r[6] == "t", algorithm=r[7], checksum=r[8])
        for r in rows
    ]


def by_path(path):
    return asset_rows(f"\"originalPath\" = '{path}'")


def short(a):
    return (f"id={a['id'][:8]} path={a['path']} offline={a['offline']} trashed={a['trashed']} "
            f"status={a['status']} favorite={a['favorite']}")


def png(seed):
    """A tiny 8x8 RGB PNG whose pixels depend on `seed`, so every file has distinct content."""
    rgb = hashlib.sha256(seed.encode()).digest()[:3]
    raw = b"".join(b"\x00" + rgb * 8 for _ in range(8))

    def chunk(tag, data):
        return struct.pack(">I", len(data)) + tag + data + struct.pack(
            ">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 8, 8, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b""))


XMP = """<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
<rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:description><rdf:Alt>
<rdf:li xml:lang="x-default">Synthetic proof sidecar</rdf:li></rdf:Alt></dc:description>
</rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end="w"?>
"""


def write_image(rel, sidecar=False):
    path = os.path.join(EXT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(png(rel))
    if sidecar:
        with open(path + ".xmp", "w") as f:
            f.write(XMP)
    return f"{IMPORT}/{rel}"


def queues(key):
    return {q["name"]: q for q in http("GET", "/queues", key=key)[1]}


def stats(q):
    s = q["statistics"]
    return f"isPaused={q.get('isPaused')} " + " ".join(
        f"{k}={s.get(k)}" for k in ("active", "waiting", "delayed", "paused"))


def is_idle(q):
    """Same rule as api.rs Queue::is_idle."""
    s = q["statistics"]
    return all(s.get(k, 0) == 0 for k in ("active", "waiting", "delayed", "paused"))


def wait_idle(key, names, timeout=180, settle=3):
    time.sleep(settle)
    deadline = time.time() + timeout
    calm = 0
    while time.time() < deadline:
        qs = queues(key)
        if all(is_idle(qs[n]) for n in names):
            calm += 1
            if calm >= 2:
                return True
        else:
            calm = 0
        time.sleep(1.5)
    return False


def wait_paused_idle(key, name="library", timeout=120):
    """ISM's pre-move wait: nothing active, waiting or delayed in the paused queue."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = queues(key)[name]["statistics"]
        if s["active"] == 0 and s["waiting"] == 0 and s["delayed"] == 0:
            return True
        time.sleep(1)
    return False


def set_paused(key, name, paused):
    return http("PUT", f"/queues/{name}", {"isPaused": paused}, key=key)


def held_job_names(name="library"):
    """Names of jobs BullMQ holds in the queue's wait list (read straight from Valkey)."""
    ids = compose("exec", "-T", "redis", "valkey-cli", "LRANGE", f"immich_bull:{name}:wait", "0",
                  "-1").stdout.split()
    return [compose("exec", "-T", "redis", "valkey-cli", "HGET", f"immich_bull:{name}:{i}",
                    "name").stdout.strip() for i in ids]


def library_count(lib):
    return int(sql_rows(f"SELECT count(*) FROM asset WHERE \"libraryId\" = '{lib}'")[0][0])


def scan(key, lib):
    return http("POST", f"/libraries/{lib}/scan", {}, key=key)[0]


# ---------------------------------------------------------------- setup (step 0)

log(f"Immich at {BASE.replace('127.0.0.1', 'localhost')}")
for _ in range(100):
    try:
        if http("GET", "/server/version", ok=False)[0] == 200:
            break
    except (urllib.error.URLError, ConnectionError, OSError):
        pass
    time.sleep(3)
http("POST", "/auth/admin-sign-up", {"email": EMAIL, "password": PASSWORD, "name": "Proof Admin"})
login = http("POST", "/auth/login", {"email": EMAIL, "password": PASSWORD})[1]
TOKEN = login["accessToken"]

# provision_api_key: login -> POST /api-keys with ADMIN_PERMISSIONS -> logout.
s_key, created = http("POST", "/api-keys", {"name": "ism-proof", "permissions": ADMIN_PERMISSIONS},
                      token=TOKEN, ok=False)
KEY = (created or {}).get("secret")
key_id = ((created or {}).get("apiKey") or {}).get("id")
s2_login = http("POST", "/auth/login", {"email": EMAIL, "password": PASSWORD})[1]
TOKEN = s2_login["accessToken"]  # session for setup-only calls (albums, library creation)

lib = http("POST", "/libraries", {"ownerId": login["userId"], "importPaths": [IMPORT],
                                   "name": "Proof library"}, token=TOKEN)[1]
LIB = lib["id"]

a_path = write_image("2026/trip/a.png", sidecar=True)
b_path = write_image("2026/trip/b.png")
e_path = write_image("2026/trip/e.png")
k_path = write_image("2026/keep/k.png")

config = http("GET", "/admin/config", key=KEY)[1]
config["library"]["watch"]["enabled"] = False
config["library"]["scan"]["enabled"] = False
# Every-minute cron: if Immich ignored `enabled: false`, check D would see a scheduled scan.
config["library"]["scan"]["cronExpression"] = "* * * * *"
http("PUT", "/admin/config", config, key=KEY)
verify = http("GET", "/admin/config", key=KEY)[1]["library"]
setup_ok = verify["watch"]["enabled"] is False and verify["scan"]["enabled"] is False
log(f"setup: admin, API key ({len(ADMIN_PERMISSIONS)} permissions), library {LIB[:8]}, "
    f"config library={json.dumps(verify)}")

scan(KEY, LIB)
wait_idle(KEY, SCAN_QUEUES)
imported = asset_rows(f"\"libraryId\" = '{LIB}'")
log(f"initial scan imported {len(imported)} assets: {[a['path'] for a in imported]}")
if not setup_ok or len(imported) != 4:
    log("setup failed")
    sys.exit(100)

# ---------------------------------------------------------------- A: endpoints and shapes

ev = []
ok = True
version = http("GET", "/server/version")[1]
ver = f"{version.get('major')}.{version.get('minor')}.{version.get('patch')}"
ok &= ver == "3.4.1"
ev.append(f"GET /api/server/version -> {json.dumps(version)} (=> {ver})")
migration = sql_rows("SELECT name FROM kysely_migrations ORDER BY name DESC LIMIT 1")[0][0]
ok &= migration == SUPPORTED_MIGRATION
ev.append(f"latest kysely_migrations.name = {migration}")
ok &= s_key == 201 and bool(KEY) and bool(key_id)
ev.append(f"POST /api/api-keys with ADMIN_PERMISSIONS -> {s_key}, secret present={bool(KEY)}, "
          f"apiKey.id present={bool(key_id)}; login fields accessToken/userId/isAdmin = "
          f"{all(k in login for k in ('accessToken', 'userId', 'isAdmin'))}")
s_out = http("POST", "/auth/logout", token=login["accessToken"], ok=False)[0]
ev.append(f"POST /api/auth/logout -> {s_out}")
cfg = http("GET", "/admin/config", key=KEY)[1]
w, sc = cfg["library"]["watch"].get("enabled"), cfg["library"]["scan"].get("enabled")
ok &= isinstance(w, bool) and isinstance(sc, bool)
ev.append(f"GET /api/admin/config (api key): library.watch.enabled={w}, library.scan.enabled={sc}")
s_put = http("PUT", "/admin/config", cfg, key=KEY)[0]
ok &= s_put == 200
ev.append(f"PUT /api/admin/config (api key, unchanged document) -> {s_put}")
qs = http("GET", "/queues", key=KEY)[1]
shape_ok = isinstance(qs, list) and all(
    isinstance(q.get("name"), str) and all(
        isinstance(q.get("statistics", {}).get(k), int) for k in ("active", "waiting", "delayed", "paused"))
    for q in qs)
names = [q["name"] for q in qs]
ok &= shape_ok and all(n in names for n in INTERFERING_QUEUES)
ev.append(f"GET /api/queues -> list of {len(qs)}; shape ok={shape_ok}; all interfering queues "
          f"present={all(n in names for n in INTERFERING_QUEUES)}")
ev.append(f"  library entry: {json.dumps(next(q for q in qs if q['name'] == 'library'))}")
s_p = set_paused(KEY, "library", True)
paused_view = queues(KEY)["library"]
s_r = set_paused(KEY, "library", False)
resumed_view = queues(KEY)["library"]
ok &= s_p[0] == 200 and paused_view["isPaused"] is True and resumed_view["isPaused"] is False
ev.append(f"PUT /api/queues/library {{isPaused:true}} -> {s_p[0]} body={json.dumps(s_p[1])}")
ev.append(f"  after pause: {stats(paused_view)}; after resume: {stats(resumed_view)}")
libs = http("GET", "/libraries", key=KEY)[1]
lib_keys = ("id", "name", "ownerId", "importPaths", "exclusionPatterns")
ok &= all(all(k in l for k in lib_keys) for l in libs)
ev.append(f"GET /api/libraries -> {len(libs)} libraries, fields {sorted(libs[0].keys())}")
ev.append(f"  assetCount present={'assetCount' in libs[0]} (api.rs treats it as optional)")
s_ex = http("PUT", f"/libraries/{LIB}", {"exclusionPatterns": libs[0]["exclusionPatterns"]},
            key=KEY, ok=False)[0]
ok &= s_ex == 200
ev.append(f"PUT /api/libraries/{{id}} {{exclusionPatterns}} -> {s_ex}")
s_scan = scan(KEY, LIB)
ok &= s_scan in (200, 201, 204)
ev.append(f"POST /api/libraries/{{id}}/scan {{}} -> {s_scan}")
wait_idle(KEY, SCAN_QUEUES)
s_asset, asset_json = http("GET", f"/assets/{imported[0]['id']}", key=KEY, ok=False)
ok &= s_asset == 200
ev.append(f"GET /api/assets/{{id}} -> {s_asset}; fields include isOffline={'isOffline' in asset_json}, "
          f"isTrashed={'isTrashed' in asset_json}, originalPath={'originalPath' in asset_json}")
ev.append("DELETE /api/assets and POST /api/trash/restore/assets are exercised in check E")
report("A. ISM endpoints and response shapes on v3.4.1", ok, ev)

# ---------------------------------------------------------------- B: paused queue holds a scan

ev = []
before = library_count(LIB)
set_paused(KEY, "library", True)
wait_paused_idle(KEY)
n_path = write_image("2026/new/n.png")
s_scan = scan(KEY, LIB)
ev.append(f"paused library queue; added {n_path}; POST scan -> {s_scan}")
held = True
samples = []
t0 = time.time()
while time.time() - t0 < HOLD_SECONDS:
    q = queues(KEY)["library"]
    samples.append(stats(q))
    if by_path(n_path) or library_count(LIB) != before:
        held = False
        break
    time.sleep(10)
ev.append(f"over {HOLD_SECONDS}s: new file imported={not held}; queue samples:")
ev += [f"  {s}" for s in sorted(set(samples))]
held_stats = queues(KEY)["library"]["statistics"]
held_count = held_stats["waiting"] + held_stats["paused"] + held_stats["delayed"]
ev.append(f"held BullMQ jobs in immich_bull:library:wait: {held_job_names()}")
ev.append(f"held job count (waiting+paused+delayed) = {held_count}; ISM is_idle() = "
          f"{is_idle(queues(KEY)['library'])}")
set_paused(KEY, "library", False)
idle = wait_idle(KEY, SCAN_QUEUES)
after = by_path(n_path)
ok = held and held_count > 0 and idle and len(after) == 1 and library_count(LIB) == before + 1
ev.append(f"after resume: idle={idle}; assets at {n_path}: {len(after)}; library count "
          f"{before} -> {library_count(LIB)}")
report("B. Scan requested while the library queue is paused is held, runs once on resume", ok, ev)

# ---------------------------------------------------------------- C: managed move

ev = []
a = by_path(a_path)[0]
album = http("POST", "/albums", {"albumName": "Proof album", "assetIds": [a["id"]]}, token=TOKEN)[1]
http("PUT", f"/assets/{a['id']}", {"isFavorite": True}, key=KEY)
side_old = sql_rows(
    f"SELECT id, path FROM asset_file WHERE type = 'sidecar' AND \"assetId\" = '{a['id']}'")
ev.append(f"asset {a['id'][:8]} at {a_path}, album {album['id'][:8]}, favorite set; "
          f"checksumAlgorithm={a['algorithm']}; sidecar rows={[r[1] for r in side_old]}")
before = library_count(LIB)
set_paused(KEY, "library", True)
quiet = wait_paused_idle(KEY)
new_rel = "2026/trip-renamed/a-renamed.png"
new_path = f"{IMPORT}/{new_rel}"
os.makedirs(os.path.join(EXT, os.path.dirname(new_rel)), exist_ok=True)
os.rename(os.path.join(EXT, "2026/trip/a.png"), os.path.join(EXT, new_rel))
os.rename(os.path.join(EXT, "2026/trip/a.png.xmp"), os.path.join(EXT, new_rel + ".xmp"))
ev.append(f"paused + quiet={quiet}; moved file and sidecar on disk to {new_path}(.xmp)")

# pg.rs move_asset + move_sidecar, statement for statement, in one transaction.
def stamp(asset_id):
    return sql_rows(f"SELECT \"updatedAt\", \"updateId\" FROM asset WHERE id = '{asset_id}'")[0]


stamp_before = stamp(a["id"])
checksum_hex = hashlib.sha1(("path:" + new_path).encode()).hexdigest()
side_id = side_old[0][0] if side_old else "00000000-0000-0000-0000-000000000000"
MOVE_SQL = r"""
BEGIN;
SELECT "originalPath" AS cur, "checksumAlgorithm"::text AS alg FROM asset WHERE id = :'asset'::uuid FOR UPDATE \gset
SELECT (:'cur' = :'expected') AS fresh \gset
\if :fresh
\else
  \echo STALE
  ROLLBACK;
  \quit
\endif
SELECT EXISTS (SELECT 1 FROM asset WHERE "originalPath" = :'newpath' AND id <> :'asset'::uuid LIMIT 1) AS taken \gset
\if :taken
  \echo PATH_TAKEN
  ROLLBACK;
  \quit
\endif
SELECT (:'alg' = 'sha1-path') AS pathsum \gset
\if :pathsum
  \set checksum 'decode(' :'sum' ', ''hex'')'
\else
  \set checksum NULL
\endif
UPDATE asset SET "originalPath" = :'newpath', "originalFileName" = :'newname',
       checksum = COALESCE(:checksum, checksum) WHERE id = :'asset'::uuid;
SELECT (:ROW_COUNT = 1) AS moved \gset
\if :moved
\else
  \echo STALE
  ROLLBACK;
  \quit
\endif
\if :has_sidecar
UPDATE asset_file SET path = :'newside' WHERE id = :'side'::uuid AND path = :'oldside';
SELECT (:ROW_COUNT = 1) AS sidemoved \gset
\if :sidemoved
\else
  \echo STALE_SIDECAR
  ROLLBACK;
  \quit
\endif
\endif
COMMIT;
\echo COMMITTED
"""
tx_out = psql(MOVE_SQL, asset=a["id"], expected=a_path, newpath=new_path,
              newname=os.path.basename(new_path), sum=checksum_hex,
              has_sidecar="true" if side_old else "false", side=side_id,
              oldside=a_path + ".xmp", newside=new_path + ".xmp")
ev.append(f"guarded transaction output: {tx_out.splitlines()[-1] if tx_out else '(none)'}")
stamp_after = stamp(a["id"])
ev.append(f"asset updatedAt/updateId bumped by the UPDATE (Immich triggers, so sync clients see it): "
          f"updatedAt={stamp_after[0] != stamp_before[0]} updateId={stamp_after[1] != stamp_before[1]}")
set_paused(KEY, "library", False)
scan(KEY, LIB)
idle = wait_idle(KEY, SCAN_QUEUES)
moved = asset_rows(f"id = '{a['id']}'")
at_new = by_path(new_path)
at_old = by_path(a_path)
album_now = http("GET", f"/albums/{album['id']}", token=TOKEN)[1]
in_album = bool(sql_rows(
    f"SELECT 1 FROM album_asset WHERE \"albumId\" = '{album['id']}' AND \"assetId\" = '{a['id']}'"))
in_album &= album_now.get("assetCount") == 1  # v3.4.1 GET /albums/{id} has no assets array
side_new = sql_rows(
    f"SELECT path FROM asset_file WHERE type = 'sidecar' AND \"assetId\" = '{a['id']}'")
m = moved[0] if moved else {}
ok = (tx_out.endswith("COMMITTED") and idle and len(moved) == 1 and m["path"] == new_path
      and m["name"] == os.path.basename(new_path) and not m["offline"] and not m["trashed"]
      and m["favorite"] and in_album and len(at_new) == 1 and not at_old
      and library_count(LIB) == before
      and (not side_old or [r[0] for r in side_new] == [new_path + ".xmp"]))
ev.append(f"after resume + scan (idle={idle}): {short(m) if m else 'asset missing'}")
ev.append(f"originalFileName={m.get('name')}; checksumAlgorithm={m.get('algorithm')}; "
          f"checksum {'= sha1(path:new)' if m.get('checksum') == checksum_hex else 'unchanged (content hash)'}")
ev.append(f"in album={in_album}; assets at new path={len(at_new)}; assets at old path={len(at_old)}; "
          f"library count {before} -> {library_count(LIB)}")
ev.append(f"sidecar rows now={[r[0] for r in side_new]}")
report("C. Managed move (pause, disk move, guarded SQL, resume, scan) keeps identity", ok, ev)

# Control: the same move WITHOUT the database update (what ISM must never do).
ev = []
b = by_path(b_path)[0]
os.makedirs(os.path.join(EXT, "2026/elsewhere"), exist_ok=True)
os.rename(os.path.join(EXT, "2026/trip/b.png"), os.path.join(EXT, "2026/elsewhere/b.png"))
scan(KEY, LIB)
wait_idle(KEY, SCAN_QUEUES)
old_b = asset_rows(f"id = '{b['id']}'")
new_b = by_path(f"{IMPORT}/2026/elsewhere/b.png")
ev.append(f"old asset: {short(old_b[0]) if old_b else 'gone'}")
ev.append(f"new path: {[short(x) for x in new_b]}")
control_ok = bool(old_b) and (old_b[0]["offline"] or old_b[0]["trashed"]) and len(new_b) == 1 \
    and new_b[0]["id"] != b["id"]
report("C-control. Unmanaged move (no DB update) loses identity: old offline, new asset id",
       control_ok, ev)

# ---------------------------------------------------------------- D: no automatic import

ev = []
lib_cfg = http("GET", "/admin/config", key=KEY)[1]["library"]
ev.append(f"config library={json.dumps(lib_cfg)}")
before = library_count(LIB)
d_path = write_image("2026/new/d.png")
auto = False
seen = set()
t0 = time.time()
while time.time() - t0 < NO_AUTO_SECONDS:
    q = queues(KEY)["library"]
    seen.add(stats(q))
    if by_path(d_path) or library_count(LIB) != before:
        auto = True
        break
    time.sleep(15)
ev.append(f"added {d_path}; after {int(time.time() - t0)}s imported automatically={auto}")
ev.append(f"library queue states seen: {sorted(seen)}")
scan(KEY, LIB)
wait_idle(KEY, SCAN_QUEUES)
d_rows = by_path(d_path)
ev.append(f"manual API scan -> assets at path: {len(d_rows)}; library count {before} -> "
          f"{library_count(LIB)}")
ok = (not auto and lib_cfg["watch"]["enabled"] is False and lib_cfg["scan"]["enabled"] is False
      and len(d_rows) == 1 and library_count(LIB) == before + 1)
report("D. Watch and scheduled scan off: no automatic import; manual scan imports once", ok, ev)

# ---------------------------------------------------------------- E: trash semantics

ev = []
e = by_path(e_path)[0]
s_del = http("DELETE", "/assets", {"ids": [e["id"]], "force": False}, key=KEY, ok=False)[0]
after_del = asset_rows(f"id = '{e['id']}'")[0]
ev.append(f"DELETE /api/assets {{force:false}} -> {s_del}: {short(after_del)}")
os.rename(os.path.join(EXT, "2026/trip/e.png"), os.path.join(TRASH, "e.png"))
scan(KEY, LIB)
wait_idle(KEY, SCAN_QUEUES)
after_scan = asset_rows(f"id = '{e['id']}'")
ev.append(f"file moved to ISM trash (/mnt/ism-trash/e.png) + scan: "
          f"{short(after_scan[0]) if after_scan else 'asset row deleted'}")
s_get = http("GET", f"/assets/{e['id']}", key=KEY, ok=False)
if s_get[0] == 200:
    ev.append(f"  GET /api/assets/{{id}} -> isTrashed={s_get[1].get('isTrashed')} "
              f"isOffline={s_get[1].get('isOffline')}")
os.rename(os.path.join(TRASH, "e.png"), os.path.join(EXT, "2026/trip/e.png"))
s_res = http("POST", "/trash/restore/assets", {"ids": [e["id"]]}, key=KEY, ok=False)
after_res = asset_rows(f"id = '{e['id']}'")
ev.append(f"file moved back + POST /api/trash/restore/assets -> {s_res[0]} {json.dumps(s_res[1])}: "
          f"{short(after_res[0]) if after_res else 'missing'}")
scan(KEY, LIB)
wait_idle(KEY, SCAN_QUEUES)
final = asset_rows(f"id = '{e['id']}'")
at_e = by_path(e_path)
ev.append(f"after scan: {short(final[0]) if final else 'missing'}; assets at path={len(at_e)}")
ok = (s_del in (200, 204) and after_del["trashed"] and bool(after_scan) and bool(final)
      and not final[0]["trashed"] and not final[0]["offline"] and len(at_e) == 1
      and at_e[0]["id"] == e["id"])
report("E. ISM trash: API trash + file out of import path, then back + API restore keeps id", ok, ev)

# ---------------------------------------------------------------- F: UI-style scan while paused

ev = []
before = library_count(LIB)
set_paused(KEY, "library", True)
wait_paused_idle(KEY)
f_path = write_image("2026/new/f.png")
s1 = http("POST", f"/libraries/{LIB}/scan", {}, token=TOKEN)[0]
s2 = http("POST", f"/libraries/{LIB}/scan", {}, token=TOKEN)[0]
time.sleep(5)
q = queues(KEY)["library"]
st = q["statistics"]
held_count = st["waiting"] + st["paused"] + st["delayed"]
ev.append(f"admin session (as the web UI) clicked Scan twice -> {s1}, {s2}; library: {stats(q)}")
ev.append(f"held jobs: {held_job_names()}")
ev.append(f"held count={held_count}; ISM is_idle()={is_idle(q)} (so ISM's post-pause check fails "
          f"closed with 'Immich queued new work while pausing')")
time.sleep(20)
not_imported = not by_path(f_path)
ev.append(f"25s later file imported={not not_imported}")
set_paused(KEY, "library", False)
idle = wait_idle(KEY, SCAN_QUEUES)
f_rows = by_path(f_path)
ev.append(f"after resume: idle={idle}; assets at path={len(f_rows)}; library count {before} -> "
          f"{library_count(LIB)}")
ok = held_count > 0 and not is_idle(q) and not_imported and len(f_rows) == 1 \
    and library_count(LIB) == before + 1
report("F. A UI scan while paused is held and visible in queue statistics", ok, ev)

# ---------------------------------------------------------------- summary

failed = [c for c, good in results if not good]
log("")
log(f"SUMMARY: {len(results) - len(failed)}/{len(results)} PASS" +
    (f"; FAILED: {', '.join(failed)}" if failed else ""))
sys.exit(len(failed))
