local H = require("helper")
local json = require("json")
local tok = require("rightsizer_token")
local V = json.decode(H.read("token-vectors.json"))
local keys = assert(tok.parse_keys(V.keys))
H.eq(#keys, 2, "two keys during rotation")
for _, c in ipairs(V.cases) do
  local jd = {}
  for k, v in pairs(c.job_desc) do jd[k] = v end
  H.eq(tok.canonical(jd), c.canonical, c.name .. " canonical")
  local status, id = tok.verify(keys, c.token, jd, c.submit_uid, c.now, V.skew_s)
  H.eq(tok.admin_comment(status, id), c.expected_admin_comment, c.name .. " admin_comment")
end
-- normalize_tres parity with the Rust unit tests.
local N = tok.normalize_tres
H.eq(N("gres/gpu:a100:2"), "gres/gpu:a100=2", "tres typed")
H.eq(N("gres:gpu:2"), "gres/gpu=2", "tres old prefix")
H.eq(N("gres/gpu=1"), "gres/gpu=1", "tres per task")
H.eq(N("gpu:2"), "gres/gpu=2", "tres bare")
H.eq(N("cpu=3,gres/gpu=1"), "gres/gpu=1", "tres drops cpu")
H.eq(N("cpu=3"), "-", "tres only cpu")
H.eq(N("gres/gpu:10240"), "gres/gpu=10240", "mem_per_tres")
H.eq(N("gres/gpu:A100"), "gres/gpu:a100=1", "tres default count")
H.eq(N("license/matlab=2,gres/gpu:2"), "gres/gpu=2,license/matlab=2", "tres sorted")
H.eq(N("gres/gpu:2k"), "gres/gpu=2k", "count suffix")
H.eq(N(""), "-", "tres empty")
H.eq(N(nil), "-", "tres nil")
-- sign/verify round trip in Lua, and key file errors.
local jd = { script = "#!/bin/sh\ntrue\n", account = "MLteam", min_mem_per_node = 100.0, cpus_per_task = 65534.0, pn_min_tmp_disk = 4294967294.0 }
local t = tok.sign(keys[2], 1000.0, jd, "01JDZ7Q4W8M7C9B5V3N2X1Y0ZB", 2000000000)
H.eq(tok.admin_comment(tok.verify(keys, t, jd, 1000, 1999999999)), "rightsizer:01JDZ7Q4W8M7C9B5V3N2X1Y0ZB:verified", "lua round trip")
H.eq(select(3, tok.verify(nil, t, jd, 1000, 1999999999)), "unknown_key", "no keys loaded")
H.eq(select(3, tok.verify(keys, "", jd, 1000, 0)), "absent", "empty token")
H.eq(select(3, tok.verify(keys, t .. "=", jd, 1000, 0)), "malformed", "padding rejected")
H.ok(not tok.parse_keys(""), "no keys")
H.ok(not tok.parse_keys("rsz-key-v1 k1 verify 1 MDEyMzQ1Njc4OWFiY2RlZg"), "no active")
H.ok(not tok.parse_keys("rsz-key-v1 k1 active 1 c2hvcnQ"), "short secret")
H.ok(not tok.parse_keys("rsz-key-v1 k/1 active 1 MDEyMzQ1Njc4OWFiY2RlZg"), "bad kid")
H.eq(tok.b64url_decode(tok.b64url_encode("hello world!?")), "hello world!?", "b64 round trip")
H.eq(tok.b64url_decode("aGVsbG8"), "hello", "b64 no pad")
H.eq(tok.b64url_decode("aGVsbG9"), nil, "b64 non-canonical trailing bits")
H.done("token")
