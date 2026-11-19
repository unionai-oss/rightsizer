local H = require("helper")
local json = require("json")
local V = json.decode(H.read("token-vectors.json"))
local keyfile = os.tmpname()
local f = io.open(keyfile, "w"); f:write(V.keys); f:close()
local logs = {}
slurm = { SUCCESS = 0, ERROR = -1, log_info = function(fmt, ...) logs[#logs + 1] = string.format(fmt, ...) end }
RIGHTSIZER_KEY_FILE = keyfile
RIGHTSIZER_LUA_DIR = ".."
local now = 0
RIGHTSIZER_NOW = function() return now end
local rc = dofile("../job_submit.lua")
H.eq(rc, 0, "script returns SUCCESS")
H.eq(#rightsizer_job_submit.keys(), 2, "keys loaded at script load")
os.remove(keyfile) -- no per-job I/O: verification keeps working

-- job_desc.environment is a proxy in Slurm (pairs() yields nothing).
local function proxy_env(vars)
  return setmetatable({}, { __index = function(_, k) return vars[k] end })
end
local function mk(case, envvars)
  local jd = {}
  for k, v in pairs(case.job_desc) do jd[k] = v end
  jd.environment = proxy_env(envvars or { RIGHTSIZER_TOKEN = case.token })
  jd.het_job_offset = 4294967294.0
  return jd
end
local by = {}
for _, c in ipairs(V.cases) do by[c.name] = c end

for _, c in ipairs(V.cases) do
  now = c.now
  local jd = mk(c)
  H.eq(slurm_job_submit(jd, {}, c.submit_uid), 0, c.name .. " SUCCESS")
  H.eq(jd.admin_comment, c.expected_admin_comment, c.name .. " admin_comment")
  H.eq(jd.comment, nil, c.name .. " comment untouched")
end
local valid = by.valid
now = valid.now
-- Het component offset 1: RIGHTSIZER_TOKEN_1 wins over RIGHTSIZER_TOKEN.
local jd = mk(valid, { RIGHTSIZER_TOKEN = "garbage", RIGHTSIZER_TOKEN_1 = valid.token })
jd.het_job_offset = 1.0
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, valid.expected_admin_comment, "het offset token")
jd = mk(valid, { RIGHTSIZER_TOKEN = valid.token })
jd.het_job_offset = 0.0
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, valid.expected_admin_comment, "het fallback to RIGHTSIZER_TOKEN")
-- Not a batch job: untouched.
jd = mk(valid); jd.script = nil
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, nil, "non-batch untouched")
-- No token: rightsizer::user only when admin_comment empty.
jd = mk(valid, {})
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, "rightsizer::user", "absent token")
jd = mk(valid, {}); jd.admin_comment = "site-note"
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, "site-note", "admin's comment preserved")
-- An error inside attribution never rejects.
jd = setmetatable({}, { __index = function(_, k) if k == "script" then return "x" end error("boom") end, __newindex = function() error("ro") end })
H.eq(slurm_job_submit(jd, {}, 1000), 0, "errors never reject")
H.ok(#logs >= 2, "logged")
H.eq(slurm_job_modify({}, {}, {}, 0), 0, "modify SUCCESS")

-- Key file missing: every token is :user (with id), logged once at load.
logs = {}
RIGHTSIZER_KEY_FILE = "/nonexistent/rightsizer.key"
dofile("../job_submit.lua")
H.ok(#logs == 1 and logs[1]:find("no signing keys"), "missing key logged once")
jd = mk(valid)
slurm_job_submit(jd, {}, 1000)
H.eq(jd.admin_comment, "rightsizer:01JDZ7Q4W8M7C9B5V3N2X1Y0ZA:user", "no keys -> user")
H.done("job_submit")
