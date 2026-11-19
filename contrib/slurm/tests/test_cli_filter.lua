local H = require("helper")
local json = require("json")
local cli = require("rightsizer_cli")
local slurm = { SUCCESS = 0 }

-- A cli_filter options proxy: reads/writes go to `store`; pairs() yields nothing.
local function proxy(store, typ)
  local sets = {}
  local p = setmetatable({}, {
    __index = function(_, k)
      if k == "type" then return typ end
      if k == "argv" then return store.argv or {} end
      return store[k]
    end,
    __newindex = function(_, k, v) store[k] = v; sets[#sets + 1] = k end,
  })
  return p, sets
end
slurm.json_cli_options = function(_) return json.encode(rawget(_G, "CURRENT_OPTS")) end

local env = {}
local function getenv(k) return env[k] end
local neg = os.tmpname(); os.remove(neg)
local stderr_buf = {}
local stderr = { write = function(_, s) stderr_buf[#stderr_buf + 1] = s end }
local now = 1000
local function run(store, typ, response)
  CURRENT_OPTS = {}
  for k, v in pairs(store) do CURRENT_OPTS[k] = v end
  CURRENT_OPTS.argv = json.as_array(store.argv or {})
  local opts, sets = proxy(store, typ)
  local calls = {}
  local runner = function(req) calls[#calls + 1] = req; if type(response) == "function" then return response(req) end return response end
  local rc = cli.pre_submit(opts, 0, { slurm = slurm, getenv = getenv, now = function() return now end, stderr = stderr, runner = runner, neg_path = neg })
  return rc, sets, calls
end
local function resp(mode, options, token, summary)
  return json.encode({ jsonrpc = "2.0", id = 1, result = { mode = mode, decisions = json.as_array({}), summary = summary or "mem 64G -> 24G",
    patch = { kind = "options", options = options }, token = token } })
end

-- Enforce sbatch: whitelisted keys applied, export carries the token.
local s = { mem = "64G", ["cpus-per-task"] = "8", argv = { "/data/train.sh" } }
local rc, sets, calls = run(s, "sbatch", resp("enforce", { mem = "24G", ["cpus-per-task"] = "4", partition = "evil", ["job-name"] = "x" }, "TOK"))
H.eq(rc, 0, "SUCCESS")
H.eq(s.mem, "24G", "mem applied"); H.eq(s["cpus-per-task"], "4", "cpus applied")
H.eq(s.partition, nil, "non-resource key ignored"); H.eq(s["job-name"], nil, "job-name ignored")
H.eq(s.export, "ALL,RIGHTSIZER_TOKEN=TOK", "export nil -> ALL")
local req = json.decode(calls[1])
H.eq(req.method, "decide", "JSON-RPC method")
H.eq(req.params.native.value.kind, "cli_filter", "native kind")
H.eq(req.params.native.value.script_path, "/data/train.sh", "script path")
H.eq(req.params.native.value.options.mem, "64G", "options sent before change")
-- Existing export list preserved.
s = { mem = "64G", export = "NONE" }
run(s, "sbatch", resp("enforce", { mem = "24G" }, "TOK"))
H.eq(s.export, "NONE,RIGHTSIZER_TOKEN=TOK", "export NONE kept")
-- Never a second memory form; never change --mem=0.
s = { ["mem-per-cpu"] = "4G" }
run(s, "sbatch", resp("enforce", { mem = "24G" }, "TOK"))
H.eq(s.mem, nil, "no second mem form"); H.eq(s.export, nil, "nothing applied -> no token")
s = { mem = "0" }
run(s, "sbatch", resp("enforce", { mem = "24G" }, "TOK"))
H.eq(s.mem, "0", "mem=0 untouched")
-- Recommend: unchanged, summary on stderr.
stderr_buf = {}
s = { mem = "64G" }
rc, sets = run(s, "sbatch", resp("recommend", { mem = "24G" }, "TOK", "would set mem 24G"))
H.eq(s.mem, "64G", "recommend unchanged"); H.eq(#sets, 0, "no writes in recommend")
H.ok(table.concat(stderr_buf):find("would set mem 24G"), "summary on stderr")
-- srun/salloc: recommend-only even when the daemon says enforce.
for _, typ in ipairs({ "srun", "salloc" }) do
  stderr_buf = {}
  s = { mem = "64G" }
  rc, sets = run(s, typ, resp("enforce", { mem = "24G" }, "TOK"))
  H.eq(#sets, 0, typ .. " unchanged"); H.ok(#stderr_buf == 1, typ .. " summary")
end
-- Inside an allocation / already sized: daemon not called.
env.SLURM_JOB_ID = "42"
s = { mem = "64G" }
rc, sets, calls = run(s, "srun", resp("enforce", { mem = "24G" }))
H.eq(#calls, 0, "SLURM_JOB_ID skips"); env.SLURM_JOB_ID = nil
env.RIGHTSIZER_DECIDED = "01J"
rc, sets, calls = run(s, "sbatch", resp("enforce", { mem = "24G" }))
H.eq(#calls, 0, "RIGHTSIZER_DECIDED skips"); env.RIGHTSIZER_DECIDED = nil
-- Malformed responses leave options unchanged and set the negative cache.
for i, bad in ipairs({ "not json", '{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"}}', "" }) do
  os.remove(neg)
  s = { mem = "64G" }
  rc, sets, calls = run(s, "sbatch", bad ~= "" and bad or nil)
  H.eq(#sets, 0, "malformed " .. i .. " unchanged"); H.eq(rc, 0, "malformed " .. i .. " SUCCESS")
  H.ok(io.open(neg, "r") ~= nil, "negative cache written " .. i)
end
-- Within 60 s the daemon is skipped; after 60 s it is called again.
now = now + 30
rc, sets, calls = run({ mem = "64G" }, "sbatch", resp("enforce", { mem = "24G" }))
H.eq(#calls, 0, "skipped within 60 s")
now = now + 31
s = { mem = "64G" }
rc, sets, calls = run(s, "sbatch", resp("enforce", { mem = "24G" }, "TOK"))
H.eq(#calls, 1, "called after 60 s"); H.eq(s.mem, "24G", "applied after recovery")
-- A runner that throws: still SUCCESS, unchanged.
os.remove(neg)
s = { mem = "64G" }
rc, sets = run(s, "sbatch", function() error("boom") end)
H.eq(rc, 0, "runner error SUCCESS"); H.eq(#sets, 0, "runner error unchanged")
-- Bare DecideResult (no JSON-RPC envelope) is accepted.
os.remove(neg)
s = { mem = "64G" }
run(s, "sbatch", json.encode({ mode = "enforce", token = "TOK", patch = { kind = "options", options = { mem = "32G" } } }))
H.eq(s.mem, "32G", "bare result")
-- Enforce without a (well-formed) token: nothing is applied (no marker, no change).
for i, t in ipairs({ json.null, "", "A,B", "A B" }) do
  os.remove(neg)
  s = { mem = "64G" }
  rc, sets = run(s, "sbatch", json.encode({ mode = "enforce", token = t, patch = { kind = "options", options = { mem = "32G" } } }))
  H.eq(s.mem, "64G", "no token " .. i .. " unchanged"); H.eq(#sets, 0, "no token " .. i .. " no writes")
end
-- The negative-cache marker is never written through a planted symlink.
os.remove(neg)
local victim = os.tmpname()
local vf = io.open(victim, "w"); vf:write("precious\n"); vf:close()
os.execute("ln -s " .. cli.shell_quote(victim) .. " " .. cli.shell_quote(neg))
run({ mem = "64G" }, "sbatch", "not json")
H.eq(H.read(victim), "precious\n", "symlinked marker: target untouched")
local nf = io.open(neg, "r"); local mark = nf and nf:read("*l"); if nf then nf:close() end
H.eq(tonumber(mark), now, "symlinked marker replaced by our own file")
os.remove(victim); os.remove(neg)
H.eq(cli.shell_quote("a'b"), "'a'\\''b'", "shell quoting")
-- popen runner against a fake helper script.
local helper = os.tmpname()
local f = io.open(helper, "w")
f:write("#!/bin/sh\n[ \"$1 $2 $3\" = \"slurm decide-client --socket\" ] || exit 2\ncat >/dev/null\nprintf '%s' '" .. resp("enforce", { mem = "8G" }) .. "'\n")
f:close(); os.execute("chmod +x " .. helper)
local out = cli.popen_runner(helper, "/tmp/x.sock", 1000)('{"x":1}')
H.eq(cli.parse_response(out).patch.options.mem, "8G", "popen runner")
H.eq(cli.popen_runner("/nonexistent/rsz", "/tmp/x.sock", 1000)("{}"), nil, "popen runner failure")
os.remove(helper); os.remove(neg)
H.done("cli_filter")
