-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- Logic of cli_filter.lua (slurm.md §2), separated so tests can inject a
-- fake `slurm` table, option proxies and a fake daemon runner.
--
-- Talking to the daemon: Slurm's Lua has no socket library (the Rocky 9
-- image ships Lua 5.4 without luaposix or luasocket, and sites cannot be
-- assumed to install them), so the request goes through a helper:
--   rsz slurm decide-client --socket SOCK --timeout-ms 1000 < request.json
-- The helper connects to the daemon's Unix socket, sends the JSON-RPC 2.0
-- request (one line), enforces the 1 s deadline itself, and prints the
-- JSON-RPC response (or a bare DecideResult) on stdout; on any error it
-- prints nothing / exits non-zero. The request travels in a temp file from
-- os.tmpname() (mode 0600) that is removed afterwards.
--
-- Negative cache: after a failure, `/tmp/rightsizer-cli-filter-<user>.neg`
-- holds os.time(); the daemon is skipped for 60 s. It is keyed by $USER
-- (sanitized) so the success path does no extra process spawn. A file
-- planted by another user can only make this user skip rightsizer (S1:
-- submissions unchanged), never change resources.

local json = require("json")

local M = {}
M.NEG_TTL_S = 60
M.RESOURCE_KEYS = {
  ["mem"] = true, ["mem-per-cpu"] = true, ["mem-per-gpu"] = true, ["cpus-per-task"] = true,
  ["gres"] = true, ["gpus"] = true, ["gpus-per-node"] = true, ["gpus-per-task"] = true,
  ["gpus-per-socket"] = true,
}
local MEM_FORMS = { "mem", "mem-per-cpu", "mem-per-gpu" }

local function shell_quote(s)
  return "'" .. tostring(s):gsub("'", "'\\''") .. "'"
end
M.shell_quote = shell_quote

-- Default runner: temp file + io.popen of the decide-client helper.
function M.popen_runner(rsz, sock, timeout_ms)
  return function(request)
    local tmp = os.tmpname()
    local f = io.open(tmp, "wb")
    if not f then return nil, "cannot write temp file" end
    f:write(request)
    f:close()
    local cmd = string.format("%s slurm decide-client --socket %s --timeout-ms %d < %s 2>/dev/null",
      shell_quote(rsz), shell_quote(sock), timeout_ms, shell_quote(tmp))
    local p = io.popen(cmd, "r")
    local out = p and p:read("*a")
    if p then p:close() end
    os.remove(tmp)
    if out == nil or out == "" then return nil, "no response" end
    return out
  end
end

function M.neg_path(getenv)
  -- $USER / $LOGNAME are unset under cron, systemd and `docker exec -u`:
  -- fall back to the home directory's name before a shared "unknown".
  local home = getenv("HOME")
  local user = (getenv("USER") or getenv("LOGNAME") or (home and home:match("([^/]+)/*$")) or "unknown"):gsub("[^%w._-]", "_")
  return "/tmp/rightsizer-cli-filter-" .. user .. ".neg"
end

local function recently_failed(path, now)
  local f = io.open(path, "r")
  if not f then return false end
  local ts = tonumber(f:read("*l") or "")
  f:close()
  return ts ~= nil and ts <= now and now - ts < M.NEG_TTL_S
end

-- The marker lives at a predictable name in a world-writable directory, so
-- it is never opened for writing in place: another user could have planted
-- a symlink there (to this user's ~/.bashrc, say) and io.open(path, "w")
-- would truncate its target. Instead the timestamp goes into a fresh
-- os.tmpname() file (mkstemp: created by us, mode 0600) that is renamed
-- over the marker; rename replaces a planted symlink itself, never its
-- target, and fails (we then drop the temp file) on a name another user
-- owns in a sticky /tmp.
local function mark_failed(path, now)
  local ok, tmp = pcall(os.tmpname)
  if not ok or type(tmp) ~= "string" then return end
  local f = io.open(tmp, "w")
  if not f then
    os.remove(tmp)
    return
  end
  f:write(string.format("%d\n", now))
  f:close()
  if not os.rename(tmp, path) then os.remove(tmp) end
end

local function nonempty(v)
  return v ~= nil and v ~= ""
end

-- Build the JSON-RPC request.
function M.request(options, offset, getenv, slurm)
  local opts = json.decode(slurm.json_cli_options(options))
  local argv = opts.argv
  local script_path = json.null
  if type(argv) == "table" and argv[1] ~= nil then script_path = argv[1] end
  return json.encode({
    jsonrpc = "2.0", id = 1, method = "decide",
    params = {
      kind = "slurm",
      native = { kind = "object", value = {
        kind = "cli_filter", type = options.type, offset = offset, options = opts,
        script_path = script_path, cwd = getenv("PWD") or json.null,
      } },
      mode = json.null,
    },
  })
end

-- Parse the helper's output: DecideResult table, or nil.
function M.parse_response(out)
  local ok, v = pcall(json.decode, out)
  if not ok or type(v) ~= "table" then return nil end
  if v.jsonrpc ~= nil or v.error ~= nil or v.result ~= nil then
    if v.error ~= nil and v.error ~= json.null then return nil end
    v = v.result
  end
  if type(v) ~= "table" or type(v.mode) ~= "string" then return nil end
  return v
end

-- Apply a DecideResult to the option proxy (enforce, sbatch only).
function M.apply(options, resp)
  local patch = resp.patch
  if type(patch) ~= "table" or patch.kind ~= "options" or type(patch.options) ~= "table" then return false end
  -- Never touch --mem=0, never introduce a second memory form.
  local present = {}
  for _, k in ipairs(MEM_FORMS) do
    if nonempty(options[k]) then present[#present + 1] = k end
  end
  local mem_zero = options["mem"] == "0"
  local applied = false
  local keys = {}
  for k in pairs(patch.options) do keys[#keys + 1] = k end
  table.sort(keys)
  for _, k in ipairs(keys) do
    local v = patch.options[k]
    local allowed = M.RESOURCE_KEYS[k] and (type(v) == "string" or type(v) == "number")
    if allowed and (k == "mem" or k == "mem-per-cpu" or k == "mem-per-gpu") then
      if mem_zero then allowed = false end
      for _, p in ipairs(present) do
        if p ~= k then allowed = false end
      end
    end
    if allowed then
      local ok = pcall(function() options[k] = tostring(v) end)
      applied = applied or ok
    end
  end
  return applied
end

-- slurm_cli_pre_submit. deps: { slurm, getenv, now, stderr, runner, neg_path }.
function M.pre_submit(options, offset, deps)
  local slurm = deps.slurm
  local getenv = deps.getenv or os.getenv
  if nonempty(getenv("SLURM_JOB_ID")) then return slurm.SUCCESS end -- inside an allocation: never resize
  -- Already sized by another layer (rsz slurm sbatch sets these).
  if nonempty(getenv("RIGHTSIZER_DECIDED")) or nonempty(getenv("RIGHTSIZER_TOKEN")) then return slurm.SUCCESS end
  local now = (deps.now or os.time)()
  local neg = deps.neg_path or M.neg_path(getenv)
  local ok, err = pcall(function()
    if recently_failed(neg, now) then return end
    local req = M.request(options, offset, getenv, slurm)
    local out = deps.runner(req)
    local resp = out and M.parse_response(out)
    if resp == nil then
      mark_failed(neg, now)
      return
    end
    local typ = options.type
    if typ == "sbatch" and resp.mode == "enforce" then
      -- A layer that cannot write its marker does not change the
      -- submission: without a well-formed token (base64url, '.'-joined for
      -- het jobs) nothing is applied, so every resized job is attributable.
      local tok = resp.token
      if type(tok) == "string" and tok:match("^[%w%-_%.]+$") and M.apply(options, resp) then
        -- Keep the user's export semantics: unset means ALL.
        local exp = options["export"]
        if not nonempty(exp) then exp = "ALL" end
        options["export"] = exp .. ",RIGHTSIZER_TOKEN=" .. tok
      end
    elseif type(resp.summary) == "string" and resp.summary ~= "" then
      (deps.stderr or io.stderr):write("rightsizer: " .. resp.summary .. "\n")
    end
  end)
  if not ok and deps.on_error then deps.on_error(err) end
  return slurm.SUCCESS
end

return M
