-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- rightsizer job_submit.lua (slurm.md §4): runs inside slurmctld, serially,
-- under the controller's locks. M3 scope: verification and attribution
-- only (no server-side sizing; that is M4).
--
-- For every batch job it verifies the RIGHTSIZER_TOKEN the client side put
-- into the job's environment against the job's *actual* uid, account,
-- script and resource fields, and writes
--   job_desc.admin_comment = "rightsizer:<decision_id>:verified"   (or ":user")
-- It never touches the user's `comment`, never changes resources, never
-- rejects a job, and performs no I/O per job: the key file is read once,
-- when slurmctld loads this script (it re-loads it when the file's mtime
-- changes, e.g. after `rsz slurm rotate-key` + `touch job_submit.lua`).
--
-- Attribution of jobs without a token: slurm.md §4.1 says ":user when absent
-- or invalid". We write "rightsizer::user" for a batch job without a token
-- only when admin_comment is still empty, so an admin's own admin_comment is
-- never overwritten; a job with a token (valid or not) always gets ours.
--
-- RIGHTSIZER_DISABLED = true (written by `rsz slurm install --disable`)
-- short-circuits the script: no key is loaded and no admin_comment written.
--
-- Configuration (read at load time): the key file path is RIGHTSIZER_KEY_FILE
-- (global or environment of slurmctld), default /etc/slurm/rightsizer.key;
-- it must be readable by SlurmUser only (0600).

local DEFAULT_KEY_FILE = "/etc/slurm/rightsizer.key"
local NO_VAL = 4294967294

-- Load the vendored modules (sha256, hmac, rightsizer_token) from this
-- script's directory.
local function script_dir()
  local src = debug.getinfo(1, "S").source or ""
  if src:sub(1, 1) == "@" then
    return src:sub(2):match("^(.*)/[^/]*$") or "."
  end
  return "."
end
local DIR = rawget(_G, "RIGHTSIZER_LUA_DIR") or script_dir()
package.path = DIR .. "/?.lua;" .. package.path

local function log(msg)
  if slurm and slurm.log_info then
    pcall(slurm.log_info, "rightsizer: %s", msg)
  end
end

local ok_mod, token = pcall(require, "rightsizer_token")
if not ok_mod then
  log("cannot load rightsizer_token.lua: " .. tostring(token))
  token = nil
end

local KEY_FILE = rawget(_G, "RIGHTSIZER_KEY_FILE") or os.getenv("RIGHTSIZER_KEY_FILE") or DEFAULT_KEY_FILE
local SKEW_S = tonumber(rawget(_G, "RIGHTSIZER_SKEW_S") or os.getenv("RIGHTSIZER_SKEW_S") or "") or 120
local DISABLED = rawget(_G, "RIGHTSIZER_DISABLED") and true or false
local KEYS = nil
if DISABLED then
  log("disabled (RIGHTSIZER_DISABLED): jobs pass through without attribution")
elseif token then
  local ok, keys, err = pcall(token.load_key_file, KEY_FILE)
  if ok and keys then
    KEYS = keys
    log(string.format("loaded %d key(s) from %s", #keys, KEY_FILE))
  else
    -- Logged once (here, at load); every token then attributes to the user.
    log("no signing keys (" .. tostring(ok and err or keys) .. "); all jobs attributed to users")
  end
end

local function now()
  return (rawget(_G, "RIGHTSIZER_NOW") or os.time)()
end

local function find_token(job_desc)
  local env = job_desc.environment
  if env == nil then return nil end
  local off = job_desc.het_job_offset
  if type(off) == "number" and off ~= NO_VAL then
    local t = env[token.TOKEN_ENV .. "_" .. string.format("%.0f", off)]
    if t ~= nil and t ~= "" then return t end
  end
  local t = env[token.TOKEN_ENV]
  if t == "" then return nil end
  return t
end

-- Attribution for one job; returns the admin_comment written (or nil).
local function attribute(job_desc, submit_uid)
  if token == nil or job_desc.script == nil then return nil end -- not a batch job
  local tok = find_token(job_desc)
  if tok == nil then
    local cur = job_desc.admin_comment
    if cur == nil or cur == "" then
      job_desc.admin_comment = "rightsizer::user"
      return "rightsizer::user"
    end
    return nil
  end
  local status, id = token.verify(KEYS, tok, job_desc, submit_uid, now(), SKEW_S)
  local c = token.admin_comment(status, id)
  job_desc.admin_comment = c
  return c
end

-- Exposed for tests.
rightsizer_job_submit = { attribute = attribute, keys = function() return KEYS end, key_file = KEY_FILE }

function slurm_job_submit(job_desc, part_list, submit_uid)
  if DISABLED then return slurm.SUCCESS end
  local ok, err = pcall(attribute, job_desc, submit_uid)
  if not ok then log("attribution failed: " .. tostring(err)) end
  return slurm.SUCCESS
end

-- A user may lower a pending job's resources (`scontrol update`) after
-- job_submit verified its token. admin_comment cannot be changed here:
-- slurmctld denies a user's update that sets it (admin-only field), and
-- this plugin never rejects. So the change is only logged; the sacct
-- importer drops the attribution of a verified job whose resources no
-- longer match its decision (`rsz history import slurm`).
local function modified_resources(jd)
  local function set(v) return v ~= nil and v ~= NO_VAL and v ~= 65534 end
  return set(jd.min_mem_per_node) or set(jd.min_mem_per_cpu) or set(jd.cpus_per_task)
    or jd.tres_per_node ~= nil or jd.tres_per_job ~= nil or jd.tres_per_task ~= nil or jd.mem_per_tres ~= nil
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
  if DISABLED then return slurm.SUCCESS end
  pcall(function()
    local ac = job_rec and job_rec.admin_comment
    if type(ac) == "string" and ac:match("^rightsizer:[^:]+:verified$") and modified_resources(job_desc) then
      log(string.format("job %.0f: resources modified by uid %.0f after verification (%s); sacct import will not attribute it",
        tonumber(job_rec.job_id) or -1, tonumber(modify_uid) or -1, ac))
    end
  end)
  return slurm.SUCCESS
end

return slurm.SUCCESS
