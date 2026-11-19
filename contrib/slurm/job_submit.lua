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
local KEYS = nil
if token then
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
  local ok, err = pcall(attribute, job_desc, submit_uid)
  if not ok then log("attribution failed: " .. tostring(err)) end
  return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
  return slurm.SUCCESS
end

return slurm.SUCCESS
