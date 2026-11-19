-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- rightsizer cli_filter.lua (slurm.md §2): transparent client-side sizing.
-- Enforces for `sbatch` (when the daemon's effective mode is enforce);
-- recommend-only for salloc/srun (their jobs carry no environment to the
-- controller, so they could not be attributed). A convenience, not a
-- control: users can bypass it. Every error leaves the options unchanged.
-- Logic lives in rightsizer_cli.lua (next to this file).
--
-- Configuration: RIGHTSIZER_RSZ (path of `rsz`, default /usr/bin/rsz) and
-- RIGHTSIZER_SOCKET (daemon socket, default /run/rightsizer.sock), as
-- globals set by the site at the top of this file or environment variables.
-- RIGHTSIZER_DISABLED = true (written by `rsz slurm install --disable`)
-- short-circuits the script: every submission passes through untouched.

local RSZ = rawget(_G, "RIGHTSIZER_RSZ") or os.getenv("RIGHTSIZER_RSZ") or "/usr/bin/rsz"
local SOCK = rawget(_G, "RIGHTSIZER_SOCKET") or os.getenv("RIGHTSIZER_SOCKET") or "/run/rightsizer.sock"
local TIMEOUT_MS = 1000

local function script_dir()
  local src = debug.getinfo(1, "S").source or ""
  if src:sub(1, 1) == "@" then
    return src:sub(2):match("^(.*)/[^/]*$") or "."
  end
  return "."
end
package.path = (rawget(_G, "RIGHTSIZER_LUA_DIR") or script_dir()) .. "/?.lua;" .. package.path

local ok_mod, cli = pcall(require, "rightsizer_cli")

function slurm_cli_setup_defaults(options, early_pass)
  return slurm.SUCCESS
end

function slurm_cli_pre_submit(options, offset)
  if rawget(_G, "RIGHTSIZER_DISABLED") then return slurm.SUCCESS end
  if not ok_mod then return slurm.SUCCESS end
  local ok = pcall(cli.pre_submit, options, offset, {
    slurm = slurm,
    runner = cli.popen_runner(RSZ, SOCK, TIMEOUT_MS),
  })
  return slurm.SUCCESS
end

function slurm_cli_post_submit(offset, job_id, step_id)
  return slurm.SUCCESS
end
