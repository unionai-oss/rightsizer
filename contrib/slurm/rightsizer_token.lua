-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- rightsizer Slurm attribution tokens, v1: a port of the verifier in
-- crates/rightsizer-slurm/src/token.rs (read its module docs for the spec).
--
--   token = base64url-nopad(0x01 | len(kid) | kid | decision_id (26) | expiry u64 BE | mac (32))
--   mac   = HMAC-SHA256(key, "rightsizer-token-v1" NUL kid NUL uid NUL account NUL
--                            sha256hex(script) NUL canonical(resources) NUL decision_id NUL expiry)
--
-- Every function here is pure (no I/O) except load_key_file().

local hmac = require("hmac")
local sha256 = require("sha256")

local M = {}
M.MAC_DOMAIN = "rightsizer-token-v1"
M.TOKEN_ENV = "RIGHTSIZER_TOKEN"
M.DEFAULT_SKEW_S = 120
local NO_VAL = 4294967294
local NO_VAL16 = 65534

local byte, char, sub, fmt = string.byte, string.char, string.sub, string.format

-- Integer formatting: Slurm pushes every number as a float (100.0).
local function int(v)
  return fmt("%.0f", v)
end
M.int = int

-- Byte-order string comparison (table.sort's default uses strcoll).
local function byte_less(a, b)
  local n = math.min(#a, #b)
  for i = 1, n do
    local x, y = byte(a, i), byte(b, i)
    if x ~= y then return x < y end
  end
  return #a < #b
end

local function trim(s)
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function is_count(s)
  local digits = #(s:match("^%d*"))
  if digits == 0 then return false end
  if digits == #s then return true end
  return digits + 1 == #s and ("kmgtp"):find(sub(s, -1), 1, true) ~= nil
end

local function strip_prefix(s, p)
  if sub(s, 1, #p) == p then return sub(s, #p + 1) end
  return nil
end

-- normalize_tres: identical to the Rust function of the same name.
function M.normalize_tres(s)
  if s == nil then return "-" end
  local out = {}
  for raw in (s .. ","):gmatch("([^,]*),") do
    local e = trim(raw):lower()
    if e ~= "" then
      local kind, rest
      local r = strip_prefix(e, "gres/") or strip_prefix(e, "gres:")
      if r then
        kind, rest = "gres", r
      else
        r = strip_prefix(e, "license/") or strip_prefix(e, "license:")
        if r then
          kind, rest = "license", r
        else
          local slash = e:find("/", 1, true)
          if slash then
            kind, rest = sub(e, 1, slash - 1), sub(e, slash + 1)
          else
            local name = e:match("^[^:=]*")
            if name == "cpu" or name == "mem" or name == "node" or name == "billing" or name == "energy" then
              kind, rest = "", e
            else
              kind, rest = "gres", e
            end
          end
        end
      end
      local left, count
      local eq = rest:match("^.*()=")
      if eq then
        left, count = sub(rest, 1, eq - 1), sub(rest, eq + 1)
      else
        local colon = rest:match("^.*():")
        if colon and is_count(sub(rest, colon + 1)) then
          left, count = sub(rest, 1, colon - 1), sub(rest, colon + 1)
        else
          left, count = rest, "1"
        end
      end
      local c1 = left:find(":", 1, true)
      local name, ty
      if c1 then name, ty = sub(left, 1, c1 - 1), sub(left, c1 + 1) else name = left end
      if not (kind == "" and name == "cpu") then
        local n = (kind ~= "" and (kind .. "/") or "") .. name
        if ty and ty ~= "" then n = n .. ":" .. ty end
        out[#out + 1] = n .. "=" .. count
      end
    end
  end
  if #out == 0 then return "-" end
  table.sort(out, byte_less)
  return table.concat(out, ",")
end

local function num_or_dash(v)
  if type(v) ~= "number" then return "-" end
  return int(v)
end

-- canonical(resources) from a job_desc (or any table with the same fields).
function M.canonical(jd)
  local cpt = jd.cpus_per_task
  if type(cpt) ~= "number" or cpt == 0 or cpt == NO_VAL16 then cpt = "-" else cpt = int(cpt) end
  local tmp = jd.pn_min_tmp_disk
  if type(tmp) ~= "number" or tmp == NO_VAL then tmp = "-" else tmp = int(tmp) end
  return table.concat({
    "mem_per_node=" .. num_or_dash(jd.min_mem_per_node),
    "mem_per_cpu=" .. num_or_dash(jd.min_mem_per_cpu),
    "cpus_per_task=" .. cpt,
    "tres_per_node=" .. M.normalize_tres(jd.tres_per_node),
    "tres_per_job=" .. M.normalize_tres(jd.tres_per_job),
    "tres_per_task=" .. M.normalize_tres(jd.tres_per_task),
    "tres_per_socket=" .. M.normalize_tres(jd.tres_per_socket),
    "mem_per_tres=" .. M.normalize_tres(jd.mem_per_tres),
    "tmp_disk=" .. tmp,
  }, "\n")
end

-- The account the MAC covers: job_desc.account, else the default account.
function M.account(jd)
  local a = jd.account
  if a == nil or a == "" then a = jd.default_account end
  if a == nil then a = "" end
  return tostring(a):lower()
end

-- base64url without padding; nil on any invalid input (padding, bad chars,
-- non-zero trailing bits, impossible length).
local B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
local B64V = {}
for i = 1, 64 do B64V[byte(B64, i)] = i - 1 end
function M.b64url_decode(s)
  if type(s) ~= "string" then return nil end
  local n = #s
  if n % 4 == 1 then return nil end
  local out = {}
  local i = 1
  while i <= n do
    local c1, c2, c3, c4 = byte(s, i, i + 3)
    local v1, v2 = B64V[c1], c2 and B64V[c2]
    if v1 == nil or v2 == nil then return nil end
    local v3 = c3 and B64V[c3]
    local v4 = c4 and B64V[c4]
    if c3 and v3 == nil then return nil end
    if c4 and v4 == nil then return nil end
    local x = ((v1 * 64 + v2) * 64 + (v3 or 0)) * 64 + (v4 or 0)
    local b1, b2, b3 = math.floor(x / 65536), math.floor(x / 256) % 256, x % 256
    if not c3 then
      if b2 ~= 0 or b3 ~= 0 then return nil end
      out[#out + 1] = char(b1)
    elseif not c4 then
      if b3 ~= 0 then return nil end
      out[#out + 1] = char(b1, b2)
    else
      out[#out + 1] = char(b1, b2, b3)
    end
    i = i + 4
  end
  return table.concat(out)
end

function M.b64url_encode(s)
  local out = {}
  for i = 1, #s, 3 do
    local a, b, c = byte(s, i, i + 2)
    local x = (a * 256 + (b or 0)) * 256 + (c or 0)
    local d = { math.floor(x / 262144) % 64, math.floor(x / 4096) % 64, math.floor(x / 64) % 64, x % 64 }
    local k = b == nil and 2 or (c == nil and 3 or 4)
    for j = 1, k do out[#out + 1] = sub(B64, d[j] + 1, d[j] + 1) end
  end
  return table.concat(out)
end

local function valid_kid(k)
  return #k >= 1 and #k <= 32 and k:match("^[A-Za-z0-9._-]+$") ~= nil
end

local function valid_ulid(id)
  return #id == 26 and id:match("^[0-7][0-9A-HJKMNP-TV-Z]+$") ~= nil
end

-- Decode a token: {kid, decision_id, expiry (number), mac} or nil, reason.
function M.parse(token)
  if type(token) ~= "string" then return nil, "malformed" end
  local raw = M.b64url_decode(trim(token))
  if not raw or #raw < 2 or byte(raw, 1) ~= 1 then return nil, "malformed" end
  local k = byte(raw, 2)
  if #raw ~= 2 + k + 26 + 8 + 32 then return nil, "malformed" end
  local kid = sub(raw, 3, 2 + k)
  if not valid_kid(kid) then return nil, "malformed" end
  local id = sub(raw, 3 + k, 2 + k + 26):upper()
  if not valid_ulid(id) then return nil, "malformed" end
  local expiry = 0
  for i = 3 + k + 26, 2 + k + 34 do expiry = expiry * 256 + byte(raw, i) end
  return { kid = kid, decision_id = id, expiry = expiry, mac = sub(raw, 3 + k + 34) }
end

function M.mac_message(kid, uid, account, script_hex, canonical, decision_id, expiry)
  return table.concat({ M.MAC_DOMAIN, kid, int(uid), account, script_hex, canonical, decision_id, int(expiry) }, "\0")
end

-- Parse a key file's text: list of {kid, state, created, secret} or nil, err.
function M.parse_keys(text)
  local keys, active = {}, 0
  local lineno = 0
  for line in (text .. "\n"):gmatch("([^\n]*)\n") do
    lineno = lineno + 1
    local l = trim(line)
    if l ~= "" and sub(l, 1, 1) ~= "#" then
      local f = {}
      for w in l:gmatch("%S+") do f[#f + 1] = w end
      if #f ~= 5 or f[1] ~= "rsz-key-v1" then return nil, "line " .. lineno .. ": bad format" end
      if not valid_kid(f[2]) then return nil, "line " .. lineno .. ": bad key id" end
      if f[3] ~= "active" and f[3] ~= "verify" then return nil, "line " .. lineno .. ": bad state" end
      if not f[4]:match("^%d+$") then return nil, "line " .. lineno .. ": bad created" end
      local secret = M.b64url_decode(f[5])
      if not secret or #secret < 16 then return nil, "line " .. lineno .. ": bad secret" end
      if f[3] == "active" then active = active + 1 end
      keys[#keys + 1] = { kid = f[2], state = f[3], created = tonumber(f[4]), secret = secret }
    end
  end
  if active ~= 1 then return nil, "exactly one active key required" end
  if #keys > 2 then return nil, "at most two keys" end
  if #keys == 2 and keys[1].kid == keys[2].kid then return nil, "duplicate key id" end
  return keys
end

function M.load_key_file(path)
  local f, err = io.open(path, "rb")
  if not f then return nil, err end
  local text = f:read("*a")
  f:close()
  return M.parse_keys(text)
end

-- Verify. `keys` may be nil (key file failed to load): everything is user.
-- Returns status ("verified" | "user"), decision_id or nil, reason.
function M.verify(keys, token, jd, submit_uid, now, skew_s)
  if token == nil or trim(tostring(token)) == "" then return "user", nil, "absent" end
  local p = M.parse(token)
  if not p then return "user", nil, "malformed" end
  if now > p.expiry + (skew_s or M.DEFAULT_SKEW_S) then return "user", p.decision_id, "expired" end
  local key
  for _, k in ipairs(keys or {}) do
    if k.kid == p.kid then key = k end
  end
  if not key then return "user", p.decision_id, "unknown_key" end
  local script = jd.script or ""
  local msg = M.mac_message(p.kid, submit_uid, M.account(jd), sha256.hex(script), M.canonical(jd), p.decision_id, p.expiry)
  if hmac.equal(hmac.sha256(key.secret, msg), p.mac) then
    return "verified", p.decision_id, "ok"
  end
  return "user", p.decision_id, "mac"
end

function M.admin_comment(status, decision_id)
  if status == "verified" then return "rightsizer:" .. decision_id .. ":verified" end
  return "rightsizer:" .. (decision_id or "") .. ":user"
end

-- Sign (tests and tooling only; the daemon signs in production).
function M.sign(key, uid, jd, decision_id, expiry)
  local msg = M.mac_message(key.kid, uid, M.account(jd), sha256.hex(jd.script or ""), M.canonical(jd), decision_id, expiry)
  local e, bytes = expiry, {}
  for i = 8, 1, -1 do
    bytes[i] = char(e % 256)
    e = math.floor(e / 256)
  end
  return M.b64url_encode(char(1, #key.kid) .. key.kid .. decision_id .. table.concat(bytes) .. hmac.sha256(key.secret, msg))
end

return M
