-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- HMAC-SHA256 (RFC 2104) on top of sha256.lua, and a constant-time compare.
-- API: hmac.sha256(key, msg) -> 32-byte binary; hmac.sha256_hex(key, msg);
--      hmac.equal(a, b) -> boolean (time depends only on the lengths).

local sha256 = require("sha256")

local M = {}
local byte, char = string.byte, string.char
local BLOCK = 64

local function xor_byte(a, b)
  -- a XOR b for bytes, without bit libraries.
  local r, p = 0, 1
  for _ = 1, 8 do
    local x, y = a % 2, b % 2
    if x ~= y then r = r + p end
    a, b, p = (a - x) / 2, (b - y) / 2, p * 2
  end
  return r
end

-- Precomputed key ^ ipad / ^ opad tables for every byte value.
local IPAD, OPAD = {}, {}
for v = 0, 255 do
  IPAD[v] = char(xor_byte(v, 0x36))
  OPAD[v] = char(xor_byte(v, 0x5c))
end

function M.sha256(key, msg)
  if #key > BLOCK then key = sha256.digest(key) end
  key = key .. string.rep("\0", BLOCK - #key)
  local ik, ok = {}, {}
  for i = 1, BLOCK do
    local b = byte(key, i)
    ik[i], ok[i] = IPAD[b], OPAD[b]
  end
  local inner = sha256.digest(table.concat(ik) .. msg)
  return sha256.digest(table.concat(ok) .. inner)
end

function M.sha256_hex(key, msg)
  return sha256.tohex(M.sha256(key, msg))
end

-- Constant-time equality: accumulates differences over the full length of
-- `a` without an early exit. Different lengths compare false.
function M.equal(a, b)
  if type(a) ~= "string" or type(b) ~= "string" then return false end
  local acc = (#a == #b) and 0 or 1
  local nb = #b
  for i = 1, #a do
    local x = byte(a, i)
    local y = (nb > 0) and byte(b, ((i - 1) % nb) + 1) or 256
    acc = acc + (x - y) * (x - y)
  end
  return acc == 0
end

return M
