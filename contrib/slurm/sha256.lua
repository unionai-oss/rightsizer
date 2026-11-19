-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- Pure-Lua SHA-256 (FIPS 180-4) for Slurm's Lua plugins. Works on Lua 5.1,
-- 5.2, 5.3, 5.4 and LuaJIT; the fastest backend is chosen at load time:
--   "native"  Lua >= 5.3 integer bit operators (compiled with load(), so the
--             file still parses on 5.1/LuaJIT)
--   "bit"     LuaJIT's bit library
--   "bit32"   Lua 5.2's bit32 library
--   "arith"   plain arithmetic with byte lookup tables (any Lua)
-- Set the global RIGHTSIZER_SHA256_BACKEND before require() to force one
-- (used by the tests).
--
-- API: sha256.digest(s) -> 32-byte binary string; sha256.hex(s) -> hex;
--      sha256.backend -> the backend in use.

local M = {}

local K = {
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}
local H0 = { 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19 }

local byte, char, floor = string.byte, string.char, math.floor
local MOD = 4294967296

-- Message padding: s .. 0x80 .. zeros .. 64-bit big-endian bit length.
local function pad(s)
  local n = #s
  local bits = n * 8
  local z = (55 - n) % 64
  local len = {}
  for i = 8, 1, -1 do
    len[i] = char(bits % 256)
    bits = floor(bits / 256)
  end
  return s .. "\128" .. string.rep("\0", z) .. table.concat(len)
end

local function be32(x)
  x = x % MOD
  return char(floor(x / 16777216) % 256, floor(x / 65536) % 256, floor(x / 256) % 256, x % 256)
end

-- Backend 1: native integer operators (Lua >= 5.3), one compiled chunk.
local native_src = [[
local K, H0, pad = ...
local unpack, pack = string.unpack, string.pack
local function digest(s)
  s = pad(s)
  local h1, h2, h3, h4, h5, h6, h7, h8 = H0[1], H0[2], H0[3], H0[4], H0[5], H0[6], H0[7], H0[8]
  local w = {}
  for off = 1, #s, 64 do
    for j = 1, 16 do w[j] = unpack(">I4", s, off + (j - 1) * 4) end
    for j = 17, 64 do
      local x, y = w[j - 15], w[j - 2]
      local s0 = (((x >> 7) | (x << 25)) ~ ((x >> 18) | (x << 14)) ~ (x >> 3)) & 0xffffffff
      local s1 = (((y >> 17) | (y << 15)) ~ ((y >> 19) | (y << 13)) ~ (y >> 10)) & 0xffffffff
      w[j] = (w[j - 16] + s0 + w[j - 7] + s1) & 0xffffffff
    end
    local a, b, c, d, e, f, g, h = h1, h2, h3, h4, h5, h6, h7, h8
    for j = 1, 64 do
      local S1 = (((e >> 6) | (e << 26)) ~ ((e >> 11) | (e << 21)) ~ ((e >> 25) | (e << 7))) & 0xffffffff
      local ch = (e & f) ~ (~e & g)
      local t1 = (h + S1 + ch + K[j] + w[j]) & 0xffffffff
      local S0 = (((a >> 2) | (a << 30)) ~ ((a >> 13) | (a << 19)) ~ ((a >> 22) | (a << 10))) & 0xffffffff
      local maj = (a & b) ~ (a & c) ~ (b & c)
      local t2 = (S0 + maj) & 0xffffffff
      h, g, f, e, d, c, b, a = g, f, e, (d + t1) & 0xffffffff, c, b, a, (t1 + t2) & 0xffffffff
    end
    h1 = (h1 + a) & 0xffffffff; h2 = (h2 + b) & 0xffffffff
    h3 = (h3 + c) & 0xffffffff; h4 = (h4 + d) & 0xffffffff
    h5 = (h5 + e) & 0xffffffff; h6 = (h6 + f) & 0xffffffff
    h7 = (h7 + g) & 0xffffffff; h8 = (h8 + h) & 0xffffffff
  end
  return pack(">I4I4I4I4I4I4I4I4", h1, h2, h3, h4, h5, h6, h7, h8)
end
return digest
]]

-- Backends 2-4: the same algorithm over band/bxor/bnot/rshift/lshift
-- functions that accept any number and return a value congruent mod 2^32.
local function generic(ops)
  local band, bxor, bnot, rshift, lshift = ops.band, ops.bxor, ops.bnot, ops.rshift, ops.lshift
  local function rotr(x, n)
    return bxor(rshift(x, n), lshift(x, 32 - n))
  end
  return function(s)
    s = pad(s)
    local H = { H0[1], H0[2], H0[3], H0[4], H0[5], H0[6], H0[7], H0[8] }
    local w = {}
    for off = 1, #s, 64 do
      for j = 1, 16 do
        local b1, b2, b3, b4 = byte(s, off + (j - 1) * 4, off + (j - 1) * 4 + 3)
        w[j] = ((b1 * 256 + b2) * 256 + b3) * 256 + b4
      end
      for j = 17, 64 do
        local x, y = w[j - 15], w[j - 2]
        local s0 = bxor(bxor(rotr(x, 7), rotr(x, 18)), rshift(x, 3))
        local s1 = bxor(bxor(rotr(y, 17), rotr(y, 19)), rshift(y, 10))
        w[j] = (w[j - 16] % MOD + s0 % MOD + w[j - 7] % MOD + s1 % MOD) % MOD
      end
      local a, b, c, d, e, f, g, h = H[1], H[2], H[3], H[4], H[5], H[6], H[7], H[8]
      for j = 1, 64 do
        local S1 = bxor(bxor(rotr(e, 6), rotr(e, 11)), rotr(e, 25))
        local ch = bxor(band(e, f), band(bnot(e), g))
        local t1 = (h + S1 % MOD + ch % MOD + K[j] + w[j]) % MOD
        local S0 = bxor(bxor(rotr(a, 2), rotr(a, 13)), rotr(a, 22))
        local maj = bxor(bxor(band(a, b), band(a, c)), band(b, c))
        local t2 = (S0 % MOD + maj % MOD) % MOD
        h, g, f, e, d, c, b, a = g, f, e, (d + t1) % MOD, c, b, a, (t1 + t2) % MOD
      end
      H[1] = (H[1] + a) % MOD; H[2] = (H[2] + b) % MOD
      H[3] = (H[3] + c) % MOD; H[4] = (H[4] + d) % MOD
      H[5] = (H[5] + e) % MOD; H[6] = (H[6] + f) % MOD
      H[7] = (H[7] + g) % MOD; H[8] = (H[8] + h) % MOD
    end
    local out = {}
    for i = 1, 8 do out[i] = be32(H[i]) end
    return table.concat(out)
  end
end

-- Arithmetic bit operations over bytes with 256x256 lookup tables.
local function arith_ops()
  local XOR, AND = {}, {}
  for a = 0, 255 do
    local xr, an = {}, {}
    for b = 0, 255 do
      local x, y, rx, ra, p = a, b, 0, 0, 1
      for _ = 1, 8 do
        local ba, bb = x % 2, y % 2
        if ba ~= bb then rx = rx + p end
        if ba == 1 and bb == 1 then ra = ra + p end
        x, y, p = (x - ba) / 2, (y - bb) / 2, p * 2
      end
      xr[b], an[b] = rx, ra
    end
    XOR[a], AND[a] = xr, an
  end
  local function op(T)
    return function(x, y)
      x, y = x % MOD, y % MOD
      local r, p = 0, 1
      for _ = 1, 4 do
        local a, b = x % 256, y % 256
        r = r + T[a][b] * p
        x, y, p = (x - a) / 256, (y - b) / 256, p * 256
      end
      return r
    end
  end
  return {
    band = op(AND),
    bxor = op(XOR),
    bnot = function(x) return MOD - 1 - x % MOD end,
    rshift = function(x, n) return floor((x % MOD) / 2 ^ n) end,
    lshift = function(x, n) return ((x % MOD) * 2 ^ n) % MOD end,
  }
end

local forced = rawget(_G, "RIGHTSIZER_SHA256_BACKEND")
local function try_native()
  if _VERSION == "Lua 5.1" or _VERSION == "Lua 5.2" then return nil end
  local loader = load or loadstring
  local ok, chunk = pcall(loader, native_src, "=sha256-native")
  if not ok or not chunk then return nil end
  local ok2, fn = pcall(chunk, K, H0, pad)
  if ok2 then return fn end
  return nil
end

local digest, backend
if (forced == nil or forced == "native") then
  digest = try_native()
  if digest then backend = "native" end
end
if not digest and (forced == nil or forced == "bit") then
  local ok, bit = pcall(require, "bit")
  if ok and type(bit) == "table" and bit.bxor then
    digest, backend = generic(bit), "bit"
  end
end
if not digest and (forced == nil or forced == "bit32") then
  local b32 = rawget(_G, "bit32")
  if not b32 then
    local ok, m = pcall(require, "bit32")
    if ok then b32 = m end
  end
  if type(b32) == "table" and b32.bxor then
    digest, backend = generic(b32), "bit32"
  end
end
if not digest then
  digest, backend = generic(arith_ops()), "arith"
end

M.backend = backend
M.digest = digest
function M.hex(s)
  return (digest(s):gsub(".", function(c) return string.format("%02x", byte(c)) end))
end
M.tohex = function(bin)
  return (bin:gsub(".", function(c) return string.format("%02x", byte(c)) end))
end

return M
