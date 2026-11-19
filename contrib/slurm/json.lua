-- SPDX-License-Identifier: Apache-2.0
-- Copyright the rightsizer authors.
--
-- Small JSON encoder/decoder for the rightsizer Slurm plugins (JSON-RPC).
-- json.null is the sentinel for JSON null. Tables with keys 1..n (or the
-- json.array marker metatable) encode as arrays; empty tables encode as {}
-- unless marked with json.as_array(t).

local M = {}
M.null = setmetatable({}, { __tostring = function() return "null" end })
local array_mt = {}
function M.as_array(t) return setmetatable(t or {}, array_mt) end

local escapes = { ['"'] = '\\"', ['\\'] = '\\\\', ['\b'] = '\\b', ['\f'] = '\\f', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }

local function encode_string(s)
  return '"' .. s:gsub('[%c"\\]', function(c)
    return escapes[c] or string.format("\\u%04x", c:byte())
  end) .. '"'
end

local function is_array(t)
  if getmetatable(t) == array_mt then return true end
  local n = 0
  for k in pairs(t) do
    if type(k) ~= "number" or k < 1 or k % 1 ~= 0 then return false end
    n = n + 1
  end
  if n == 0 then return false end
  for i = 1, n do if t[i] == nil then return false end end
  return true
end

local function encode(v, out)
  local t = type(v)
  if v == nil or v == M.null then
    out[#out + 1] = "null"
  elseif t == "boolean" then
    out[#out + 1] = v and "true" or "false"
  elseif t == "number" then
    if v ~= v or v == math.huge or v == -math.huge then error("json: cannot encode " .. tostring(v)) end
    if v % 1 == 0 and v > -2^53 and v < 2^53 then
      out[#out + 1] = string.format("%.0f", v)
    else
      out[#out + 1] = string.format("%.17g", v)
    end
  elseif t == "string" then
    out[#out + 1] = encode_string(v)
  elseif t == "table" then
    if is_array(v) then
      out[#out + 1] = "["
      for i = 1, #v do
        if i > 1 then out[#out + 1] = "," end
        encode(v[i], out)
      end
      out[#out + 1] = "]"
    else
      local keys = {}
      for k in pairs(v) do
        if type(k) ~= "string" then error("json: object keys must be strings") end
        keys[#keys + 1] = k
      end
      table.sort(keys)
      out[#out + 1] = "{"
      for i, k in ipairs(keys) do
        if i > 1 then out[#out + 1] = "," end
        out[#out + 1] = encode_string(k)
        out[#out + 1] = ":"
        encode(v[k], out)
      end
      out[#out + 1] = "}"
    end
  else
    error("json: cannot encode " .. t)
  end
end

function M.encode(v)
  local out = {}
  encode(v, out)
  return table.concat(out)
end

local function utf8_char(c)
  if c < 0x80 then return string.char(c) end
  if c < 0x800 then return string.char(0xC0 + math.floor(c / 64), 0x80 + c % 64) end
  if c < 0x10000 then
    return string.char(0xE0 + math.floor(c / 4096), 0x80 + math.floor(c / 64) % 64, 0x80 + c % 64)
  end
  return string.char(0xF0 + math.floor(c / 262144), 0x80 + math.floor(c / 4096) % 64,
    0x80 + math.floor(c / 64) % 64, 0x80 + c % 64)
end

function M.decode(s)
  if type(s) ~= "string" then error("json: expected a string") end
  local pos = 1
  local function fail(msg) error("json: " .. msg .. " at position " .. pos, 0) end
  local function ws()
    pos = s:find("[^ \t\r\n]", pos) or (#s + 1)
  end
  local value
  local function str()
    pos = pos + 1
    local buf = {}
    while true do
      local c = s:sub(pos, pos)
      if c == "" then fail("unterminated string") end
      if c == '"' then pos = pos + 1; break end
      if c == "\\" then
        local e = s:sub(pos + 1, pos + 1)
        local map = { ['"'] = '"', ['\\'] = '\\', ['/'] = '/', b = '\b', f = '\f', n = '\n', r = '\r', t = '\t' }
        if map[e] then
          buf[#buf + 1] = map[e]; pos = pos + 2
        elseif e == "u" then
          local h = s:sub(pos + 2, pos + 5)
          if not h:match("^%x%x%x%x$") then fail("bad \\u escape") end
          local c1 = tonumber(h, 16); pos = pos + 6
          if c1 >= 0xD800 and c1 <= 0xDBFF then
            local h2 = s:match("^\\u(%x%x%x%x)", pos)
            local c2 = h2 and tonumber(h2, 16)
            if not c2 or c2 < 0xDC00 or c2 > 0xDFFF then fail("unpaired surrogate") end
            c1 = 0x10000 + (c1 - 0xD800) * 1024 + (c2 - 0xDC00); pos = pos + 6
          elseif c1 >= 0xDC00 and c1 <= 0xDFFF then
            fail("unpaired surrogate")
          end
          buf[#buf + 1] = utf8_char(c1)
        else
          fail("bad escape")
        end
      elseif c:byte() < 32 then
        fail("control character in string")
      else
        local j = s:find('[\\"%c]', pos) or (#s + 1)
        buf[#buf + 1] = s:sub(pos, j - 1); pos = j
      end
    end
    return table.concat(buf)
  end
  function value()
    ws()
    local c = s:sub(pos, pos)
    if c == "{" then
      pos = pos + 1
      local t = {}
      ws()
      if s:sub(pos, pos) == "}" then pos = pos + 1; return t end
      while true do
        ws()
        if s:sub(pos, pos) ~= '"' then fail("expected object key") end
        local k = str()
        ws()
        if s:sub(pos, pos) ~= ":" then fail("expected ':'") end
        pos = pos + 1
        t[k] = value()
        ws()
        local d = s:sub(pos, pos); pos = pos + 1
        if d == "}" then return t end
        if d ~= "," then fail("expected ',' or '}'") end
      end
    elseif c == "[" then
      pos = pos + 1
      local t = M.as_array({})
      ws()
      if s:sub(pos, pos) == "]" then pos = pos + 1; return t end
      while true do
        t[#t + 1] = value()
        ws()
        local d = s:sub(pos, pos); pos = pos + 1
        if d == "]" then return t end
        if d ~= "," then fail("expected ',' or ']'") end
      end
    elseif c == '"' then
      return str()
    elseif s:sub(pos, pos + 3) == "true" then pos = pos + 4; return true
    elseif s:sub(pos, pos + 4) == "false" then pos = pos + 5; return false
    elseif s:sub(pos, pos + 3) == "null" then pos = pos + 4; return M.null
    else
      local num = s:match("^-?%d+%.?%d*[eE]?[-+]?%d*", pos)
      if not num or num == "" or num == "-" then fail("unexpected character") end
      local n = tonumber(num)
      if not n then fail("bad number") end
      pos = pos + #num
      return n
    end
  end
  local v = value()
  ws()
  if pos <= #s then fail("trailing data") end
  return v
end

return M
