local H = require("helper")
local hmac = require("hmac")
local function unhex(h) return (h:gsub("..", function(x) return string.char(tonumber(x, 16)) end)) end
-- RFC 4231 test cases 1-7 (HMAC-SHA-256).
local cases = {
  { string.rep("\11", 20), "Hi There", "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7" },
  { "Jefe", "what do ya want for nothing?", "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" },
  { string.rep("\170", 20), string.rep("\221", 50), "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe" },
  { unhex("0102030405060708090a0b0c0d0e0f10111213141516171819"), string.rep("\205", 50), "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b" },
  { string.rep("\12", 20), "Test With Truncation", "a3b6167473100ee06e0c796c2955552b", 16 },
  { string.rep("\170", 131), "Test Using Larger Than Block-Size Key - Hash Key First", "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54" },
  { string.rep("\170", 131), "This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.",
    "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2" },
}
for i, c in ipairs(cases) do
  local h = hmac.sha256_hex(c[1], c[2])
  if c[4] then h = h:sub(1, c[4] * 2) end
  H.eq(h, c[3], "RFC 4231 case " .. i)
end
H.ok(hmac.equal("abc", "abc"), "equal same")
H.ok(not hmac.equal("abc", "abd"), "equal differs")
H.ok(not hmac.equal("abc", "ab"), "equal length")
H.ok(not hmac.equal("", "a"), "equal empty vs one")
H.ok(hmac.equal("", ""), "equal empty")
H.ok(not hmac.equal("abc", nil), "equal nil")
H.done("hmac")
