local H = require("helper")
local backends = { "native", "bit", "bit32", "arith" }
local vectors = {
  { "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" },
  { "abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" },
  { "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1" },
  { "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu",
    "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1" },
  { string.rep("a", 55), "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318" },
  { string.rep("a", 56), "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a" },
  { string.rep("a", 64), "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb" },
}
local loaded = {}
for _, b in ipairs(backends) do
  package.loaded.sha256 = nil
  RIGHTSIZER_SHA256_BACKEND = b
  local s = require("sha256")
  if s.backend == b then
    loaded[#loaded + 1] = b
    for i, v in ipairs(vectors) do H.eq(s.hex(v[1]), v[2], b .. " vector " .. i) end
    H.eq(#s.digest("abc"), 32, b .. " digest length")
  end
end
package.loaded.sha256 = nil
RIGHTSIZER_SHA256_BACKEND = nil
local s = require("sha256")
H.ok(s.backend ~= "arith" or (_VERSION == "Lua 5.1" and not jit), "fast backend chosen: " .. s.backend)
local t0 = os.clock()
H.eq(s.hex(string.rep("a", 1000000)), "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0", "million a")
local dt = os.clock() - t0
H.ok(dt < 5, string.format("1 MB in %.2fs", dt))
print("  sha256 backends tested: " .. table.concat(loaded, ",") .. string.format("; default %s, 1 MB in %.3fs", s.backend, dt))
H.done("sha256")
