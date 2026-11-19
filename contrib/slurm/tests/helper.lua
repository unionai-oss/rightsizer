-- Minimal test harness for the rightsizer Slurm Lua modules.
package.path = "../?.lua;./?.lua;" .. package.path
local H = { passed = 0, failed = 0 }
function H.eq(got, want, name)
  if got == want then
    H.passed = H.passed + 1
  else
    H.failed = H.failed + 1
    io.stderr:write(string.format("FAIL %s\n  got:  %s\n  want: %s\n", name, tostring(got), tostring(want)))
  end
end
function H.ok(cond, name) H.eq(not not cond, true, name) end
function H.read(path)
  local f = assert(io.open(path, "rb")); local s = f:read("*a"); f:close(); return s
end
function H.done(suite)
  print(string.format("%-16s %-10s %3d passed, %d failed", suite, _VERSION .. (jit and "/JIT" or ""), H.passed, H.failed))
  os.exit(H.failed == 0 and 0 or 1)
end
return H
