"""Generates crates/rightsizer-features/tests/fixtures (pyarrow 26)."""
import sys, os, datetime
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.ipc as ipc
out = sys.argv[1]
n = 200
t = pa.table({
    "id": pa.array(range(n), pa.int64()),
    "score": pa.array([i * 0.5 for i in range(n)], pa.float64()),
    "name": pa.array([f"user{i}" for i in range(n)], pa.string()),
    "tags": pa.array([[i, i + 1] for i in range(n)], pa.list_(pa.int32())),
    "meta": pa.array([{"a": i, "b": str(i)} for i in range(n)]),
    "ts": pa.array([datetime.datetime(2026, 1, 1)] * n, pa.timestamp("us")),
    "ok": pa.array([i % 2 == 0 for i in range(n)], pa.bool_()),
    "cat": pa.array([f"c{i%3}" for i in range(n)]).dictionary_encode(),
})
pq.write_table(t, os.path.join(out, "pyarrow.parquet"), row_group_size=50)
with ipc.new_file(os.path.join(out, "table.arrow"), t.schema) as w:
    w.write_table(t)
with ipc.new_stream(os.path.join(out, "table.arrows"), t.schema) as w:
    w.write_table(t)
