# rightsizer-runtime

Pure-Python half of rightsizer: the invocation policy model, in-task
measurement (cgroup v1/v2, `getrusage`, process RSS, NVML) and the
`RunRecord` writer. It imports with the standard library only; `fsspec`
and `pynvml` are optional extras imported lazily.

```
cd python/rightsizer-runtime
uv run --python 3.12 --with pytest --with fsspec pytest -q
```
