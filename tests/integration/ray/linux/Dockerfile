# Linux Ray head for the memory-monitor OOM test (the monitor is Linux-only).
# Image: rsz-it/ray-linux:2.60.0 (tests/integration/ray/linux/run.sh).
FROM python:3.12-slim
RUN pip install --no-cache-dir "ray[default]==2.60.0" && rm -rf /root/.cache
ENV RAY_USAGE_STATS_ENABLED=0 PYTHONUNBUFFERED=1
WORKDIR /work
