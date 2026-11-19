# Runtime image for the rsz binary built by tests/infra/build-linux.sh.
# Build context is tests/infra/.state/bin (contains only `rsz`).
ARG BASE=debian:bookworm-slim
FROM ${BASE}
RUN if command -v apt-get >/dev/null; then \
      apt-get update -qq && apt-get install -y -qq --no-install-recommends ca-certificates >/dev/null \
      && rm -rf /var/lib/apt/lists/*; fi
COPY rsz /usr/local/bin/rsz
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rsz"]
