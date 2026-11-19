# Slim Slurm image for rightsizer integration tests (arm64 or amd64).
# Derived from giovtorres/slurm-docker-cluster (MIT) but built from the SchedMD
# git tag with ./configure (download.schedmd.com is not always reachable) and
# without apptainer/lmod/spack/gosu.
#
# Built with: Lua (job_submit/lua, cli_filter/lua), MySQL accounting
# (slurmdbd), cgroup/v2 (dbus-devel), JSON serializer (sacct --json).
ARG BASE=rockylinux/rockylinux:9

FROM ${BASE} AS builder
ARG SLURM_TAG
RUN set -ex \
 && dnf -y install dnf-plugins-core epel-release \
 && dnf config-manager --set-enabled crb \
 && dnf -y install --exclude='*.i686' \
      bzip2 dbus-devel gcc gcc-c++ hwloc-devel http-parser-devel json-c-devel \
      libyaml-devel lua-devel lz4-devel make mariadb-devel munge-devel \
      openssl-devel pam-devel perl python3 readline-devel wget \
 && dnf clean all
RUN set -ex \
 && wget -q -O /tmp/slurm.tgz "https://github.com/SchedMD/slurm/archive/refs/tags/${SLURM_TAG}.tar.gz" \
 && mkdir /src && tar -xzf /tmp/slurm.tgz -C /src --strip-components=1 \
 && cd /src \
 && ./configure --prefix=/usr --libdir=/usr/lib64 --sysconfdir=/etc/slurm \
      --with-lua --with-mysql_config=/usr/bin \
 && make -j"$(nproc)" >/dev/null \
 && make install DESTDIR=/staging >/dev/null \
 && rm -rf /staging/usr/share/doc /staging/usr/share/man /staging/usr/include \
 && ls /staging/usr/lib64/slurm | grep -E 'lua|cgroup_v2|mysql|serializer_json|jobacct_gather_cgroup|proctrack_cgroup|task_cgroup'

FROM ${BASE}
ARG SLURM_TAG
LABEL org.opencontainers.image.title="rsz-it slurm" org.opencontainers.image.version="${SLURM_TAG}"
RUN set -ex \
 && dnf -y install dnf-plugins-core epel-release \
 && dnf config-manager --set-enabled crb \
 && dnf -y install --exclude='*.i686' \
      dbus-libs hwloc-libs http-parser jq json-c libyaml lua lz4 mariadb-connector-c \
      munge procps-ng psmisc python3 readline util-linux \
 && dnf clean all && rm -rf /var/cache/dnf
COPY --from=builder /staging/ /
RUN set -ex \
 && groupadd -r -g 990 slurm && useradd -r -g slurm -u 990 -d /var/lib/slurm slurm \
 && useradd -m -u 1000 alice \
 && /sbin/create-munge-key -f \
 && mkdir -p /var/run/slurm /var/spool/slurm /var/lib/slurm /var/log/slurm /etc/slurm /etc/slurm-extra /data \
 && chown slurm:slurm /var/run/slurm /var/spool/slurm /var/lib/slurm /var/log/slurm \
 && chmod 1777 /data \
 && echo 'export PATH=/opt/rsz/bin:$PATH' >/etc/profile.d/rsz.sh
COPY etc/ /etc/slurm/
RUN chown slurm:slurm /etc/slurm/slurmdbd.conf && chmod 600 /etc/slurm/slurmdbd.conf \
 && chmod 644 /etc/slurm/slurm.conf /etc/slurm/cgroup.conf
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
ENV PATH=/opt/rsz/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
