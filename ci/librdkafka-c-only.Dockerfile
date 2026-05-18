ARG BASE_IMAGE=quay.io/pypa/manylinux2014_x86_64:latest
ARG BUILDER_IMAGE=docker-reg.devops.xiaohongshu.com/cpp-infra/build_env:brpc_v20250728

FROM ${BUILDER_IMAGE} AS librdkafka-builder

ARG LIBRDKAFKA_COMMIT=unknown
ARG LIBRDKAFKA_PREFIX=/opt/librdkafka
ARG INSTALL_OS_DEPS=1

COPY librdkafka-src.tar /tmp/librdkafka-src.tar

RUN set -eux; \
    if [ "${INSTALL_OS_DEPS}" = "0" ]; then \
        echo "Skipping builder OS dependency installation"; \
    elif command -v apk >/dev/null 2>&1; then \
        apk add --no-cache \
            bash \
            gcc \
            g++ \
            make \
            cmake \
            pkgconf \
            zlib-dev \
            zstd-dev \
            openssl-dev; \
    elif command -v yum >/dev/null 2>&1; then \
        if [ ! -f /etc/yum.repos.d/epel-aliyun.repo ]; then \
            printf '%s\n' \
                '[epel-aliyun]' \
                'name=EPEL 7 aliyun' \
                'baseurl=http://mirrors.aliyun.com/epel/7/x86_64/' \
                'enabled=1' \
                'gpgcheck=0' \
                >/etc/yum.repos.d/epel-aliyun.repo; \
        fi; \
        yum_pkgs="\
            gcc \
            gcc-c++ \
            make \
            pkgconfig \
            zlib-devel \
            libzstd-devel \
            openssl-devel"; \
        if ! command -v cmake >/dev/null 2>&1; then yum_pkgs="${yum_pkgs} cmake"; fi; \
        yum --disableplugin=fastestmirror install -y ${yum_pkgs}; \
        yum clean all || true; \
    elif command -v apt-get >/dev/null 2>&1; then \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            build-essential \
            ca-certificates \
            cmake \
            pkg-config \
            zlib1g-dev \
            libzstd-dev \
            libssl-dev; \
        rm -rf /var/lib/apt/lists/*; \
    else \
        echo "unsupported builder image: no apk, yum, or apt-get found" >&2; \
        exit 1; \
    fi; \
    command -v cmake; \
    command -v gcc; \
    mkdir -p /tmp/librdkafka-src /tmp/librdkafka-build "${LIBRDKAFKA_PREFIX}"; \
    tar -xf /tmp/librdkafka-src.tar -C /tmp/librdkafka-src; \
    cmake -S /tmp/librdkafka-src -B /tmp/librdkafka-build \
        -DRDKAFKA_BUILD_CPP=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5; \
    cmake --build /tmp/librdkafka-build --target rdkafka -- -j"$(nproc)"; \
    cmake --install /tmp/librdkafka-build --prefix "${LIBRDKAFKA_PREFIX}"; \
    test -f "${LIBRDKAFKA_PREFIX}/include/librdkafka/rdkafka.h"; \
    { test -f "${LIBRDKAFKA_PREFIX}/lib/librdkafka.so" || test -f "${LIBRDKAFKA_PREFIX}/lib64/librdkafka.so"; }; \
    rm -rf /tmp/librdkafka-src /tmp/librdkafka-build /tmp/librdkafka-src.tar

FROM ${BASE_IMAGE}

ARG LIBRDKAFKA_COMMIT=unknown
ARG LIBRDKAFKA_PREFIX=/opt/librdkafka

ENV LIBRDKAFKA_COMMIT=${LIBRDKAFKA_COMMIT}
ENV LIBRDKAFKA_PREFIX=${LIBRDKAFKA_PREFIX}
ENV C_INCLUDE_PATH=${LIBRDKAFKA_PREFIX}/include
ENV LIBRARY_PATH=${LIBRDKAFKA_PREFIX}/lib:${LIBRDKAFKA_PREFIX}/lib64
ENV LD_LIBRARY_PATH=${LIBRDKAFKA_PREFIX}/lib:${LIBRDKAFKA_PREFIX}/lib64
ENV PKG_CONFIG_PATH=${LIBRDKAFKA_PREFIX}/lib/pkgconfig:${LIBRDKAFKA_PREFIX}/lib64/pkgconfig

COPY --from=librdkafka-builder ${LIBRDKAFKA_PREFIX} ${LIBRDKAFKA_PREFIX}
COPY --from=librdkafka-builder /usr/lib64/libzstd.so.1* ${LIBRDKAFKA_PREFIX}/lib/

RUN set -eux; \
    { command -v python3 || test -x /opt/python/cp311-cp311/bin/python; }; \
    command -v gcc; \
    test -f "${LIBRDKAFKA_PREFIX}/include/librdkafka/rdkafka.h"; \
    { test -f "${LIBRDKAFKA_PREFIX}/lib/librdkafka.so" || test -f "${LIBRDKAFKA_PREFIX}/lib64/librdkafka.so"; }; \
    ldd "${LIBRDKAFKA_PREFIX}/lib/librdkafka.so"; \
    if command -v ldconfig >/dev/null 2>&1; then ldconfig; fi
