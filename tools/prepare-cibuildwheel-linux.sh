#!/bin/bash
#

# cibuildwheel builder for Linux

LIBRDKAFKA_VERSION=$1

if [[ -z $LIBRDKAFKA_VERSION ]]; then
    if [[ -z $RDKAFKA_SOURCE_DIR ]]; then
        echo "Usage: $0 <librdkafka-version/tag/gitref>"
        exit 1
    fi
    LIBRDKAFKA_VERSION=local
fi

set -ex

echo "# Installing basic system dependencies"
yum install -y zlib-devel openssl-devel gcc-c++

if [[ -n $RDKAFKA_SOURCE_DIR ]]; then
    if ! which cmake >/dev/null 2>&1; then
        yum install -y cmake
    fi
    if [[ $RDKAFKA_SOURCE_DIR != /* ]]; then
        RDKAFKA_SOURCE_DIR="$PWD/$RDKAFKA_SOURCE_DIR"
    fi
    echo "# Building librdkafka from local source: $RDKAFKA_SOURCE_DIR"
    builddir=/tmp/rdkafka-build
    cmake -S "$RDKAFKA_SOURCE_DIR" -B "$builddir" \
        -DRDKAFKA_BUILD_CPP=OFF -DBUILD_SHARED_LIBS=ON
    cmake --build "$builddir"
    cmake --install "$builddir" --prefix /usr
    exit 0
fi

echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
$(dirname $0)/bootstrap-librdkafka.sh --require-ssl ${LIBRDKAFKA_VERSION} /usr
