#!/bin/sh
set -e
set -x

THRIFT_VER=0.9.2
mkdir -p "$THRIFT_PREFIX"

wget http://archive.apache.org/dist/thrift/${THRIFT_VER}/thrift-${THRIFT_VER}.tar.gz
tar -xzvf thrift-${THRIFT_VER}.tar.gz
cd thrift-${THRIFT_VER}
./configure --prefix="$THRIFT_PREFIX" --enable-libs=no --enable-tests=no --enable-tutorial=no
make -j2 && make install
