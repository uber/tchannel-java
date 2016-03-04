#!/bin/sh
set -e
set -x

mkdir -p "$THRIFT_PREFIX"

wget http://archive.apache.org/dist/thrift/0.9.2/thrift-0.9.2.tar.gz
tar -xzvf thrift-0.9.2.tar.gz
cd thrift-0.9.2
./configure --prefix="$THRIFT_PREFIX" --enable-libs=no --enable-tests=no --enable-tutorial=no
make -j2 && make install
