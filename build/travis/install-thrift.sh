#!/bin/sh
set -e
set -x

THRIFTDIR="$HOME/thrift"
mkdir -p "$THRIFTDIR"

wget http://archive.apache.org/dist/thrift/0.9.2/thrift-0.9.2.tar.gz
tar -xzvf thrift-0.9.2.tar.gz
cd thrift-0.9.2
./configure \
	--prefix="$THRIFTDIR" --with-cpp=no --with-c_glib=no --with-java=yes \
	--with-ruby=no --with-erlang=no --with-go=no --with-nodejs=no
make -j2 && make install
