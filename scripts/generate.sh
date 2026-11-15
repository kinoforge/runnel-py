#!/bin/sh
set -eu
version="${RUNNEL_PROTO_VERSION:-v0.0.4-beta.1}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git -c advice.detachedHead=false clone -q --depth 1 --branch "$version" https://github.com/kinoforge/runnel "$work/runnel"
find src/runnel/v1 -name '*_pb2*' -delete
python -m grpc_tools.protoc -I "$work/runnel/proto" \
  --python_out=src --grpc_python_out=src --mypy_out=src --mypy_grpc_out=src \
  "$work"/runnel/proto/runnel/v1/*.proto
