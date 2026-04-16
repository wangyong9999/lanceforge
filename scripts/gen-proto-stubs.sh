#!/bin/bash
# Regenerate Python proto stubs for SDK and E2E tools.
# Run this after modifying crates/proto/proto/lance_service.proto.
set -e

PROTO=crates/proto/proto/lance_service.proto
SDK=lance-integration/sdk/python/lanceforge
TOOLS=lance-integration/tools

echo "Generating Python proto stubs from $PROTO ..."

python3 -m grpc_tools.protoc \
  -I crates/proto/proto \
  --python_out="$SDK" \
  --grpc_python_out="$SDK" \
  "$PROTO"

# Fix the generated grpc import for package-relative import
sed -i 's/^import lance_service_pb2 as/from lanceforge import lance_service_pb2 as/' \
  "$SDK/lance_service_pb2_grpc.py"

# Also generate into tools/ for E2E tests (they use PYTHONPATH=tools)
python3 -m grpc_tools.protoc \
  -I crates/proto/proto \
  --python_out="$TOOLS" \
  --grpc_python_out="$TOOLS" \
  "$PROTO"

echo "Done. Stubs generated in:"
echo "  $SDK/lance_service_pb2.py"
echo "  $SDK/lance_service_pb2_grpc.py"
echo "  $TOOLS/lance_service_pb2.py"
echo "  $TOOLS/lance_service_pb2_grpc.py"
