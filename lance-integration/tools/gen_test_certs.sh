#!/bin/bash
# Generate self-signed certificates for TLS testing.
# Usage: ./gen_test_certs.sh <output_dir>
#
# Creates:
#   ca.pem / ca-key.pem    — CA certificate and key
#   server.pem / server-key.pem — Server cert signed by CA (SAN: localhost, 127.0.0.1)

set -euo pipefail

OUT="${1:-/tmp/lanceforge_tls_certs}"
mkdir -p "$OUT"

# CA
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 \
    -keyout "$OUT/ca-key.pem" -out "$OUT/ca.pem" \
    -nodes -subj "/CN=LanceForge Test CA" 2>/dev/null

# Server key + CSR
openssl req -newkey rsa:2048 -sha256 \
    -keyout "$OUT/server-key.pem" -out "$OUT/server.csr" \
    -nodes -subj "/CN=localhost" 2>/dev/null

# Sign server cert with CA (with SAN for localhost + 127.0.0.1)
openssl x509 -req -in "$OUT/server.csr" \
    -CA "$OUT/ca.pem" -CAkey "$OUT/ca-key.pem" -CAcreateserial \
    -out "$OUT/server.pem" -days 365 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null

rm -f "$OUT/server.csr" "$OUT/ca.srl"

echo "Certificates generated in $OUT"
ls -la "$OUT"/*.pem
