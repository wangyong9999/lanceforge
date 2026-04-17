#!/bin/bash
# ╔══════════════════════════════════════════════════════════════╗
# ║  LanceForge Release Script                                  ║
# ║  One command to build ALL release artifacts:                 ║
# ║    tar.gz, .whl, .deb, Docker image, Helm chart             ║
# ║                                                              ║
# ║  Usage:                                                      ║
# ║    ./scripts/release.sh v0.2.0              # build only     ║
# ║    ./scripts/release.sh v0.2.0 --publish    # build + upload ║
# ╚══════════════════════════════════════════════════════════════╝
set -euo pipefail

VERSION="${1:?Usage: release.sh <version> [--publish]}"
PUBLISH="${2:-}"
TAG="$VERSION"
SEMVER="${VERSION#v}"  # v0.1.0 → 0.1.0

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
step() { echo -e "\n${BLUE}[$(date +%H:%M:%S)] $1${NC}"; }
ok()   { echo -e "${GREEN}  ✓ $1${NC}"; }
fail() { echo -e "${RED}  ✗ $1${NC}"; exit 1; }

echo -e "${BOLD}╔════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  LanceForge Release: ${TAG}${NC}"
echo -e "${BOLD}╚════════════════════════════════════════════╝${NC}"

cd "$ROOT"

# ── Pre-flight checks ──
step "1/8  Pre-flight checks"
command -v cargo   >/dev/null || fail "cargo not found"
command -v python3 >/dev/null || fail "python3 not found"
command -v strip   >/dev/null || fail "strip not found"
command -v dpkg-deb >/dev/null || echo "  (dpkg-deb not found — skipping .deb)"
# Mirror ci.yml gate: deny warnings on the 6 owned crates.
cargo clippy --lib --tests \
    -p lance-distributed-common -p lance-distributed-proto \
    -p lance-distributed-meta -p lance-distributed-coordinator \
    -p lance-distributed-worker -p lanceforge \
    -- -D warnings >/dev/null 2>&1 || fail "clippy found warnings"
ok "tools ready, clippy clean"

# ── Tests ──
step "2/8  Running tests"
cargo test --lib \
    -p lance-distributed-common -p lance-distributed-proto \
    -p lance-distributed-meta -p lance-distributed-coordinator \
    -p lance-distributed-worker 2>&1 | tail -1
ok "all tests passed"

# ── Build release-lto binaries (matches release.yml on GitHub) ──
step "3/8  Building release-lto binaries (LTO enabled — ~3× slower)"
cargo build --profile release-lto \
    -p lance-distributed-coordinator -p lance-distributed-worker 2>&1 | tail -1
ok "binaries built"

# ── Prepare dist/ ──
rm -rf "$DIST"
mkdir -p "$DIST"

# ── tar.gz ──
step "4/8  Packaging tar.gz"
cp target/release-lto/lance-coordinator "$DIST/"
cp target/release-lto/lance-worker "$DIST/"
strip "$DIST/lance-coordinator" "$DIST/lance-worker"
tar czf "$DIST/lanceforge-${TAG}-${OS}-${ARCH}.tar.gz" \
    -C "$DIST" lance-coordinator lance-worker
ok "$(ls -lh "$DIST/lanceforge-${TAG}-${OS}-${ARCH}.tar.gz" | awk '{print $5}')"

# ── Python wheel ──
step "5/8  Building Python wheel"
# Regenerate proto stubs into SDK
bash scripts/gen-proto-stubs.sh 2>/dev/null
# Update version in setup.py
sed -i "s/version=\"[^\"]*\"/version=\"${SEMVER}\"/" lance-integration/sdk/python/setup.py
(cd lance-integration/sdk/python && python3 setup.py bdist_wheel -d "$DIST" 2>&1 | tail -1)
ok "$(ls "$DIST"/*.whl)"

# ── .deb package ──
step "6/8  Building .deb package"
if command -v dpkg-deb >/dev/null; then
    DEB_ROOT="$DIST/deb-staging/lanceforge_${SEMVER}_amd64"
    mkdir -p "$DEB_ROOT/DEBIAN" "$DEB_ROOT/usr/bin" "$DEB_ROOT/etc/lanceforge" \
             "$DEB_ROOT/lib/systemd/system" "$DEB_ROOT/usr/share/doc/lanceforge"

    # Control
    cat > "$DEB_ROOT/DEBIAN/control" <<CTRL
Package: lanceforge
Version: ${SEMVER}
Section: database
Priority: optional
Architecture: amd64
Depends: libssl3 (>= 3.0), ca-certificates
Maintainer: LanceForge Team <ws1084025704@gmail.com>
Homepage: https://github.com/wangyong9999/lanceforge
Description: Distributed multimodal retrieval engine for Lance tables
 Coordinator and worker server binaries for LanceForge.
CTRL

    # Binaries
    cp "$DIST/lance-coordinator" "$DEB_ROOT/usr/bin/"
    cp "$DIST/lance-worker" "$DEB_ROOT/usr/bin/"

    # Config
    cp dist/deb/lanceforge_0.1.0_amd64/etc/lanceforge/config.yaml \
       "$DEB_ROOT/etc/lanceforge/" 2>/dev/null || \
    cat > "$DEB_ROOT/etc/lanceforge/config.yaml" <<'YAML'
executors:
  - id: worker_0
    host: 127.0.0.1
    port: 50100
tables: []
default_table_path: /var/lib/lanceforge/tables
metadata_path: /var/lib/lanceforge/metadata.json
YAML
    echo "/etc/lanceforge/config.yaml" > "$DEB_ROOT/DEBIAN/conffiles"

    # systemd
    cp "$ROOT/dist/deb/lanceforge_0.1.0_amd64/lib/systemd/system/"*.service \
       "$DEB_ROOT/lib/systemd/system/" 2>/dev/null || true

    # postinst
    cat > "$DEB_ROOT/DEBIAN/postinst" <<'SCRIPT'
#!/bin/bash
set -e
id -u lanceforge >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin lanceforge
mkdir -p /var/lib/lanceforge/tables
chown -R lanceforge:lanceforge /var/lib/lanceforge
systemctl daemon-reload
echo "LanceForge installed. Run: systemctl start lance-coordinator"
SCRIPT
    chmod 755 "$DEB_ROOT/DEBIAN/postinst"

    # prerm
    cat > "$DEB_ROOT/DEBIAN/prerm" <<'SCRIPT'
#!/bin/bash
systemctl stop lance-coordinator 2>/dev/null || true
systemctl stop 'lance-worker@*' 2>/dev/null || true
SCRIPT
    chmod 755 "$DEB_ROOT/DEBIAN/prerm"

    # Copyright
    mkdir -p "$DEB_ROOT/usr/share/doc/lanceforge"
    echo "Apache-2.0" > "$DEB_ROOT/usr/share/doc/lanceforge/copyright"

    dpkg-deb --build "$DEB_ROOT" "$DIST/lanceforge_${SEMVER}_amd64.deb" 2>&1
    ok "$(ls -lh "$DIST/lanceforge_${SEMVER}_amd64.deb" | awk '{print $5}')"
    rm -rf "$DIST/deb-staging"
else
    echo "  (skipped — dpkg-deb not available)"
fi

# ── Helm chart ──
step "7/8  Packaging Helm chart"
if command -v helm >/dev/null; then
    sed -i "s/^version:.*/version: ${SEMVER}/" deploy/helm/lanceforge/Chart.yaml
    sed -i "s/^appVersion:.*/appVersion: \"${TAG}\"/" deploy/helm/lanceforge/Chart.yaml
    helm package deploy/helm/lanceforge/ -d "$DIST" 2>&1
    ok "$(ls "$DIST"/lanceforge-*.tgz 2>/dev/null)"
else
    # Fallback: tar the chart directory
    tar czf "$DIST/lanceforge-chart-${SEMVER}.tgz" -C deploy/helm lanceforge
    ok "chart tarball (helm not installed, using tar)"
fi

# ── Checksums ──
step "8/8  Generating checksums"
(cd "$DIST" && sha256sum *.tar.gz *.whl *.deb *.tgz 2>/dev/null > SHA256SUMS.txt)
cat "$DIST/SHA256SUMS.txt"
ok "SHA256SUMS.txt"

# ── Clean up loose binaries from dist/ ──
rm -f "$DIST/lance-coordinator" "$DIST/lance-worker"

# ── Summary ──
echo ""
echo -e "${BOLD}╔════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  Release artifacts in dist/:               ║${NC}"
echo -e "${BOLD}╚════════════════════════════════════════════╝${NC}"
ls -lh "$DIST"/ | grep -v "^total\|deb-staging"
echo ""

# ── Publish ──
if [ "$PUBLISH" = "--publish" ]; then
    step "Publishing to GitHub"
    git tag -a "$TAG" -m "LanceForge ${TAG}" 2>/dev/null || echo "  tag $TAG already exists"
    git push lanceforge "$TAG" 2>/dev/null || echo "  tag already pushed"

    # Create release
    ASSETS=()
    for f in "$DIST"/*.tar.gz "$DIST"/*.whl "$DIST"/*.deb "$DIST"/*.tgz "$DIST"/SHA256SUMS.txt; do
        [ -f "$f" ] && ASSETS+=("$f")
    done

    ~/.local/bin/gh release create "$TAG" "${ASSETS[@]}" \
        --repo wangyong9999/lanceforge \
        --title "LanceForge ${TAG}" \
        --generate-notes \
        2>&1 || echo "  release already exists, uploading assets"

    # Upload any missing assets to existing release
    for f in "${ASSETS[@]}"; do
        ~/.local/bin/gh release upload "$TAG" "$f" --repo wangyong9999/lanceforge 2>/dev/null || true
    done

    ok "Published to https://github.com/wangyong9999/lanceforge/releases/tag/${TAG}"
else
    echo -e "  To publish: ${BOLD}./scripts/release.sh ${TAG} --publish${NC}"
fi
