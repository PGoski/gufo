#!/usr/bin/env bash
# Build the Gufo production container image (Linux x86_64, gfx1151) and
# extract standalone binaries into dist/.
#
# Prerequisites on an AMD Strix Halo machine: docker (or DOCKER=podman),
# network access to cache.nixos.org, ~25 GB free disk.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER="${DOCKER:-docker}"
IMAGE_NAME="${GUFO_IMAGE_NAME:-gufo}"
REV="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
IMAGE="${IMAGE_NAME}:${REV}"

command -v "$DOCKER" >/dev/null || { echo "docker not found (set DOCKER=...)" >&2; exit 1; }
if [ "$(uname -m)" != "x86_64" ]; then
  echo "WARNING: host is $(uname -m); Gufo is x86_64-only and this build needs" >&2
  echo "linux/amd64 emulation (qemu) unless run on an AMD Strix Halo machine." >&2
fi

echo "==> Building production image ${IMAGE} via nix build .#release"
"$DOCKER" build --platform linux/amd64 -f "$ROOT/.devops/docker/Dockerfile" -t "$IMAGE" "$ROOT"

echo "==> Extracting binaries to dist/"
mkdir -p "$ROOT/dist"
CID="$("$DOCKER" create "$IMAGE")"
cleanup() { "$DOCKER" rm -f "$CID" >/dev/null 2>&1 || true; }
trap cleanup EXIT
"$DOCKER" cp "$CID:/usr/local/bin/gufo" "$ROOT/dist/gufo"
"$DOCKER" cp "$CID:/usr/local/bin/gufo-server" "$ROOT/dist/gufo-server"
chmod +x "$ROOT/dist/gufo" "$ROOT/dist/gufo-server"
ls -lh "$ROOT/dist"
sha256sum "$ROOT/dist/gufo" "$ROOT/dist/gufo-server"

cat <<EOF

Done.
  Image:  ${IMAGE}
  Binary: ${ROOT}/dist/gufo

Serve a model on Strix Halo:
  ${DOCKER} run --rm --device /dev/kfd --device /dev/dri \\
    --group-add keep-groups -p 8080:8080 -v /path/to/models:/models \\
    ${IMAGE} serve llm --model /models/model.gguf

Print version / diagnose:
  ${DOCKER} run --rm --device /dev/kfd --device /dev/dri ${IMAGE} --version
  ${DOCKER} run --rm --device /dev/kfd --device /dev/dri ${IMAGE} diagnose
EOF
