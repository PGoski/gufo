# Production container build for Gufo (Linux x86_64, AMD Strix Halo gfx1151).
# Uses the repository's pinned Nix production package (`nix build .#release`),
# so the compiler and ROCm versions match the qualified toolchain exactly.
#
# Run from the repository root (or use ./docker-build.sh):
#   docker build -f .devops/docker/Dockerfile -t gufo:latest .
#
# Requires network access to cache.nixos.org and roughly 25 GB of free disk.

FROM --platform=linux/amd64 nixos/nix:latest AS build
ENV NIX_CONFIG="experimental-features = nix-command flakes"
WORKDIR /src
COPY . .
RUN nix build -L --option sandbox false .#release \
 && mkdir -p /export \
 && nix path-info -r ./result > /tmp/runtime-closure \
 && tar -cf - -C / $(sed 's|^/||' /tmp/runtime-closure) | tar -xf - -C /export \
 && echo "$(realpath ./result)/bin/gufo" > /export/GUFO_PATH \
 && echo "$(realpath ./result)/bin/gufo-server" > /export/GUFO_SERVER_PATH

FROM --platform=linux/amd64 debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /export/ /
RUN ln -s "$(cat /GUFO_PATH)" /usr/local/bin/gufo \
 && ln -s "$(cat /GUFO_SERVER_PATH)" /usr/local/bin/gufo-server \
 && gufo --version
LABEL org.opencontainers.image.title="gufo" \
      org.opencontainers.image.description="Gufo Engine - local inference runtime for AMD Strix Halo (gfx1151)" \
      org.opencontainers.image.source="https://github.com/gufo-org/gufo"
ENTRYPOINT ["/usr/local/bin/gufo"]
CMD ["--help"]
