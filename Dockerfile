FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
# kubectl is needed for the SSH server to bridge connections
# curl is needed to download kubectl
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install kubectl for the image's target architecture. TARGETARCH is set
# automatically by BuildKit (amd64/arm64) and is what makes CI's buildx
# multi-arch builds pick the right binary — but a legacy (non-BuildKit)
# `docker build`, which is what skaffold's docker builder can end up doing
# locally, leaves it EMPTY. That turned the URL into .../bin/linux//kubectl,
# and since plain `curl -LO` saves HTTP error bodies, the "kubectl" installed
# was a 241-byte XML error page — surfacing much later as
# "[Errno 8] Exec format error: 'kubectl'" on every exec bridge (web terminal,
# SSH session, SFTP). Hence: fall back to dpkg's native arch when TARGETARCH
# is unset (correct for any same-arch build), and curl with -f so a bad
# download fails the *build*, not the first user session.
ARG TARGETARCH
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}" && \
    curl -fLO "https://dl.k8s.io/release/$(curl -fL -s https://dl.k8s.io/release/stable.txt)/bin/linux/${arch}/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl && \
    kubectl version --client >/dev/null

# Copy application code
COPY whistler/ whistler/
COPY manifests/ manifests/

# Install Python dependencies
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir .

# Default entrypoint (can be overridden)
CMD ["python", "-m", "whistler.server"]
