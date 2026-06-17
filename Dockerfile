FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
# kubectl is needed for the SSH server to bridge connections
# curl is needed to download kubectl
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install kubectl for the image's target architecture. TARGETARCH is set
# automatically by BuildKit (amd64/arm64); without it a fixed amd64 binary
# fails with "Exec format error" on arm64 nodes (e.g. Apple-silicon
# docker-desktop), breaking every `kubectl exec` bridge.
ARG TARGETARCH
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/${TARGETARCH}/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

# Copy application code
COPY whistler/ whistler/
COPY manifests/ manifests/
COPY bin/ bin/

# Install Python dependencies
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir .

# Default entrypoint (can be overridden)
CMD ["python", "-m", "whistler.server"]
