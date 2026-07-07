# Whistler test & cluster orchestration.
#
#   make test              # unit tests in a container (Tier A)
#   make test-local        # unit tests in the local venv
#   make cluster-up        # create the k3d integration cluster
#   make cluster-down      # delete it
#   make integration       # full C1 round trip (creates+tears down a cluster)
#   make integration-keep  # same, but keep the cluster for fast re-runs
#   make desktop-selkies2-local  # run the XFCE Selkies 2.x (pixelflux) image standalone (no cluster)
#   make desktop-gnome-selkies2-local  # same, for the full GNOME Shell (Selkies 2.x) image
#   make desktop-sidecar-local   # run the streamer sidecar + display-unaware XFCE pair (no cluster)
#   make desktop-gnome-sidecar-local # same pair, but with the GNOME Shell workload image
#   make desktop-sidecar-local-down  # stop the pair (either variant) and remove its volumes

CLUSTER      ?= whistler-it
TEST_IMAGE   ?= whistler-test
PYTHON       ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)
SELKIES2_IMAGE ?= whistler-desktop-xfce-selkies2:dev
GNOME_SELKIES2_IMAGE ?= whistler-desktop-gnome-selkies2:dev
# Lightweight encoding profile for the standalone local targets. On Apple Silicon
# the image runs amd64 x264 software encoding under QEMU emulation, and Selkies'
# default resolution is too heavy — the first keyframe is slow enough that the
# client's stream watchdog gives up. This overrides to a profile the emulated
# encoder can keep up with. Bump it (or override on the command line) on a native
# amd64/Linux host, e.g. `make desktop-selkies2-local SELKIES2_RESOLUTION=1920x1080`.
SELKIES2_RESOLUTION ?= 800x600

.PHONY: test test-local cluster-up cluster-down integration integration-keep \
        desktop-selkies2-local desktop-gnome-selkies2-local \
        desktop-sidecar-local desktop-gnome-sidecar-local \
        desktop-sidecar-local-down clean help

help:
	@grep -E '^[a-zA-Z_-]+:.*?#' $(MAKEFILE_LIST) | sed 's/:.*#/\t/' | sort

test: # Build the test image and run unit tests inside the container
	docker build -f Dockerfile.test -t $(TEST_IMAGE) .
	docker run -t --rm $(TEST_IMAGE)

test-local: # Run unit tests in the local venv
	$(PYTHON) -m pytest tests/unit -v

cluster-up: # Create the k3d cluster and install CRDs/PriorityClass
	k3d cluster create $(CLUSTER) --wait
	k3d kubeconfig merge $(CLUSTER) --kubeconfig-switch-context
	kubectl apply -f charts/whistler/crds/crds.yaml
	kubectl apply -f charts/whistler/templates/priorityclass.yaml

cluster-down: # Delete the k3d cluster
	k3d cluster delete $(CLUSTER)

integration: # Full C1 round trip against a throwaway k3d cluster
	CLUSTER=$(CLUSTER) PYTHON=$(PYTHON) scripts/integration.sh

integration-keep: # Same, but keep the cluster afterwards for fast iteration
	KEEP_CLUSTER=1 CLUSTER=$(CLUSTER) PYTHON=$(PYTHON) scripts/integration.sh

integration-existing: # C1 round trip against the current kubectl context (kind/docker-desktop)
	PROVIDER=existing PYTHON=$(PYTHON) scripts/integration.sh

# The 2.x web client requires a browser secure context (WebCodecs). Plain HTTP
# is fine when browsing from the same machine (http://localhost is secure);
# to reach it from ANOTHER machine, run `make desktop-selkies2-local
# SELKIES2_HTTPS=true` and open https://<host>:8082/ (self-signed — click
# through the warning).
SELKIES2_HTTPS ?= false

desktop-selkies2-local: # Build + run the Selkies 2.x (pixelflux/WebSockets) spike image standalone (no cluster); open http://localhost:8082/
	docker build -t $(SELKIES2_IMAGE) desktops/xfce-selkies2
	@echo "Open http://localhost:8082/ (Selkies 2.x dashboard). WebSockets transport —"
	@echo "no TURN needed on any host OS; a plain port publish is enough."
	@echo "Browsing from another machine? Re-run with SELKIES2_HTTPS=true and use https://."
	docker run --rm -it \
	  -p 8082:8082 \
	  -e SELKIES_RESOLUTION=$(SELKIES2_RESOLUTION) \
	  -e SELKIES_ENABLE_HTTPS=$(SELKIES2_HTTPS) \
	  $(SELKIES2_IMAGE)

# Full GNOME Shell over Selkies 2.x. Runtime-configurable identity via env
# (DESKTOP_USER/PUID/PGID/DESKTOP_SUDO); defaults to abc/1000/1000/false — same
# secure-context/HTTPS rules as desktop-selkies2-local. Runs as root PID 1 (to
# set up the user) but needs no --privileged; see desktops/gnome-selkies2/README.
desktop-gnome-selkies2-local: # Build + run the full GNOME Shell (Selkies 2.x) desktop image standalone (no cluster); open http://localhost:8082/
	docker build -t $(GNOME_SELKIES2_IMAGE) desktops/gnome-selkies2
	@echo "Open http://localhost:8082/ (Selkies 2.x dashboard) — the real GNOME Shell."
	@echo "WebSockets transport: no TURN on any host OS. Browsing from another"
	@echo "machine? Re-run with SELKIES2_HTTPS=true and use https://."
	docker run --rm -it \
	  -p 8082:8082 \
	  -e SELKIES_RESOLUTION=$(SELKIES2_RESOLUTION) \
	  -e SELKIES_ENABLE_HTTPS=$(SELKIES2_HTTPS) \
	  $(GNOME_SELKIES2_IMAGE)

# The sidecar-mode pair (streamer-selkies2 + xfce-plain) as two containers
# sharing X/Pulse volumes — the cluster-free equivalent of a
# `streamer: sidecar` desktop pod (see desktops/compose-sidecar.yaml for the
# exact k8s↔compose mapping). Same secure-context/HTTPS and resolution rules
# as desktop-selkies2-local. Ctrl-C stops both; volumes survive for a fast
# restart until the -down target removes them.
desktop-sidecar-local: # Build + run the streamer sidecar + display-unaware XFCE pair (no cluster); open http://localhost:8082/
	@echo "Open http://localhost:8082/ (Selkies 2.x dashboard) — XFCE rendered by the"
	@echo "workload container into the streamer sidecar's display. Browsing from"
	@echo "another machine? Re-run with SELKIES2_HTTPS=true and use https://."
	SELKIES2_RESOLUTION=$(SELKIES2_RESOLUTION) SELKIES2_HTTPS=$(SELKIES2_HTTPS) \
	  docker compose -f desktops/compose-sidecar.yaml up --build

# Same pair with the GNOME Shell workload (desktops/gnome-plain). GNOME needs
# two things XFCE doesn't, both handled here/in the compose file: Selkies'
# h264 streaming mode (mutter emits no damage for static content — without it
# static windows render black) and a shared IPC namespace for MIT-SHM (the
# compose file wires ipc: for every workload; pods get it for free).
desktop-gnome-sidecar-local: # Build + run the streamer sidecar + display-unaware GNOME Shell pair (no cluster); open http://localhost:8082/
	@echo "Open http://localhost:8082/ (Selkies 2.x dashboard) — the real GNOME Shell"
	@echo "rendered by the workload container into the streamer sidecar's display."
	@echo "Browsing from another machine? Re-run with SELKIES2_HTTPS=true and use https://."
	SIDECAR_WORKLOAD=gnome-plain \
	  SIDECAR_WORKLOAD_IMAGE=whistler-desktop-gnome-plain:dev \
	  SELKIES2_H264_STREAMING=true \
	  SELKIES2_RESOLUTION=$(SELKIES2_RESOLUTION) SELKIES2_HTTPS=$(SELKIES2_HTTPS) \
	  docker compose -f desktops/compose-sidecar.yaml up --build

desktop-sidecar-local-down: # Stop the sidecar pair (either variant) and remove its shared X/Pulse volumes
	docker compose -f desktops/compose-sidecar.yaml down -v

clean: # Remove the test image and any leftover cluster
	-docker rmi $(TEST_IMAGE) 2>/dev/null
	-k3d cluster delete $(CLUSTER) 2>/dev/null
