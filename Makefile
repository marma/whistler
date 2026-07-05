# Whistler test & cluster orchestration.
#
#   make test              # unit tests in a container (Tier A)
#   make test-local        # unit tests in the local venv
#   make cluster-up        # create the k3d integration cluster
#   make cluster-down      # delete it
#   make integration       # full C1 round trip (creates+tears down a cluster)
#   make integration-keep  # same, but keep the cluster for fast re-runs
#   make desktop-webrtc-local  # run the WebRTC desktop image standalone (no cluster)
#   make desktop-gnome-flashback-webrtc-local  # same, for the GNOME Flashback image
#   make desktop-selkies2-local  # same, for the Selkies 2.x (pixelflux) spike image
#   make desktop-gnome-selkies2-local  # same, for the full GNOME Shell (Selkies 2.x) image

CLUSTER      ?= whistler-it
TEST_IMAGE   ?= whistler-test
PYTHON       ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)
WEBRTC_IMAGE ?= whistler-desktop-xfce-webrtc:dev
GNOME_FLASHBACK_WEBRTC_IMAGE ?= whistler-desktop-gnome-flashback-webrtc:dev
SELKIES2_IMAGE ?= whistler-desktop-xfce-selkies2:dev
GNOME_SELKIES2_IMAGE ?= whistler-desktop-gnome-selkies2:dev
# Lightweight encoding profile for the standalone local target. On Apple Silicon
# the image runs amd64 x264 software encoding under QEMU emulation, and Selkies'
# defaults (1280x720 / 60fps / 8Mbps) are too heavy — the first keyframe is slow
# enough that the client's stream watchdog gives up. These override to a profile
# the emulated encoder can keep up with. Bump them (or override on the command
# line) on a native amd64/Linux host, e.g. `make desktop-webrtc-local WEBRTC_FRAMERATE=60`.
WEBRTC_RESOLUTION    ?= 800x600
WEBRTC_FRAMERATE     ?= 10
WEBRTC_VIDEO_BITRATE ?= 1500

.PHONY: test test-local cluster-up cluster-down integration integration-keep \
        desktop-webrtc-local desktop-gnome-flashback-webrtc-local \
        desktop-selkies2-local desktop-gnome-selkies2-local clean help

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

desktop-webrtc-local: # Build + run the WebRTC desktop image standalone (no cluster); open http://localhost:8082/
	docker build -t $(WEBRTC_IMAGE) desktops/xfce-webrtc
	@echo "Open http://localhost:8082/ (Selkies' own UI). On macOS/Windows the internal"
	@echo "TURN is enabled so media flows; on Linux you can drop it and add --network host."
	docker run --rm -it \
	  -p 8082:8082 \
	  -p 3478:3478/udp -p 3478:3478/tcp \
	  -p 49160-49200:49160-49200/udp \
	  -e SELKIES_USE_INTERNAL_TURN=1 \
	  -e SELKIES_RESOLUTION=$(WEBRTC_RESOLUTION) \
	  -e SELKIES_FRAMERATE=$(WEBRTC_FRAMERATE) \
	  -e SELKIES_VIDEO_BITRATE=$(WEBRTC_VIDEO_BITRATE) \
	  $(WEBRTC_IMAGE)

desktop-gnome-flashback-webrtc-local: # Build + run the GNOME Flashback WebRTC desktop image standalone (no cluster); open http://localhost:8082/
	docker build -t $(GNOME_FLASHBACK_WEBRTC_IMAGE) desktops/gnome-flashback-webrtc
	@echo "Open http://localhost:8082/ (Selkies' own UI). Needs --privileged (systemd as"
	@echo "PID 1 — see desktops/gnome-flashback-webrtc/README.md). On macOS/Windows the"
	@echo "internal TURN is enabled so media flows; on Linux you can drop it and add"
	@echo "--network host."
	docker run --rm -it --privileged \
	  -p 8082:8082 \
	  -p 3478:3478/udp -p 3478:3478/tcp \
	  -p 49160-49200:49160-49200/udp \
	  -e SELKIES_USE_INTERNAL_TURN=1 \
	  -e SELKIES_RESOLUTION=$(WEBRTC_RESOLUTION) \
	  -e SELKIES_FRAMERATE=$(WEBRTC_FRAMERATE) \
	  -e SELKIES_VIDEO_BITRATE=$(WEBRTC_VIDEO_BITRATE) \
	  $(GNOME_FLASHBACK_WEBRTC_IMAGE)

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
	  -e SELKIES_RESOLUTION=$(WEBRTC_RESOLUTION) \
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
	  -e SELKIES_RESOLUTION=$(WEBRTC_RESOLUTION) \
	  -e SELKIES_ENABLE_HTTPS=$(SELKIES2_HTTPS) \
	  $(GNOME_SELKIES2_IMAGE)

clean: # Remove the test image and any leftover cluster
	-docker rmi $(TEST_IMAGE) 2>/dev/null
	-k3d cluster delete $(CLUSTER) 2>/dev/null
