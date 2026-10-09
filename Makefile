# Whistler test & cluster orchestration.
#
#   make test              # unit tests in a container (Tier A)
#   make test-local        # unit tests in the local venv
#   make cluster-up        # create the k3d integration cluster
#   make cluster-down      # delete it
#   make integration       # full C1 round trip (creates+tears down a cluster)
#   make integration-keep  # same, but keep the cluster for fast re-runs
#   make vm-gnome-desktop-image  # bake the GNOME desktop VM (needs qemu/KVM)
#   make devbase-image           # bake the SSH-only dev-server VM

CLUSTER      ?= whistler-it
TEST_IMAGE   ?= whistler-test
PYTHON       ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)

.PHONY: test test-local cluster-up cluster-down integration integration-keep \
        vm-gnome-desktop-image \
        devbase-image clean help

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

vm-gnome-desktop-image: # Bake the GNOME-Shell+Selkies KubeVirt containerDisk (24.04; needs qemu/KVM; PUSH=1 to push, CUDA=1 for the -cuda GPU variant, IMAGE/TAG to override)
	PUSH=$(or $(PUSH),0) CUDA=$(or $(CUDA),0) desktops/vm-gnome-selkies/build.sh

devbase-image: # Bake the devbase dev-server containerDisk — no desktop, SSH only (26.04; needs qemu/KVM; PUSH=1 to push, VARIANT=base|cuda|cuda-dev, IMAGE/TAG to override)
	PUSH=$(or $(PUSH),0) VARIANT=$(or $(VARIANT),base) images/devbase/build.sh

clean: # Remove the test image and any leftover cluster
	-docker rmi $(TEST_IMAGE) 2>/dev/null
	-k3d cluster delete $(CLUSTER) 2>/dev/null
