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

# The GPU variants are not a choice a template makes: a VM with a GPU boots
# <image>-cuda in place of the image its template names (gpu_image_variant in
# whistler/config.py). So the two are baked together by default, and a
# rebuild cannot leave one of them on an older Selkies than the other.
vm-gnome-desktop-image: # Bake the GNOME-Shell+Selkies KubeVirt containerDisks, lean and -cuda (24.04; needs qemu/KVM; PUSH=1 to push, CUDA=0|1 for just one, IMAGE/TAG to override)
	set -e; for cuda in $(or $(CUDA),0 1); do \
	  PUSH=$(or $(PUSH),0) CUDA=$$cuda desktops/vm-gnome-selkies/build.sh; \
	done

# base and cuda together, for the same reason. (No baked CUDA SDK: a user who
# wants nvcc installs it in the guest, e.g. with pixi; images/devbase/README.md.)
devbase-image: # Bake the devbase dev-server containerDisks, base and -cuda — no desktop, SSH only (26.04; needs qemu/KVM; PUSH=1 to push, VARIANT=base|cuda for just one, IMAGE/TAG to override)
	set -e; for variant in $(or $(VARIANT),base cuda); do \
	  PUSH=$(or $(PUSH),0) VARIANT=$$variant images/devbase/build.sh; \
	done

clean: # Remove the test image and any leftover cluster
	-docker rmi $(TEST_IMAGE) 2>/dev/null
	-k3d cluster delete $(CLUSTER) 2>/dev/null
