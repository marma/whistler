# Whistler test & cluster orchestration.
#
#   make test              # unit tests in a container (Tier A)
#   make test-local        # unit tests in the local venv
#   make cluster-up        # create the k3d integration cluster
#   make cluster-down      # delete it
#   make integration       # full C1 round trip (creates+tears down a cluster)
#   make integration-keep  # same, but keep the cluster for fast re-runs

CLUSTER      ?= whistler-it
TEST_IMAGE   ?= whistler-test
PYTHON       ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)

.PHONY: test test-local cluster-up cluster-down integration integration-keep clean help

help:
	@grep -E '^[a-zA-Z_-]+:.*?#' $(MAKEFILE_LIST) | sed 's/:.*#/\t/' | sort

test: # Build the test image and run unit tests inside the container
	docker build -f Dockerfile.test -t $(TEST_IMAGE) .
	docker run --rm $(TEST_IMAGE)

test-local: # Run unit tests in the local venv
	$(PYTHON) -m pytest tests/unit -v

cluster-up: # Create the k3d cluster and install CRDs/PriorityClass
	k3d cluster create $(CLUSTER) --wait
	k3d kubeconfig merge $(CLUSTER) --kubeconfig-switch-context
	kubectl apply -f manifests/crds.yaml
	kubectl apply -f charts/whistler/templates/priorityclass.yaml

cluster-down: # Delete the k3d cluster
	k3d cluster delete $(CLUSTER)

integration: # Full C1 round trip against a throwaway k3d cluster
	CLUSTER=$(CLUSTER) PYTHON=$(PYTHON) scripts/integration.sh

integration-keep: # Same, but keep the cluster afterwards for fast iteration
	KEEP_CLUSTER=1 CLUSTER=$(CLUSTER) PYTHON=$(PYTHON) scripts/integration.sh

integration-existing: # C1 round trip against the current kubectl context (kind/docker-desktop)
	PROVIDER=existing PYTHON=$(PYTHON) scripts/integration.sh

clean: # Remove the test image and any leftover cluster
	-docker rmi $(TEST_IMAGE) 2>/dev/null
	-k3d cluster delete $(CLUSTER) 2>/dev/null
