SHELL := /bin/bash
IMAGE ?= sentinel:dev
KIND_CLUSTER ?= sentinel

.PHONY: all fmt vet lint test test-race test-integration build docker-build generate manifests deploy undeploy kind-load
all: fmt vet test

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test ./...

test-race:
	go test ./... -race -count=1

test-integration:
	SENTINEL_KIND_TEST=1 go test -tags=integration ./tests -v -count=1

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/sentinel ./cmd/manager

docker-build:
	docker build -t $(IMAGE) .

generate:
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

manifests:
	controller-gen rbac:roleName=sentinel crd paths="./..." output:crd:artifacts:config=config/crd/bases

deploy:
	kubectl apply -k config

undeploy:
	kubectl delete -k config --ignore-not-found

kind-load: docker-build
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

# Opt-in: explicitly name a disposable local test context.
.PHONY: test-network
test-network:
	./scripts/network-integration.sh $(NETWORK_CONTEXT)
