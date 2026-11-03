BINARY  := cometbft-prune-pacer
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null)
ifeq ($(VERSION),)
$(warning git describe failed (not a git checkout, or "dubious ownership" when building another user's clone as root): version is "dev")
VERSION := dev
endif
GOOS    ?= linux
GOARCH  ?= amd64
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build build-linux test install clean

## build: binary for this machine -> bin/cometbft-prune-pacer
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/$(BINARY)

## build-linux: static binary for the node -> bin/cometbft-prune-pacer-linux-amd64
build-linux:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-$(GOOS)-$(GOARCH) ./cmd/$(BINARY)

test:
	go test -count=1 ./...

## install: go install with the git version -> $(go env GOPATH)/bin (or GOBIN); no root
install:
	CGO_ENABLED=0 go install -trimpath -ldflags "$(LDFLAGS)" ./cmd/$(BINARY)
	@echo "installed $$(go env GOBIN)$$( [ -n "$$(go env GOBIN)" ] || echo "$$(go env GOPATH)/bin")/$(BINARY)"

clean:
	rm -rf bin
