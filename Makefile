BINARY  := cometbft-prune-pacer
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
PREFIX  ?= /usr/local
GOOS    ?= linux
GOARCH  ?= amd64
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build build-linux test install clean

## build: binary for this machine -> bin/cometbft-prune-pacer
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) .

## build-linux: static binary for the node -> bin/cometbft-prune-pacer-linux-amd64
build-linux:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-$(GOOS)-$(GOARCH) .

test:
	go test -count=1 ./...

## install: build here and copy to $(PREFIX)/bin (run on the node; needs root)
install: build
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 0755 bin/$(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)

clean:
	rm -rf bin
