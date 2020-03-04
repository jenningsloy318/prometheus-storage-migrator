

GO           ?= go
GOFMT        ?= $(GO)fmt
pkgs          = ./...

BIN_DIR                 ?= $(shell pwd)/build

all:   build 

build: 
	@echo ">> building binaries"
	$(GO) build  -o $(BIN_DIR)/prometheus-storage-migrator 

fmt:
	@echo ">> format code style"
	$(GOFMT) -w $$(find . -path ./vendor -prune -o -name '*.go' -print) 




.PHONY: all build fmt