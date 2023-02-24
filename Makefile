# Copyright 2021 Loggie Authors

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: fmt fmt-check build docker-build docker-push docker-multi-arch test

# put REPO=xxx into .env, or make REPO=xxx
-include .env

TAG=$(shell git describe --tags --exact-match 2> /dev/null || git symbolic-ref -q --short HEAD)-$(shell git rev-parse --short HEAD)

GOFILES=$(shell find . -name "*.go" -type f -not -path "./vendor/*")

extra_flags := -ldflags '-X github.com/loggie-io/loggie/pkg/core/global._VERSION_=${TAG} -s -w -extldflags "-static"'

all: lint docker-multi-arch

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Fmt

fmt: ## Run gofmt .go files
	@gofmt -s -w ${GOFILES}

fmt-check: ## Check the fmt of .go files
	@diff=`gofmt -s -d ${GOFILES}`; \
	if [ -n "$${diff}" ]; then \
		echo "Please run 'make fmt' and commit the result:"; \
		echo "$${diff}"; \
		exit 1; \
	fi;

##@ Lint

lint: golangci-lint ## Run golangci-lint
	$(GOLANGCI-LINT) run --timeout=10m

GOLANGCI-LINT = ./bin/golangci-lint
golangci-lint: ## Download golangci-lint locally if necessary.
	$(call get-golangci-lint,$(GOLANGCI-LINT))

define get-golangci-lint
@[ -f $(1) ] || { \
set -e ;\
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.50.1 ;\
}
endef

##@ Test

test: ## Run go test ./...
	go test ./...

##@ Benchmark

benchmark: ## Run benchmark
	go test -test.bench=".*" -benchmem ./test/benchmark/...

##@ Build

build: ## go build
	CGO_ENABLED=1 GOOS=${GOOS} GOARCH=${GOARCH} go build -mod=vendor -a ${extra_flags} -o loggie cmd/loggie/main.go

##@ Build(without sqlite)

build-no-sqlite: ## go build without sqlite
	GOOS=${GOOS} GOARCH=${GOARCH} go build -tags driver_badger -mod=vendor -a -ldflags '-X github.com/loggie-io/loggie/pkg/core/global._VERSION_=${TAG} -X github.com/loggie-io/loggie/pkg/util/persistence/driver._DRIVER_=driver_badger -s -w' -o loggie cmd/loggie/main.go

##@ Images

docker-build: ## Docker build -t ${REPO}:${TAG}, try: make docker-build REPO=<YourRepoHost>, ${TAG} generated by git
	docker build -t ${REPO}:${TAG} .

docker-push: ## Docker push ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

docker-multi-arch: ## Docker buildx, try: make docker-build REPO=<YourRepoHost>, ${TAG} generated by git
	docker buildx build --platform linux/amd64,linux/arm64 -t ${REPO}:${TAG} . --push

LOG_DIR ?= /tmp/log ## log directory
LOG_MAXSIZE ?= 10 ## max size in MB of the logfile before it's rolled
LOG_QPS ?= 0 ## qps of line generate
LOG_TOTAL ?= 5 ## total line count
LOG_LINE_BYTES ?= 1024 ## bytes per line
LOG_MAX_BACKUPS ?= 5 ## max number of rolled files to keep
genfiles: ## generate log files, try: make genfiles LOG_TOTAL=30000
	go run cmd/loggie/main.go genfiles -totalCount=${LOG_TOTAL} -lineBytes=${LOG_LINE_BYTES} -qps=${LOG_QPS} \
	-log.maxBackups=${LOG_MAX_BACKUPS} -log.maxSize=${LOG_MAXSIZE} -log.directory=${LOG_DIR} -log.noColor=true \
	-log.enableStdout=false -log.enableFile=true -log.timeFormat="2006-01-02 15:04:05.000"