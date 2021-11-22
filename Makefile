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

.PHONY: fmt fmt-check build-image push-image

# project name
PROJECT=loggie

# put REPO=xxx into .env, or make REPO=xxx
-include .env

TAG=$(shell git describe --tags --exact-match 2> /dev/null || git symbolic-ref -q --short HEAD)-$(shell git rev-parse --short HEAD)

GOFILES=$(shell find . -name "*.go" -type f -not -path "./vendor/*")

all: fmt-check build-image push-image

fmt:
	@gofmt -s -w ${GOFILES}

fmt-check:
	@diff=`gofmt -s -d ${GOFILES}`; \
	if [ -n "$${diff}" ]; then \
		echo "Please run 'make fmt' and commit the result:"; \
		echo "$${diff}"; \
		exit 1; \
	fi;

build-image:
	docker build -t ${REPO}/${PROJECT}:${TAG} .

push-image:
	docker push ${REPO}/${PROJECT}:${TAG}