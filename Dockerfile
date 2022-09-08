# Build the binary
FROM --platform=$BUILDPLATFORM golang:1.17 as builder

ARG TARGETARCH
ARG TARGETOS

# Copy in the go src
WORKDIR /
COPY . .
# Build
#RUN make build

RUN if [ "$TARGETARCH" = "arm64" ]; then apt-get update && apt-get install -y gcc-aarch64-linux-gnu && export CC=aarch64-linux-gnu-gcc && export CC_FOR_TARGET=gcc-aarch64-linux-gnu; fi \
  && GOOS=$TARGETOS GOARCH=$TARGETARCH CC=$CC CC_FOR_TARGET=$CC_FOR_TARGET make build

# Run
FROM --platform=$BUILDPLATFORM debian:buster-slim
WORKDIR /
COPY --from=builder /loggie .

ENTRYPOINT ["/loggie"]
