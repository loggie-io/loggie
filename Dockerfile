# Build the binary
FROM --platform=$BUILDPLATFORM golang:1.17 as builder

ARG TARGETARCH

# Copy in the go src
WORKDIR /
COPY . .
# Build
#RUN CGO_ENABLED=1 go build -mod=vendor -a -o loggie cmd/loggie/main.go

RUN if [ "$TARGETARCH" = "arm64" ]; then apt-get update && apt-get install -y gcc-aarch64-linux-gnu && export CC=aarch64-linux-gnu-gcc && export CC_FOR_TARGET=gcc-aarch64-linux-gnu; fi \
  && CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH CC=$CC CC_FOR_TARGET=$CC_FOR_TARGET go build -mod=vendor -a -ldflags '-extldflags "-static"' -o loggie cmd/loggie/main.go

# Run
FROM --platform=$BUILDPLATFORM debian:buster-slim
WORKDIR /
COPY --from=builder /loggie .

ENTRYPOINT ["/loggie"]
