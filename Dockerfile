# Build the binary
FROM golang:1.17.0 as builder

# Copy in the go src
WORKDIR /
COPY . .
# Build
RUN CGO_ENABLED=1 go build -mod=vendor -a -o loggie cmd/loggie/main.go

# Run
FROM debian:buster-slim
WORKDIR /
COPY --from=builder /loggie .

ENTRYPOINT ["/loggie"]
