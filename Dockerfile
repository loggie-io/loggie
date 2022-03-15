# Build the binary
FROM golang:1.16.0 as builder

# Copy in the go src
WORKDIR /go/src/loggie.io/loggie
COPY . .
# Build
RUN CGO_ENABLED=1 go build -mod=vendor -a -o loggie cmd/loggie/main.go

# Run
FROM alpine:3.15.0
WORKDIR /
COPY --from=builder /go/src/loggie.io/loggie/loggie .

ENTRYPOINT ["/loggie"]
