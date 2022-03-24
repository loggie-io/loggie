# Build the binary
FROM golang:1.18 as builder

# Copy in the go src
WORKDIR /go/src/loggie.io/loggie
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build
RUN CGO_ENABLED=1 go build -mod=vendor -a -o loggie cmd/loggie/main.go

# Run
FROM gcr.io/distroless/static-debian11
WORKDIR /app
COPY --from=builder /go/src/loggie.io/loggie ./
ENTRYPOINT ["/loggie"]
