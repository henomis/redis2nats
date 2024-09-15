FROM golang:1.22

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY cmd/ ./cmd/
COPY nats/ ./nats/

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /redis2nats cmd/redis2nats/main.go


EXPOSE 6379

# Run
CMD ["/redis2nats"]