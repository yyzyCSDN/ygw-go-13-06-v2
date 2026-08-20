FROM golang:1.23-bookworm
WORKDIR /workspace
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build ./...
CMD ["bash"]
