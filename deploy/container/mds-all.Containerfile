# Demo / quickstart image: control plane + broker in one container (mem stores).
# Not for production. Prefer mdscontrolplane + mdsbroker images for real deploys.
#
# Build from the repo root:
#   podman build -f deploy/container/mds-all.Containerfile -t mds-all:dev .
#
# Run (Kafka on localhost:9092, CP gRPC on :8080, metrics on :9100 and :9101):
#   podman run --rm -p 9092:9092 -p 8080:8080 -p 9100:9100 -p 9101:9101 mds-all:dev

FROM bufbuild/buf:1.47.2 AS buf

FROM golang:1.26 AS build
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
ENV GOBIN=/usr/local/bin
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
WORKDIR /src
COPY dst /src/dst
COPY shared /src/shared
COPY controlplane/go.mod controlplane/go.sum /src/controlplane/
COPY broker/go.mod broker/go.sum /src/broker/
WORKDIR /src/broker
RUN go mod download
WORKDIR /src
COPY controlplane /src/controlplane
COPY broker /src/broker
WORKDIR /src/controlplane
RUN cd api && buf generate
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mdscontrolplane ./cmd/mdscontrolplane
WORKDIR /src/broker
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mdsbroker ./cmd/mdsbroker

# Alpine: shell + wget so the entrypoint can start both processes and wait for CP.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -H -u 65532 nonroot
COPY --from=build /out/mdscontrolplane /mdscontrolplane
COPY --from=build /out/mdsbroker /mdsbroker
COPY deploy/container/mds-all-entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
USER nonroot
EXPOSE 8080 9092 9100 9101
ENTRYPOINT ["/entrypoint.sh"]
