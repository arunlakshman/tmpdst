# Image for the broker (cmd/mdsbroker).
# Build from the root of the monorepo (needs dst, shared, and controlplane):
#   podman build -f deploy/container/mdsbroker.Containerfile -t mdsbroker:dev .
#
# Generates controlplane protos in-image (same as the control plane image), then
# builds the broker. Root .dockerignore applies (module-local .dockerignore does not).

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
WORKDIR /src/broker
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mdsbroker ./cmd/mdsbroker

# Static binary, no shell, runs as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mdsbroker /mdsbroker
EXPOSE 9092 9100
ENTRYPOINT ["/mdsbroker"]
