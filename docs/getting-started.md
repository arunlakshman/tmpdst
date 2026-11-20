# Getting started

Run MDS locally in five steps with the all-in-one demo image. This page is the
canonical quickstart. Containerfile and Makefile detail lives in
[`deploy/container/README.md`](../deploy/container/README.md).

## Prerequisites

- [Podman](https://podman.io/)
- On macOS: `podman machine start`

## Five steps (`mds-all`)

One container runs the control plane (`-store mem`) and the broker
(`-objects mem`). **State is lost when the container stops. This is not for
production.**

1. Install Podman and start a machine if you are on macOS:

       podman machine start

2. From the repo root, build the demo image:

       podman build -f deploy/container/mds-all.Containerfile -t mds-all:dev .

   Or: `make image-all`.

3. Run it and publish the Kafka and control-plane ports:

       podman run --rm --name mds-all \
         -p 9092:9092 -p 8080:8080 -p 9100:9100 -p 9101:9101 \
         mds-all:dev

4. Point a Kafka client at `127.0.0.1:9092` (the image advertises that address).

5. Produce and consume. Stop with Ctrl-C (or `podman stop mds-all`).

### Smoke without a Kafka client

After step 3:

    curl -s localhost:9101/metrics   # control plane Prometheus text
    curl -s localhost:9100/metrics   # broker Prometheus text

Something should accept TCP on `localhost:9092`.

### Produce and consume (Kafka CLI)

With Apache Kafka tools on your PATH (any recent 3.x console scripts work):

    kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
      --create --topic demo --partitions 1 --replication-factor 1

    echo 'hello' | kafka-console-producer.sh \
      --bootstrap-server 127.0.0.1:9092 --topic demo

    kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
      --topic demo --from-beginning --timeout-ms 5000

Turn off idempotent produce if your client defaults it on
(`enable.idempotence=false`). InitProducerId is not served in 0.1.0.

## What you just ran

- In-memory log store and in-memory object store
- One process pair suitable for a demo, not a durable deploy
- For real stores and separate images, see [operations.md](operations.md)

## Next

- [Architecture](architecture.md) - runtime, Kafka APIs, groups, terms
- [Operations](operations.md) - flags, metrics, containers
