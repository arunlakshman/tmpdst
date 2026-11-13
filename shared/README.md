# shared

Code that two products use. It may know the outside world they both talk to (the object store), but not the machines of one product.
Module path: `github.com/manifesterio/mds/shared`.

It sits between `dst` and the two products (`controlplane`, `broker`):
`dst <- shared <- controlplane <- broker`. It may import only itself and outside
libraries, never `controlplane` or `broker`.

## Packages

- `awsmetrics`: Prometheus metrics for AWS SDK calls (one sample per HTTP attempt).
- `httpconnections`: HTTP client settings with many idle connections per host.
- `status`: the plain-data words `ErrCode` and `Outcome` that every answer from the outside world uses.
- `objectstore`: the object store contract (the calls and results as sealed sums, the `Store` and
  `Classifier` interfaces) and its spec (`Spec`, with its sorted-slice self-check).
  `objectstore/s3` is the one S3 adapter (broker and control plane); `objectstore/s3/fakes3` is
  a small fake S3 server for tests.
- `clock`: the real clock (`Real`) and the test clock (`Fake`) as `machine.Clock`, over `dst`.
- `runinfo`: run info for a process: version, git commit, run id, logger, metrics
  server, and the run manifest file. The git commit is set at link time with
  `-ldflags "-X github.com/manifesterio/mds/shared/runinfo.GitCommit=<sha>"`.

## Build

From the repo root: `make -C shared build`. It runs format, ASCII, vet, lint,
modernize and the unit tests.
