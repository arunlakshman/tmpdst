# shared

Code that two products use and that has no product words in it.
Module path: `github.com/manifesterio/mds/shared`.

It sits between `dst` and the two products (`controlplane`, `broker`):
`dst <- shared <- controlplane <- broker`. It may import only itself and outside
libraries, never `controlplane` or `broker`.

## Packages

- `awsmetrics`: Prometheus metrics for AWS SDK calls (one sample per HTTP attempt).
- `httpconnections`: HTTP client settings with many idle connections per host.
- `runinfo`: run info for a process: version, git commit, run id, logger, metrics
  server, and the run manifest file. The git commit is set at link time with
  `-ldflags "-X github.com/manifesterio/mds/shared/runinfo.GitCommit=<sha>"`.

## Build

From the repo root: `make -C shared build`. It runs format, ASCII, vet, lint,
modernize and the unit tests.
