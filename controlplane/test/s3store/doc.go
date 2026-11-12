// Package s3store has the integration tests of the shared S3 adapter
// (shared/objectstore/s3) against a real S3 service, SeaweedFS in a container. The
// tests are in this module because the container helper is internal/testinfra. They
// run with `make it` (the integration build tag).
package s3store
