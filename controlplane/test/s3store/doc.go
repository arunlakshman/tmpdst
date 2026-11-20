// Package s3store has the integration tests of the shared S3 adapter
// (shared/objectstore/s3) against a real S3 service, SeaweedFS in a container. The
// tests live in this module and use shared/testinfra for the container. They
// run with `make it` (the integration build tag).
package s3store
