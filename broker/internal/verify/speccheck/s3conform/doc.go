// Package s3conform holds check C3 for the object store: the shared S3 adapter
// (shared/objectstore/s3) is compared with objectstore.Spec by the broker suites in
// speccheck/checker. The tests are here, and not in shared, because the suites are
// part of the broker. The test with a real S3 service runs only when the environment
// variable MDS_S3_ENDPOINT is set. The tests on the in-process fake S3 always run.
package s3conform
