package broker

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	admittedBatchCannotEncode          = assert.NewUnreachable("broker: admitted batches cannot be encoded")              //determinismcheck:ok catalog handle, set once at start
	commitAnswersEveryBatchOrder       = assert.NewAlwaysOrUnreachable("broker: commit answers every batch in order")     //determinismcheck:ok catalog handle, set once at start
	commitDeadlineExceeded             = assert.NewReachable("broker: commit deadline exceeded")                          //determinismcheck:ok catalog handle, set once at start
	commitKeyReusedDifferentRequest    = assert.NewUnreachable("broker: commit key reused with a different request")      //determinismcheck:ok catalog handle, set once at start
	commitRetried                      = assert.NewReachable("broker: commit retried")                                    //determinismcheck:ok catalog handle, set once at start
	commitSucceededAfterOldRetryBudget = assert.NewReachable("broker: commit succeeded after the old retry budget")       //determinismcheck:ok catalog handle, set once at start
	committedSegmentMissing            = assert.NewUnreachable("broker: committed segment missing")                       //determinismcheck:ok catalog handle, set once at start
	corruptGetDetected                 = assert.NewReachable("broker: corrupt GET detected")                              //determinismcheck:ok catalog handle, set once at start
	corruptProduceBatchRejected        = assert.NewReachable("broker: corrupt produce batch rejected")                    //determinismcheck:ok catalog handle, set once at start
	fetchParkedForPoll                 = assert.NewReachable("broker: fetch parked for a poll")                           //determinismcheck:ok catalog handle, set once at start
	gzipBatchAdmitted                  = assert.NewReachable("broker: gzip batch admitted")                               //determinismcheck:ok catalog handle, set once at start
	idempotentBatchRejected            = assert.NewReachable("broker: idempotent batch rejected")                         //determinismcheck:ok catalog handle, set once at start
	lz4BatchAdmitted                   = assert.NewReachable("broker: lz4 batch admitted")                                //determinismcheck:ok catalog handle, set once at start
	oversizeBatchSealedAlone           = assert.NewReachable("broker: oversize batch sealed alone")                       //determinismcheck:ok catalog handle, set once at start
	readsPausedByBackpressure          = assert.NewReachable("broker: reads paused by backpressure")                      //determinismcheck:ok catalog handle, set once at start
	readsResumedAfterBackpressure      = assert.NewReachable("broker: reads resumed after backpressure")                  //determinismcheck:ok catalog handle, set once at start
	requestRetriesExhausted            = assert.NewReachable("broker: request retries exhausted")                         //determinismcheck:ok catalog handle, set once at start
	segmentExceedsCommitSize           = assert.NewUnreachable("broker: segment exceeds the control plane's commit size") //determinismcheck:ok catalog handle, set once at start
	segmentRetriesExhausted            = assert.NewReachable("broker: segment retries exhausted")                         //determinismcheck:ok catalog handle, set once at start
	segmentSealedBatchCap              = assert.NewReachable("broker: segment sealed by batch cap")                       //determinismcheck:ok catalog handle, set once at start
	segmentSealedPartitionCap          = assert.NewReachable("broker: segment sealed by partition cap")                   //determinismcheck:ok catalog handle, set once at start
	segmentSealedSize                  = assert.NewReachable("broker: segment sealed by size")                            //determinismcheck:ok catalog handle, set once at start
	segmentSealedTimer                 = assert.NewReachable("broker: segment sealed by timer")                           //determinismcheck:ok catalog handle, set once at start
	snappyBatchAdmitted                = assert.NewReachable("broker: snappy batch admitted")                             //determinismcheck:ok catalog handle, set once at start
	zstdBatchAdmitted                  = assert.NewReachable("broker: zstd batch admitted")                               //determinismcheck:ok catalog handle, set once at start
)
