package kafka

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	backpressurePauseNonNegative            = assert.NewAlways("kafka: a backpressure pause has a non-negative duration")                                           //determinismcheck:ok catalog handle, set once at start
	batchCommittedAtMostOncePerSend         = assert.NewAlways("kafka: a batch is committed at most once per send")                                                 //determinismcheck:ok catalog handle, set once at start
	batchWithBadCRCAccepted                 = assert.NewUnreachable("kafka: a batch with a bad CRC is accepted")                                                    //determinismcheck:ok catalog handle, set once at start
	batchWithProducerIDAccepted             = assert.NewUnreachable("kafka: a batch with a producer id is accepted")                                                //determinismcheck:ok catalog handle, set once at start
	clientAckedOffsetsIncrease              = assert.NewAlways("kafka: a client's acked batches for a partition are at increasing offsets")                         //determinismcheck:ok catalog handle, set once at start
	committedRangeIsOneProducedBatch        = assert.NewAlways("kafka: a committed range is one whole produced batch")                                              //determinismcheck:ok catalog handle, set once at start
	committedSegmentWithinBatchCap          = assert.NewAlways("kafka: a committed segment holds at most MaxSegmentBatches batches")                                //determinismcheck:ok catalog handle, set once at start
	committedSegmentReportsNonNegative      = assert.NewAlways("kafka: a committed segment reports non-negative occupancy")                                         //determinismcheck:ok catalog handle, set once at start
	committedSegmentWithinPartitionCap      = assert.NewAlways("kafka: a committed segment spans at most MaxSegmentPartitions partitions")                          //determinismcheck:ok catalog handle, set once at start
	committedSegmentCommitCostFits          = assert.NewAlways("kafka: a committed segment's commit cost (1 + partitions) fits the store's commit item limit")      //determinismcheck:ok catalog handle, set once at start
	consumerNeverReadsPastHighWatermark     = assert.NewAlways("kafka: a consumer never reads past the high watermark")                                             //determinismcheck:ok catalog handle, set once at start
	consumerNeverReceivesCorruptBatch       = assert.NewAlways("kafka: a consumer never receives a corrupt batch")                                                  //determinismcheck:ok catalog handle, set once at start
	consumerReadsCommittedValue             = assert.NewAlways("kafka: a consumer reads the committed value at each offset")                                        //determinismcheck:ok catalog handle, set once at start
	consumerResumesWhereLastReadEnded       = assert.NewAlways("kafka: a consumer resumes exactly where its last read ended")                                       //determinismcheck:ok catalog handle, set once at start
	consumerSeesEveryOffsetInOrder          = assert.NewAlways("kafka: a consumer sees every offset in order, none skipped")                                        //determinismcheck:ok catalog handle, set once at start
	consumerFetchNeverOutOfRange            = assert.NewAlways("kafka: a consumer's fetch is never out of range")                                                   //determinismcheck:ok catalog handle, set once at start
	definitelyRejectedBatchNeverCommitted   = assert.NewAlways("kafka: a definitely rejected batch is never committed")                                             //determinismcheck:ok catalog handle, set once at start
	failedSegmentReportsNonNegative         = assert.NewAlways("kafka: a failed segment reports non-negative time and occupancy")                                   //determinismcheck:ok catalog handle, set once at start
	fetchInSettlingSucceeds                 = assert.NewAlways("kafka: a fetch invoked in Settling succeeds")                                                       //determinismcheck:ok catalog handle, set once at start
	fetchHighWatermarkNotAboveLog           = assert.NewAlways("kafka: a fetch never reports a high watermark above the committed log")                             //determinismcheck:ok catalog handle, set once at start
	produceInSettlingIsAcked                = assert.NewAlwaysOnFailure("kafka: a produce still running in Settling is acked")                                      //determinismcheck:ok catalog handle, set once at start
	recordCommittedWhereProduced            = assert.NewAlways("kafka: a record is committed where and as it was produced")                                         //determinismcheck:ok catalog handle, set once at start
	sealedSegmentWithinBatchCap             = assert.NewAlways("kafka: a sealed segment holds at most MaxSegmentBatches batches")                                   //determinismcheck:ok catalog handle, set once at start
	sealedSegmentOvershootsBytesByLastBatch = assert.NewAlways("kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch")                      //determinismcheck:ok catalog handle, set once at start
	sealedSegmentWithinPartitionCap         = assert.NewAlways("kafka: a sealed segment spans at most MaxSegmentPartitions partitions")                             //determinismcheck:ok catalog handle, set once at start
	sealedSegmentCommitCostFits             = assert.NewAlways("kafka: a sealed segment's implied commit cost (1 + partitions) fits the store's commit item limit") //determinismcheck:ok catalog handle, set once at start
	segmentSealedWithinBatchWindow          = assert.NewAlways("kafka: a segment is sealed within the window of its first batch")                                   //determinismcheck:ok catalog handle, set once at start
	segmentPutTimeNonNegative               = assert.NewAlways("kafka: a segment's put time is non-negative and it took at least one attempt")                      //determinismcheck:ok catalog handle, set once at start
	storedBatchCommittedAtMostOnce          = assert.NewAlways("kafka: a stored batch is committed at most once")                                                   //determinismcheck:ok catalog handle, set once at start
	ackFactWellFormed                       = assert.NewAlways("kafka: ack fact is well formed")                                                                    //determinismcheck:ok catalog handle, set once at start
	ackedRangeInControlPlane                = assert.NewAlways("kafka: acked range is committed in the control plane")                                              //determinismcheck:ok catalog handle, set once at start
	ackedRecordAtAckedOffset                = assert.NewAlways("kafka: an acked record is in the log at its acked offset")                                          //determinismcheck:ok catalog handle, set once at start
	clientBatchBuilds                       = assert.NewUnreachable("kafka: client batch builds")                                                                   //determinismcheck:ok catalog handle, set once at start
	clientRequestEncodes                    = assert.NewUnreachable("kafka: client request encodes")                                                                //determinismcheck:ok catalog handle, set once at start
	committedBatchIntact                    = assert.NewAlways("kafka: committed batch is intact")                                                                  //determinismcheck:ok catalog handle, set once at start
	committedOffsetsContiguous              = assert.NewAlways("kafka: committed offsets are contiguous")                                                           //determinismcheck:ok catalog handle, set once at start
	committedRangeHoldsRecordCount          = assert.NewAlways("kafka: committed range holds its record count")                                                     //determinismcheck:ok catalog handle, set once at start
	committedRangeLiesInsideSegment         = assert.NewAlways("kafka: committed range lies inside its segment")                                                    //determinismcheck:ok catalog handle, set once at start
	committedSegmentExistsObjectStore       = assert.NewAlways("kafka: committed segment exists in the object store")                                               //determinismcheck:ok catalog handle, set once at start
	everyCommittedRecordProduced            = assert.NewAlways("kafka: every committed record was produced")                                                        //determinismcheck:ok catalog handle, set once at start
	everyConsumerCaughtUp                   = assert.NewAlways("kafka: every consumer caught up with the final log")                                                //determinismcheck:ok catalog handle, set once at start
	noOpPendingAtEnd                        = assert.NewAlways("kafka: no op pending at end")                                                                       //determinismcheck:ok catalog handle, set once at start
	batchRecordsKeepOrder                   = assert.NewAlways("kafka: records of a batch keep their order")                                                        //determinismcheck:ok catalog handle, set once at start
	segmentDurationsAddUp                   = assert.NewAlways("kafka: segment put + queue wait + commit equals its seal-to-commit total")                          //determinismcheck:ok catalog handle, set once at start
	segmentDurationsNonNegative             = assert.NewAlways("kafka: segment put, queue wait and commit durations are non-negative")                              //determinismcheck:ok catalog handle, set once at start
	campaignCommittedAnyRecords             = assert.NewSometimes("kafka: the campaign committed records")                                                          //determinismcheck:ok catalog handle, set once at start
	highWatermarkIsLogEnd                   = assert.NewAlways("kafka: the high watermark is the end of the committed log")                                         //determinismcheck:ok catalog handle, set once at start
	kernelAnnouncedSettling                 = assert.NewAlways("kafka: the kernel announced Settling")                                                              //determinismcheck:ok catalog handle, set once at start
	workloadIssuedFetches                   = assert.NewSometimes("kafka: workload issued fetches")                                                                 //determinismcheck:ok catalog handle, set once at start
	workloadIssuedProduces                  = assert.NewSometimes("kafka: workload issued produces")                                                                //determinismcheck:ok catalog handle, set once at start
)
