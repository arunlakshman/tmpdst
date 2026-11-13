package echo

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	getReturnsAckedPayload           = assert.NewAlways("echo: GET of an acked offset returns the acked payload") //determinismcheck:ok catalog handle, set once at start
	ackFactWellFormed                = assert.NewAlways("echo: ack fact is well formed")                          //determinismcheck:ok catalog handle, set once at start
	ackedOffsetCommitted             = assert.NewAlways("echo: acked offset is committed")                        //determinismcheck:ok catalog handle, set once at start
	ackedSegmentExistsObjectStore    = assert.NewAlways("echo: acked segment exists in the object store")         //determinismcheck:ok catalog handle, set once at start
	ackedSegmentHoldsAckedPayload    = assert.NewAlways("echo: acked segment holds the acked payload")            //determinismcheck:ok catalog handle, set once at start
	offsetAckedOnePayload            = assert.NewAlways("echo: an offset is acked with one payload")              //determinismcheck:ok catalog handle, set once at start
	commitAssignsExactlyOneBatch     = assert.NewAlways("echo: commit assigns exactly the one batch")             //determinismcheck:ok catalog handle, set once at start
	commitKeyReusedDifferentRequest  = assert.NewUnreachable("echo: commit key reused with a different request")  //determinismcheck:ok catalog handle, set once at start
	commitRetriedAfterMaybeApplied   = assert.NewReachable("echo: commit retried after MaybeApplied")             //determinismcheck:ok catalog handle, set once at start
	committedSegmentMissing          = assert.NewUnreachable("echo: committed segment missing")                   //determinismcheck:ok catalog handle, set once at start
	corruptionDetectedRead           = assert.NewReachable("echo: corruption detected on read")                   //determinismcheck:ok catalog handle, set once at start
	noRequestPendingAtEnd            = assert.NewAlways("echo: no request pending at end")                        //determinismcheck:ok catalog handle, set once at start
	requestInSettlingSucceeds        = assert.NewAlways("echo: request invoked in Settling succeeds")             //determinismcheck:ok catalog handle, set once at start
	kernelAnnouncedSettlingAndSettle = assert.NewAlways("echo: the kernel announced Settling and Settle")         //determinismcheck:ok catalog handle, set once at start
	workloadIssuedRequests           = assert.NewSometimes("echo: workload issued requests")                      //determinismcheck:ok catalog handle, set once at start
)
