package simulator

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	callAbandoned                 = assert.NewReachable("rebalance-sim: a call is abandoned")                          //determinismcheck:ok catalog handle, set once at start
	failedWriteEndsTerm           = assert.NewReachable("rebalance-sim: a failed write ends the term")                 //determinismcheck:ok catalog handle, set once at start
	heartbeatToldJoinAgain        = assert.NewReachable("rebalance-sim: a heartbeat is told to join again")            //determinismcheck:ok catalog handle, set once at start
	joinRoundCompletes            = assert.NewReachable("rebalance-sim: a join round completes")                       //determinismcheck:ok catalog handle, set once at start
	lateReplyAbandonedCallDropped = assert.NewReachable("rebalance-sim: a late reply to an abandoned call is dropped") //determinismcheck:ok catalog handle, set once at start
	leaveRemovesMember            = assert.NewReachable("rebalance-sim: a leave removes a member")                     //determinismcheck:ok catalog handle, set once at start
	memberGetsAssignment          = assert.NewReachable("rebalance-sim: a member gets its assignment")                 //determinismcheck:ok catalog handle, set once at start
	memberNeverSyncedRemoved      = assert.NewReachable("rebalance-sim: a member that never synced is removed")        //determinismcheck:ok catalog handle, set once at start
	refusedLoadReadSentAgain      = assert.NewReachable("rebalance-sim: a refused load read is sent again")            //determinismcheck:ok catalog handle, set once at start
	roundCompletesManyMembers     = assert.NewReachable("rebalance-sim: a round completes with many members")          //determinismcheck:ok catalog handle, set once at start
	silentMemberRemoved           = assert.NewReachable("rebalance-sim: a silent member is removed")                   //determinismcheck:ok catalog handle, set once at start
	takeoverLoadsGroupMembers     = assert.NewReachable("rebalance-sim: a takeover loads a group with members")        //determinismcheck:ok catalog handle, set once at start
	offsetCommitAcked             = assert.NewReachable("rebalance-sim: an offset commit is acked")                    //determinismcheck:ok catalog handle, set once at start
	groupsReadAgainFromFreshTail  = assert.NewReachable("rebalance-sim: groups are read again from a fresh tail")      //determinismcheck:ok catalog handle, set once at start
	roleLostWhileCallsWait        = assert.NewReachable("rebalance-sim: the role is lost while calls wait")            //determinismcheck:ok catalog handle, set once at start
	writeTimerEndsTerm            = assert.NewReachable("rebalance-sim: the write timer ends the term")                //determinismcheck:ok catalog handle, set once at start
	commitAnswerHasOneCode        = assert.NewAlways("sim: a commit answer has one code per offset")                   //determinismcheck:ok catalog handle, set once at start
	generationNeverGoesDown       = assert.NewAlways("sim: a generation never goes down")                              //determinismcheck:ok catalog handle, set once at start
	knownMemberKeepsID            = assert.NewAlways("sim: a known member keeps its id")                               //determinismcheck:ok catalog handle, set once at start
	machineAnswersOnlyWithRole    = assert.NewAlways("sim: a machine answers only with the role")                      //determinismcheck:ok catalog handle, set once at start
	silentMemberRemovedCheck      = assert.NewAlways("sim: a silent member is removed")                                //determinismcheck:ok catalog handle, set once at start
	syncAnswerIsLoggedAssignment  = assert.NewAlways("sim: a sync answer is the logged assignment")                    //determinismcheck:ok catalog handle, set once at start
	ackedWriteHasSpecAnswer       = assert.NewAlways("sim: acked write has the spec answer")                           //determinismcheck:ok catalog handle, set once at start
	ackedWriteInLog               = assert.NewAlways("sim: acked write is in the log")                                 //determinismcheck:ok catalog handle, set once at start
	ackedWriteVisible             = assert.NewAlways("sim: acked write is visible")                                    //determinismcheck:ok catalog handle, set once at start
	ackedOffsetCommitInLog        = assert.NewAlways("sim: an acked offset commit is in the log")                      //determinismcheck:ok catalog handle, set once at start
	ackedOffsetCommitSurvives     = assert.NewAlways("sim: an acked offset commit survives")                           //determinismcheck:ok catalog handle, set once at start
	answeredGenerationInLog       = assert.NewAlways("sim: an answered generation is in the log")                      //determinismcheck:ok catalog handle, set once at start
	consumerGroupsSettle          = assert.NewAlways("sim: consumer groups settle")                                    //determinismcheck:ok catalog handle, set once at start
	everyCallGetsAnswer           = assert.NewAlways("sim: every call gets an answer")                                 //determinismcheck:ok catalog handle, set once at start
	everyCallGetsOneAnswer        = assert.NewAlways("sim: every call gets one answer")                                //determinismcheck:ok catalog handle, set once at start
	finalReadsAnswered            = assert.NewAlways("sim: final reads are answered")                                  //determinismcheck:ok catalog handle, set once at start
	knownAction                   = assert.NewAlways("sim: known action")                                              //determinismcheck:ok catalog handle, set once at start
	liveReplicasConverge          = assert.NewAlways("sim: live replicas converge")                                    //determinismcheck:ok catalog handle, set once at start
	noTwoLeadersInGeneration      = assert.NewAlways("sim: no two leaders of one generation")                          //determinismcheck:ok catalog handle, set once at start
	offsetsNeverGoBackwards       = assert.NewAlways("sim: offsets never go backwards")                                //determinismcheck:ok catalog handle, set once at start
	oneWriterPerEpoch             = assert.NewAlways("sim: one writer per epoch")                                      //determinismcheck:ok catalog handle, set once at start
	readsAgreeWithSpec            = assert.NewAlways("sim: reads agree with the spec")                                 //determinismcheck:ok catalog handle, set once at start
	replicasAgreeEverySlot        = assert.NewAlways("sim: replicas agree on every slot")                              //determinismcheck:ok catalog handle, set once at start
	requestIDsNeverRepeat         = assert.NewAlways("sim: request ids never repeat")                                  //determinismcheck:ok catalog handle, set once at start
	runEnds                       = assert.NewAlways("sim: run ends")                                                  //determinismcheck:ok catalog handle, set once at start
	segmentListsNeverGoBackwards  = assert.NewAlways("sim: segment lists never go backwards")                          //determinismcheck:ok catalog handle, set once at start
)

// These two are recorded by dst/check (Checker.NoHole and check.Replay), which
// builds the name from the prefix "sim: " (see sim.check). The check code does not
// use a handle yet, so the names are declared here only to put them in the catalog.
var (
	_ = assert.NewAlways("sim: log has no hole")
	_ = assert.NewAlways("sim: log entries decode")
)
