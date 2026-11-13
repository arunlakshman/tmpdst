package replica

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	putNotAnsweredSameSlot        = assert.NewReachable("logcp-sim: Put not answered, same slot repeated")             //determinismcheck:ok catalog handle, set once at start
	putNeverWinsBelowTrimFloor    = assert.NewAlways("logcp-sim: a Put never wins a slot below the trim floor")        //determinismcheck:ok catalog handle, set once at start
	groupEntryUnderByteCap        = assert.NewAlways("logcp-sim: a group entry stays under the byte cap")              //determinismcheck:ok catalog handle, set once at start
	checkpointReadBadCRC          = assert.NewReachable("logcp-sim: checkpoint read had a bad CRC, read again")        //determinismcheck:ok catalog handle, set once at start
	epochClaimed                  = assert.NewReachable("logcp-sim: epoch claimed")                                    //determinismcheck:ok catalog handle, set once at start
	idempotentReplayAfterTakeover = assert.NewReachableNotExpected("logcp-sim: idempotent replay after takeover")      //determinismcheck:ok catalog handle, set once at start
	logTrimmedBelowCheckpoint     = assert.NewReachable("logcp-sim: log trimmed below a checkpoint")                   //determinismcheck:ok catalog handle, set once at start
	lostSlotRetriedLaterSlot      = assert.NewReachable("logcp-sim: lost slot retried at a later slot")                //determinismcheck:ok catalog handle, set once at start
	putRefusedBelowFloor          = assert.NewReachableNotExpected("logcp-sim: put refused at or below the floor")     //determinismcheck:ok catalog handle, set once at start
	replayedKeyAnsweredFromWindow = assert.NewReachableNotExpected("logcp-sim: replayed key answered from the window") //determinismcheck:ok catalog handle, set once at start
	replicaCrashed                = assert.NewReachable("logcp-sim: replica crashed")                                  //determinismcheck:ok catalog handle, set once at start
	replicaHitTrimmedSlot         = assert.NewReachable("logcp-sim: replica hit a trimmed slot")                       //determinismcheck:ok catalog handle, set once at start
	replicaRestoredFromCheckpoint = assert.NewReachable("logcp-sim: replica restored from a checkpoint")               //determinismcheck:ok catalog handle, set once at start
	replicaStalled                = assert.NewReachable("logcp-sim: replica stalled")                                  //determinismcheck:ok catalog handle, set once at start
	standbyTookOver               = assert.NewReachable("logcp-sim: standby took over")                                //determinismcheck:ok catalog handle, set once at start
	writerFencedNowReadOnly       = assert.NewReachable("logcp-sim: writer fenced, now read-only")                     //determinismcheck:ok catalog handle, set once at start
)
