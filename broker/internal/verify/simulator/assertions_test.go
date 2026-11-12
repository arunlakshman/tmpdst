package simulator

import "github.com/manifesterio/mds/dst/assert"

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/assert. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	alwaysBroken           = assert.NewAlways("always broken")
	echoLostResponse       = assert.NewSometimes("echo lost a response")
	endReached             = assert.NewReachable("end reached")
	engineAppliedSomething = assert.NewAlways("engine applied something")
	fine                   = assert.NewAlways("fine")
	historyExists          = assert.NewAlways("history exists")
	hit                    = assert.NewReachable("hit")
	neverTrue              = assert.NewSometimes("never true")
	oneCrashFact           = assert.NewAlways("one crash fact")
	pingsHappen            = assert.NewSometimes("pings happen")
	putsComplete           = assert.NewAlways("puts complete")
	restartedOnce          = assert.NewAlways("restarted once")
	sawPings               = assert.NewAlways("saw pings")
)
