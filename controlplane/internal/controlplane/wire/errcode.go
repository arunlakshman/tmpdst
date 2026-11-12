package wire

import (
	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// errCodePairs is the one list that ties each runtime error code to its wire code.
// Both directions read it. To add a code, add one line here, plus the new
// constant in runtime and the new value in the proto enum. TestErrCodeTableIsComplete
// fails if a code is missing. The names of the two sides differ (ErrTimeout and
// ERR_CODE_TIMEOUT), so this table is the one thing that is not derived from the
// .proto.
var errCodePairs = [...]struct {
	core runtime.ErrCode
	wire cpv1.ErrCode
}{
	{runtime.OK, cpv1.ErrCode_ERR_CODE_OK},
	{runtime.ErrTimeout, cpv1.ErrCode_ERR_CODE_TIMEOUT},
	{runtime.ErrUnavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
	{runtime.ErrNotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
	{runtime.ErrConflict, cpv1.ErrCode_ERR_CODE_CONFLICT},
	{runtime.ErrConnClosed, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
	{runtime.ErrTooLarge, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
	{runtime.ErrCorrupt, cpv1.ErrCode_ERR_CODE_CORRUPT},
	{runtime.ErrInvalid, cpv1.ErrCode_ERR_CODE_INVALID},
	{runtime.ErrIO, cpv1.ErrCode_ERR_CODE_IO},
	{runtime.ErrUnknownMember, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
	{runtime.ErrIllegalGeneration, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
	{runtime.ErrUnknownPartition, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
	{runtime.ErrRebalanceInProgress, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
	{runtime.ErrInconsistentGroupProtocol, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
	{runtime.ErrTopicExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
}

// ErrCodeToCore maps a wire ErrCode to runtime.ErrCode. Unknown values become ErrIO.
func ErrCodeToCore(c cpv1.ErrCode) runtime.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return runtime.ErrIO
}

// ErrCodeFromCore maps runtime.ErrCode to the wire enum. Unknown values become ERR_CODE_IO.
func ErrCodeFromCore(c runtime.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}
