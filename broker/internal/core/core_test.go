package core

import (
	"testing"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

func TestClassOfCoversEveryEvent(t *testing.T) {
	control := []EventData{
		NodeStarted{}, StopRequest{}, TimerFired{}, SendDone{}, ConnectDone{},
		ObjectDone{Result: objectstore.PutDone{}}, ObjectDone{Result: objectstore.GetDone{}}, ObjectDone{Result: objectstore.DeleteDone{}}, ObjectDone{Result: objectstore.ListDone{}}, ControlPlaneDone{},
	}
	data := []EventData{ConnOpened{}, BytesReceived{}, ConnClosed{}}
	for _, b := range control {
		if InboxKindOf(b) != machine.Control {
			t.Errorf("%T: want Control", b)
		}
	}
	for _, b := range data {
		if InboxKindOf(b) != machine.Data {
			t.Errorf("%T: want Data", b)
		}
	}
}

func TestErrCodeString(t *testing.T) {
	want := map[ErrCode]string{
		OK: "OK", ErrTimeout: "ErrTimeout", ErrUnavailable: "ErrUnavailable",
		ErrNotFound: "ErrNotFound", ErrConflict: "ErrConflict",
		ErrConnClosed: "ErrConnClosed", ErrTooLarge: "ErrTooLarge",
		ErrCorrupt: "ErrCorrupt", ErrInvalid: "ErrInvalid", ErrIO: "ErrIO",
		ErrUnknownMember: "ErrUnknownMember", ErrIllegalGeneration: "ErrIllegalGeneration",
		ErrUnknownPartition: "ErrUnknownPartition", ErrTopicExists: "ErrTopicExists",
		ErrCode(999): "ErrCode(999)",
	}
	for c, s := range want {
		if c.String() != s {
			t.Errorf("%d: got %q want %q", uint16(c), c.String(), s)
		}
	}
}
