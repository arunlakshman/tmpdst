package core

import "testing"

func TestClassOfCoversEveryEvent(t *testing.T) {
	control := []EventData{
		NodeStarted{}, StopRequest{}, TimerFired{}, SendDone{}, ConnectDone{},
		ObjectPutDone{}, ObjectGetDone{}, ObjectDeleteDone{}, ObjectListDone{}, ControlPlaneDone{},
		LogAppendDone{}, LogReadDone{}, LogGetEndDone{}, LogDeleteOldDone{},
	}
	data := []EventData{ConnOpened{}, BytesReceived{}, ConnClosed{}}
	for _, b := range control {
		if InboxKindOf(b) != Control {
			t.Errorf("%T: want Control", b)
		}
	}
	for _, b := range data {
		if InboxKindOf(b) != Data {
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
		ErrCode(999): "ErrCode(999)",
	}
	for c, s := range want {
		if c.String() != s {
			t.Errorf("%d: got %q want %q", uint16(c), c.String(), s)
		}
	}
}
