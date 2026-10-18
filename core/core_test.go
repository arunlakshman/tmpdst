package core

import "testing"

func TestClassOfCoversEveryEvent(t *testing.T) {
	control := []EventBody{
		Started{}, Stop{}, TimerFired{}, SendDone{}, DialDone{},
		ObjPutDone{}, ObjGetDone{}, ObjDeleteDone{}, ObjListDone{}, CPDone{},
	}
	data := []EventBody{ConnOpened{}, BytesReceived{}, ConnClosed{}}
	for _, b := range control {
		if ClassOf(b) != Control {
			t.Errorf("%T: want Control", b)
		}
	}
	for _, b := range data {
		if ClassOf(b) != Data {
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
