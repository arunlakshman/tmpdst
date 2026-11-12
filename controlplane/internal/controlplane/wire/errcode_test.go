package wire

import (
	"strings"
	"testing"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Every wire code must map to a runtime code and back, so a new code cannot be forgotten on one side.
func TestErrCodeTableIsComplete(t *testing.T) {
	for number, name := range cpv1.ErrCode_name {
		code := cpv1.ErrCode(number)
		if back := ErrCodeFromCore(ErrCodeToCore(code)); back != code {
			t.Errorf("wire code %s comes back as %s", name, back)
		}
	}
	for code := runtime.OK; code <= runtime.ErrTopicExists; code++ {
		if strings.HasPrefix(code.String(), "ErrCode(") {
			continue // a retired number
		}
		if back := ErrCodeToCore(ErrCodeFromCore(code)); back != code {
			t.Errorf("core code %s comes back as %s", code, back)
		}
	}
	if got := ErrCodeToCore(cpv1.ErrCode(9999)); got != runtime.ErrIO {
		t.Errorf("unknown wire code = %v, want ErrIO", got)
	}
	if got := ErrCodeFromCore(runtime.ErrCode(9999)); got != cpv1.ErrCode_ERR_CODE_IO {
		t.Errorf("unknown core code = %v, want ERR_CODE_IO", got)
	}
}
