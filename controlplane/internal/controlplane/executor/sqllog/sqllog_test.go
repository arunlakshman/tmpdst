package sqllog_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/sqllog"
)

func TestSQLLogRejectsABadTablePrefix(t *testing.T) {
	for _, bad := range []string{"Bad", "1abc", "a-b", "a b", "x;drop table y", "ab\"c"} {
		if _, err := sqllog.New(nil, bad); err == nil {
			t.Errorf("prefix %q was accepted", bad)
		}
	}
	for _, ok := range []string{"", "mds_", "cp1_"} {
		if _, err := sqllog.New(nil, ok); err != nil {
			t.Errorf("prefix %q was rejected: %v", ok, err)
		}
	}
}
