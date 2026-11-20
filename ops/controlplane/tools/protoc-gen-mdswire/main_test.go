package main

import "testing"

func TestGoName(t *testing.T) {
	for in, want := range map[string]string{
		"topic":             "Topic",
		"member_id":         "MemberID",
		"group_instance_id": "GroupInstanceID",
		"leader_epoch":      "LeaderEpoch",
		"identity":          "Identity",
	} {
		if got := goName(in); got != want {
			t.Errorf("goName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDoc(t *testing.T) {
	tests := []struct {
		comment, msg, goName, want string
	}{
		// The message name at the start becomes the Go name. Blank lines stay.
		{" LookupRequest finds segments.\n\n More.\n", "LookupRequest", "FindSegments", "// FindSegments finds segments.\n//\n// More.\n"},
		// The same name: nothing to change.
		{" TopicInfo describes one topic.\n", "TopicInfo", "TopicInfo", "// TopicInfo describes one topic.\n"},
		// Only a whole first word is replaced.
		{" LookupRequestX is other.\n", "LookupRequest", "FindSegments", "// LookupRequestX is other.\n"},
		{" A LookupRequest asks.\n", "LookupRequest", "FindSegments", "// A LookupRequest asks.\n"},
		// No comment gives no doc.
		{"", "LookupRequest", "FindSegments", ""},
	}
	for _, tt := range tests {
		if got := doc(tt.comment, tt.msg, tt.goName); got != tt.want {
			t.Errorf("doc(%q) = %q, want %q", tt.comment, got, tt.want)
		}
	}
}
