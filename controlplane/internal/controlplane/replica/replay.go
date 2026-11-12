package replica

import (
	"errors"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
)

// EntryCommands returns the commands that one log entry carries, in the order
// they are applied. A heartbeat or fence entry gives one no-op command. The
// simulator uses it to replay the log into the spec.
func EntryCommands(data []byte) ([]metadb.Entry, error) {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return nil, errors.New("replica: truncated log envelope")
	}
	if _, cmds, ok := decodeGroup(payload); ok {
		return cmds, nil
	}
	e, err := metadb.Decode(payload)
	if err != nil {
		return nil, err
	}
	return []metadb.Entry{e}, nil
}
