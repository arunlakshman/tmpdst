package logcp

// HTTP Handler was removed in CP-DSM-7. Production manicp serves gRPC through
// controlplane/server over the machine run loop. Package tests drive Service
// as a Store directly.
