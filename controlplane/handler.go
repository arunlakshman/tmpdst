package controlplane

// JSON HTTP /v1/* handlers were removed in CP-DSM-7. manicp serves gRPC only
// (controlplane/server). Brokers dial via controlplane/cpclient.
