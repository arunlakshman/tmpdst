// Package executor executes control-plane IOCalls outside Step: Dynamo log
// slots and object-store checkpoints. AWS SDK types stay here so
// controlplane/replica stays free of them (decision 56).
package executor
