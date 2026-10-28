// Package exec executes control-plane IOCalls outside Step: Dynamo log
// slots and object-store checkpoints. AWS SDK types stay here so
// controlplane/machine stays free of them (decision 56).
package exec
