package cptest

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/controlplane/fakedb"
	"manifester/controlplane/logsm"
)

// Conformance C3 for the command-log prototype (cp/logsm, idea A): the same
// random operations against a replica applying a log and a fresh
// veritas.ControlPlane, every response compared. Not a production store.

func TestConformanceLogSMMem(t *testing.T) {
	run(t, func() impl { return storeImpl{logsm.NewMemStore(4)} }, 200, 100)
}

func TestConformanceLogSMFakeDB(t *testing.T) {
	run(t, func() impl {
		db := fakedb.New()
		if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("log")}); err != nil {
			t.Fatal(err)
		}
		r := logsm.NewReplica(logsm.Config{Log: logsm.NewDynLog(db, "log"), Writer: "w0"})
		return storeImpl{&logsm.Store{R: r, C: logsm.NewCommitter(r, 4)}}
	}, 100, 20)
}
