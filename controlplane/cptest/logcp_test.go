package cptest

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/controlplane/fakedb"
	"manifester/controlplane/logcp"
	"manifester/controlplane/logcp/logcptest"
)

// Conformance for the log-backed production service as a Store.

func TestConformanceLogCPMem(t *testing.T) {
	run(t, func() impl { return storeImpl{logcptest.NewMem(t)} }, 200, 100)
}

func fakeDBService(t testing.TB) *logcp.Service {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	cfg := logcptest.FastConfig("n0", 0, logcp.NewDynLog(db, "cp", logcp.DynLogOptions{}))
	cfg.DB, cfg.Table, cfg.Objects = db, "cp", logcptest.NewObjects()
	return logcptest.Start(t, cfg)
}

func TestConformanceLogCPFakeDB(t *testing.T) {
	run(t, func() impl { return storeImpl{fakeDBService(t)} }, 100, 20)
}
