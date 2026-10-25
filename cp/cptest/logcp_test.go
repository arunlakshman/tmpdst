package cptest

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/cp/cpclient"
	"manifester/cp/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
)

// Conformance C3 for the log-backed production service (stage 2): the same
// random operations against logcp.Service as a cp.Store, and over HTTP through
// its handler and cpclient, against a fresh veritas.ControlPlane.

func TestConformanceLogCPMem(t *testing.T) {
	run(t, func() impl { return storeImpl{logcptest.NewMem(t)} }, 200, 100)
}

func TestConformanceLogCPHTTP(t *testing.T) {
	run(t, func() impl { return logcpHTTP(t, logcptest.NewMem(t)) }, 100, 40)
}

// logcpHTTP drives s through its own handler (role rules included) and cpclient.
func logcpHTTP(t testing.TB, s *logcp.Service) impl {
	srv := logcptest.Server(t, s)
	return httpImpl{cpclient.New(srv.URL, srv.Client())}
}

// fakeDBService is a service over a DynLog on a fresh fakedb table, with the
// index and checkpoint parts configured.
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

func TestConformanceLogCPFakeDBHTTP(t *testing.T) {
	run(t, func() impl { return logcpHTTP(t, fakeDBService(t)) }, 60, 10)
}
