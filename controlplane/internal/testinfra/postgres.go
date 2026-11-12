//go:build integration

package testinfra

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // the "pgx" driver
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var postgresSvc service

// PostgresInfo says how to reach one fresh PostgreSQL schema.
type PostgresInfo struct {
	// DSN opens the new schema: its search_path is Schema, so table names
	// without a schema (as sqllog uses them) land in the new schema.
	DSN    string
	Schema string
}

// Postgres starts (once per package) a PostgreSQL container and returns a DSN
// for a new empty schema. Pass the DSN to sqllog.OpenPostgres. The schema is
// dropped when the test ends. It fails the test if Docker is not running.
func Postgres(t testing.TB) PostgresInfo {
	t.Helper()
	const user, pass, db = "postgres", "postgres", "postgres"
	addr, err := postgresSvc.start(testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image:        PostgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": user, "POSTGRES_PASSWORD": pass, "POSTGRES_DB": db},
		// Postgres restarts once while it initialises, so it logs this line twice.
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}}, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, addr, db)
	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := unique("schema")
	if _, err := admin.ExecContext(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("testinfra: create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		c, err := sql.Open("pgx", base)
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})
	return PostgresInfo{DSN: base + "&search_path=" + schema, Schema: schema}
}
