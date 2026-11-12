//go:build integration

// Package testinfra starts the real services the integration tests need:
// DynamoDB Local, PostgreSQL and SeaweedFS, in Docker, through testcontainers.
//
// Each service starts at most once per test package, the first time a test asks
// for it. Every test then gets its own fresh table, schema or bucket, so tests
// do not see each other's data.
//
// A test package calls Main from TestMain, to stop the containers at the end:
//
//	func TestMain(m *testing.M) { testinfra.Main(m) }
//
// If Docker is not running, the helpers fail the test. They never skip it.
// Only code under the integration build tag imports this package, so
// testcontainers never reaches the binary.
package testinfra

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// Image versions are pinned on purpose. Change them here only.
const (
	DynamoDBImage  = "amazon/dynamodb-local:3.1.0"
	PostgresImage  = "postgres:17.11-alpine"
	SeaweedFSImage = "chrislusf/seaweedfs:4.48"
)

// service is one shared container, started on first use.
type service struct {
	once sync.Once
	addr string // host:port of the exposed port
	err  error
}

var (
	mu      sync.Mutex
	started []testcontainers.Container
	counter int
	stamp   = time.Now().UnixNano() % 100000000
)

// start runs the container once and remembers its host:port.
func (s *service) start(req testcontainers.GenericContainerRequest, port string) (string, error) {
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		req.Started = true
		c, err := testcontainers.GenericContainer(ctx, req)
		if c != nil {
			mu.Lock()
			started = append(started, c)
			mu.Unlock()
		}
		if err != nil {
			s.err = fmt.Errorf("testinfra: cannot start %s (is Docker running?): %w", req.Image, err)
			return
		}
		host, err := c.Host(ctx)
		if err != nil {
			s.err = err
			return
		}
		p, err := c.MappedPort(ctx, port)
		if err != nil {
			s.err = err
			return
		}
		s.addr = fmt.Sprintf("%s:%s", host, p.Port())
	})
	return s.addr, s.err
}

// unique returns a name that no other test in this run uses. It is safe as a
// table name, a schema name and (after "_" becomes "-") a bucket name.
func unique(kind string) string {
	mu.Lock()
	counter++
	n := counter
	mu.Unlock()
	return fmt.Sprintf("t%d_%s_%d", stamp, kind, n)
}

// Main runs the tests of the package and then stops the containers that the
// tests started. Call it from TestMain. It exits the process.
func Main(m *testing.M) {
	code := m.Run()
	Stop()
	os.Exit(code)
}

// Stop terminates every container started so far. Main calls it; call it
// yourself only if you do not use Main. The ryuk reaper of testcontainers
// removes the containers anyway if the process dies before Stop.
func Stop() {
	mu.Lock()
	cs := started
	started = nil
	mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, c := range cs {
		if err := c.Terminate(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "testinfra: stop container:", err)
		}
	}
}
