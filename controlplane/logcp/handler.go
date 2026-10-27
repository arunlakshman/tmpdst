package logcp

import (
	"net/http"

	cp "manifester/controlplane"
)

// RoleHeader is set on every response: "active" or "standby". It is advisory
// (brokers may log it); a standby answers reads and refuses commands.
const RoleHeader = "X-CP-Role"

// Handler is cp.NewHandler(s) plus the role rules at the edge: a standby
// answers POST /v1/commit-segment and /v1/create-topic with 503 and
// Retry-After before the body is read, so brokers fail over to the next CP URL
// (cp/cpclient) without any work done here. Lookups and describes are served
// by any node: they are fresh reads. /healthz is 503 while the node is
// starting, stopped or cannot apply the log.
func (s *Service) Handler() http.Handler {
	inner := cp.NewHandler(s)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := "standby"
		if s.isActive() {
			role = "active"
		}
		w.Header().Set(RoleHeader, role)
		switch {
		case r.URL.Path == "/healthz":
			if err := s.Healthy(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		case role == "standby" && r.Method == http.MethodPost && isWrite(r.URL.Path):
			s.m.reject("not_active")
			w.Header().Set("Retry-After", "1")
			http.Error(w, "not active", http.StatusServiceUnavailable)
			return
		}
		inner.ServeHTTP(w, r)
	})
}

func isWrite(path string) bool { return path == "/v1/commit-segment" || path == "/v1/create-topic" }
