# Control-plane OpenAPI spec

`controlplane.json` is the source of truth (OpenAPI 3.1). `controlplane.yaml` is generated from it,
because the repo takes no YAML dependency (decision I) and JSON is what the Go tests can parse.

    go run ./api/openapi/cmd/specgen          # regenerate controlplane.yaml after editing the JSON
    go run ./api/openapi/cmd/specgen -check   # fail if the YAML is stale

`go test ./api/...` (part of `go test ./...`) keeps the spec honest:

- the YAML equals the rendering of the JSON;
- every route registered in `cp/handler.go` is in the spec and vice versa;
- every schema matches its `cp` wire struct by reflection (JSON names, required = no `omitempty`, types, int formats);
- the `ErrCode` enum equals the `core.ErrCode` spellings `cp.ParseErrCode` accepts;
- the worked examples, listed in order in `x-replay`, are sent to `cp.NewHandler` over `cp/memstore` and the
  answers must equal the example responses. Every request example must be in `x-replay`.

A new wire type needs an entry in `wireTypes` in `spec_test.go`.

## Optional validation and browsing (Docker, pinned)

Not run by the build. The versions below were chosen but NOT pulled or run when this was written
(the Docker daemon was not running), so confirm the tags exist before relying on them.

    docker run --rm -v "$PWD/api/openapi:/spec:ro" redocly/cli:1.34.5 lint /spec/controlplane.yaml
    docker run --rm -p 8081:8080 -e SWAGGER_JSON=/spec/controlplane.yaml -v "$PWD/api/openapi:/spec:ro" swaggerapi/swagger-ui:v5.17.14
    docker run --rm -p 8082:80 -e SPEC_URL=controlplane.yaml -v "$PWD/api/openapi/controlplane.yaml:/usr/share/nginx/html/controlplane.yaml:ro" redocly/redoc:v2.1.5

Then open http://127.0.0.1:8081 (Swagger UI) or http://127.0.0.1:8082 (Redoc).
