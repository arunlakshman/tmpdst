# Deploy

All deployment files of the repo are here.

| Folder | What it is |
|---|---|
| `helm/mdscontrolplane` | Helm chart for the control plane. |
| `container` | Containerfiles for the control plane, the broker, and the demo all-in-one image (`mds-all`). Also a compose file for the broker integration test backends (Podman). See `container/README.md`. |
