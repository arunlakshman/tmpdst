//go:build integration

package testinfra

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

var (
	podmanOnce sync.Once
	podmanErr  error
)

// ensurePodman makes testcontainers talk only to Podman: DOCKER_HOST points at
// the Podman API socket, the default network is "podman", and Ryuk is disabled
// (needed on rootless / macOS machine setups where the reaper cannot reach the
// socket). No docker binary or docker->podman symlink is required.
func ensurePodman() error {
	podmanOnce.Do(func() {
		podmanErr = configurePodman()
	})
	return podmanErr
}

func configurePodman() error {
	if _, err := exec.LookPath("podman"); err != nil {
		return fmt.Errorf("testinfra: podman not found on PATH (this project uses Podman only): %w", err)
	}
	if err := exec.Command("podman", "info").Run(); err != nil {
		return fmt.Errorf("testinfra: Podman is not running (or the Podman socket is not reachable). Start Podman and run again: %w", err)
	}

	sock, err := podmanAPISocket()
	if err != nil {
		return err
	}
	if err := os.Setenv("DOCKER_HOST", sock); err != nil {
		return fmt.Errorf("testinfra: set DOCKER_HOST: %w", err)
	}
	// Ryuk expects Docker's "bridge" network / a reachable docker.sock inside the
	// reaper. Encode the known-good Podman path: we Stop() containers ourselves.
	if err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"); err != nil {
		return fmt.Errorf("testinfra: set TESTCONTAINERS_RYUK_DISABLED: %w", err)
	}
	return nil
}

// podmanAPISocket returns a unix:// DOCKER_HOST for the local Podman API.
// On macOS the usable socket is on the host (podman machine); RemoteSocket from
// `podman info` is the path inside the VM and is wrong for DOCKER_HOST.
func podmanAPISocket() (string, error) {
	if out, err := exec.Command("podman", "machine", "inspect", "--format", "{{.ConnectionInfo.PodmanSocket.Path}}").Output(); err == nil {
		path := strings.TrimSpace(string(out))
		// machine inspect may print a blank line when there is no machine (Linux).
		if path != "" && path != "<no value>" {
			path = strings.TrimPrefix(path, "unix://")
			if st, err := os.Stat(path); err == nil && !st.IsDir() {
				return "unix://" + path, nil
			}
		}
	}

	out, err := exec.Command("podman", "info", "--format", "{{.Host.RemoteSocket.Path}}").Output()
	if err != nil {
		return "", fmt.Errorf("testinfra: cannot discover Podman API socket: %w", err)
	}
	path := strings.TrimSpace(string(out))
	if path == "" {
		return "", fmt.Errorf("testinfra: Podman API socket path is empty")
	}
	if !strings.HasPrefix(path, "unix://") {
		path = "unix://" + path
	}
	fsPath := strings.TrimPrefix(path, "unix://")
	if st, err := os.Stat(fsPath); err != nil || st.IsDir() {
		// Last resort: XDG runtime dir (rootless Linux).
		if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
			candidate := filepath.Join(xdg, "podman", "podman.sock")
			if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
				return "unix://" + candidate, nil
			}
		}
		return "", fmt.Errorf("testinfra: Podman API socket not found at %s (is the Podman machine running?)", fsPath)
	}
	return path, nil
}

// pullImage fetches image with `podman pull` before the container-start timer.
// Cold pulls can take longer than the start budget; warm pulls are fast.
func pullImage(image string) error {
	cmd := exec.Command("podman", "pull", image)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}
