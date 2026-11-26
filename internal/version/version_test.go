package version

import (
	"runtime"
	"strings"
	"testing"
)

func TestDefaultValues(t *testing.T) {
	info := Get()

	if info.Version != "dev" {
		t.Errorf("default Version = %q, want %q", info.Version, "dev")
	}
	if info.GitCommit != "unknown" {
		t.Errorf("default GitCommit = %q, want %q", info.GitCommit, "unknown")
	}
	if info.GitTreeState != "unknown" {
		t.Errorf("default GitTreeState = %q, want %q", info.GitTreeState, "unknown")
	}
	if info.BuildDate != "unknown" {
		t.Errorf("default BuildDate = %q, want %q", info.BuildDate, "unknown")
	}
}

func TestGetReturnsRuntimeInfo(t *testing.T) {
	info := Get()

	if info.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", info.GoVersion, runtime.Version())
	}
	if info.Compiler != runtime.Compiler {
		t.Errorf("Compiler = %q, want %q", info.Compiler, runtime.Compiler)
	}
	if !strings.Contains(info.Platform, "/") {
		t.Errorf("Platform should contain '/', got %q", info.Platform)
	}
}

func TestOverrideValues(t *testing.T) {
	// Save originals
	origVersion := Version
	origCommit := GitCommit
	origTree := GitTreeState
	origDate := BuildDate

	// Restore after test
	defer func() {
		Version = origVersion
		GitCommit = origCommit
		GitTreeState = origTree
		BuildDate = origDate
	}()

	// Simulate ldflags override
	Version = "1.2.3"
	GitCommit = "abc123"
	GitTreeState = "clean"
	BuildDate = "2025-01-01T00:00:00Z"

	info := Get()

	if info.Version != "1.2.3" {
		t.Errorf("Version = %q, want %q", info.Version, "1.2.3")
	}
	if info.GitCommit != "abc123" {
		t.Errorf("GitCommit = %q, want %q", info.GitCommit, "abc123")
	}
	if info.GitTreeState != "clean" {
		t.Errorf("GitTreeState = %q, want %q", info.GitTreeState, "clean")
	}
	if info.BuildDate != "2025-01-01T00:00:00Z" {
		t.Errorf("BuildDate = %q, want %q", info.BuildDate, "2025-01-01T00:00:00Z")
	}
}

func TestInfoString(t *testing.T) {
	info := Get()
	s := info.String()

	if !strings.Contains(s, "Quota API Server") {
		t.Errorf("String() should contain 'Quota API Server', got %q", s)
	}
	if !strings.Contains(s, info.Version) {
		t.Errorf("String() should contain version %q, got %q", info.Version, s)
	}
	if !strings.Contains(s, info.GitCommit) {
		t.Errorf("String() should contain commit %q, got %q", info.GitCommit, s)
	}
	if !strings.Contains(s, info.GoVersion) {
		t.Errorf("String() should contain go version %q, got %q", info.GoVersion, s)
	}
	if !strings.Contains(s, info.Platform) {
		t.Errorf("String() should contain platform %q, got %q", info.Platform, s)
	}
}
