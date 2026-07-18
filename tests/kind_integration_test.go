//go:build integration

package tests

import (
	"os"
	"os/exec"
	"testing"
)

func TestKindBreachFlow(t *testing.T) {
	if os.Getenv("SENTINEL_KIND_TEST") != "1" {
		t.Skip("set SENTINEL_KIND_TEST=1 to run destructive Kind integration test")
	}
	command := exec.Command("bash", "../scripts/kind-integration.sh")
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
}
