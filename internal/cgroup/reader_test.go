package cgroup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReadV2AndFindPod(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "kubepods-pod1234_5678.slice")
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "memory.current"), []byte("80\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "memory.max"), []byte("100\n"), 0644); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(root)
	found, err := reader.FindPod(context.Background(), "1234-5678")
	if err != nil || found != path {
		t.Fatalf("FindPod()=%q,%v", found, err)
	}
	reading, err := reader.Read(context.Background(), path)
	if err != nil || reading.Usage != 80 || reading.Limit != 100 {
		t.Fatalf("Read()=%#v,%v", reading, err)
	}
}
func TestReadV1AndUnlimited(t *testing.T) {
	path := t.TempDir()
	_ = os.WriteFile(filepath.Join(path, "memory.usage_in_bytes"), []byte("1"), 0644)
	_ = os.WriteFile(filepath.Join(path, "memory.limit_in_bytes"), []byte("9223372036854771712"), 0644)
	_, err := NewReader(t.TempDir()).Read(context.Background(), path)
	if !errors.Is(err, ErrUnlimited) {
		t.Fatalf("error=%v", err)
	}
}

func TestSignalTERMMultipleContainerDescendants(t *testing.T) {
	root := t.TempDir()
	paths := []string{filepath.Join(root, "container-a"), filepath.Join(root, "container-b")}
	commands := make([]*exec.Cmd, 0, len(paths))
	for _, path := range paths {
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("sleep", "30")
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
		t.Cleanup(func() { _ = command.Process.Kill() })
		pid := strconv.Itoa(command.Process.Pid)
		if err := os.WriteFile(filepath.Join(path, "cgroup.procs"), []byte(pid+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := NewReader(root).SignalTERM(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		err := command.Wait()
		if err == nil || !strings.Contains(err.Error(), "signal: terminated") {
			t.Fatalf("process was not terminated by SIGTERM: %v", err)
		}
	}
}
