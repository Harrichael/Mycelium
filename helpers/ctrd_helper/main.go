// ctrd_helper: line-delimited JSON bridge between the Mycelium node
// agent (Erlang) and containerd.
//
// Wire protocol (stdin requests, stdout responses + events, stderr
// logs). The Erlang side holds this process as an OS port and enforces
// sequential op issuance: at most one op is in flight.
//
// Implementation note: v1 delegates to the `ctr` CLI for simplicity.
// A later revision will replace this with containerd's native Go
// client and event streaming over gRPC; the on-wire line-JSON protocol
// does not change.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type request struct {
	Op        string            `json:"op"`
	ID        string            `json:"id,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Image     string            `json:"image,omitempty"`
	Cmd       []string          `json:"cmd,omitempty"`
	Env       []string          `json:"env,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	CPUMax    []int             `json:"cpu_max,omitempty"`
	MemMax    int64             `json:"mem_max,omitempty"`
	Signal    string            `json:"signal,omitempty"`
	TimeoutMs int               `json:"timeout_ms,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type response struct {
	Type  string      `json:"type"`            // "ack" | "error" | "event"
	Op    string      `json:"op,omitempty"`    // echo of request op
	ID    string      `json:"id,omitempty"`
	Kind  string      `json:"kind,omitempty"`  // event kind
	Items interface{} `json:"items,omitempty"` // list response
	Code  string      `json:"code,omitempty"`
	Msg   string      `json:"msg,omitempty"`
	ExitCode *int     `json:"exit_code,omitempty"`
	Sig   string      `json:"signal,omitempty"`
	At    string      `json:"at,omitempty"`
}

func main() {
	out := json.NewEncoder(os.Stdout)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var req request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			write(out, response{Type: "error", Code: "bad_request", Msg: err.Error()})
			continue
		}
		handle(out, req)
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "ctrd_helper: stdin scan error: %v\n", err)
		os.Exit(1)
	}
}

func write(enc *json.Encoder, r response) {
	if err := enc.Encode(r); err != nil {
		fmt.Fprintf(os.Stderr, "ctrd_helper: encode error: %v\n", err)
	}
}

func handle(out *json.Encoder, req request) {
	switch req.Op {
	case "ping":
		write(out, response{Type: "ack", Op: "ping"})

	case "create":
		if err := doCreate(req); err != nil {
			write(out, response{Type: "error", Op: "create", ID: req.ID, Code: "create_failed", Msg: err.Error()})
			return
		}
		write(out, response{Type: "ack", Op: "create", ID: req.ID})

	case "start":
		if err := doStart(req); err != nil {
			write(out, response{Type: "error", Op: "start", ID: req.ID, Code: "start_failed", Msg: err.Error()})
			return
		}
		go watchExit(out, req.Namespace, req.ID)
		write(out, response{Type: "ack", Op: "start", ID: req.ID})

	case "kill":
		if err := doKill(req); err != nil {
			write(out, response{Type: "error", Op: "kill", ID: req.ID, Code: "kill_failed", Msg: err.Error()})
			return
		}
		write(out, response{Type: "ack", Op: "kill", ID: req.ID})

	case "delete":
		// Reap any lingering task before removing the container —
		// containerd rejects container deletion while a task is still
		// attached (common after adopter picks up a stopped task that
		// was never reaped by watchExit). Both calls are best-effort;
		// "not found" just means already gone.
		_ = runCtrQuiet(req.Namespace, "tasks", "delete", req.ID)
		_ = runCtrQuiet(req.Namespace, "containers", "delete", req.ID)
		write(out, response{Type: "ack", Op: "delete", ID: req.ID})

	case "list":
		items, err := doList(req.Namespace)
		if err != nil {
			write(out, response{Type: "error", Op: "list", Code: "list_failed", Msg: err.Error()})
			return
		}
		write(out, response{Type: "ack", Op: "list", Items: items})

	default:
		write(out, response{Type: "error", Op: req.Op, Code: "unknown_op", Msg: "unknown op: " + req.Op})
	}
}

func doCreate(req request) error {
	if req.Namespace == "" {
		return fmt.Errorf("namespace required")
	}
	if !imagePresent(req.Namespace, req.Image) {
		if err := runCtr(req.Namespace, "images", "pull", req.Image); err != nil {
			return fmt.Errorf("image pull: %w", err)
		}
	}
	args := []string{"containers", "create"}
	for k, v := range req.Labels {
		args = append(args, "--label", k+"="+v)
	}
	if len(req.CPUMax) == 2 && req.CPUMax[1] > 0 {
		args = append(args, "--cpu-quota", fmt.Sprintf("%d", req.CPUMax[0]))
		args = append(args, "--cpu-period", fmt.Sprintf("%d", req.CPUMax[1]))
	}
	if req.MemMax > 0 {
		args = append(args, "--memory-limit", fmt.Sprintf("%d", req.MemMax))
	}
	for _, e := range req.Env {
		args = append(args, "--env", e)
	}
	if req.Cwd != "" {
		args = append(args, "--cwd", req.Cwd)
	}
	args = append(args, req.Image, req.ID)
	args = append(args, req.Cmd...)
	return runCtr(req.Namespace, args...)
}

func doStart(req request) error {
	return runCtr(req.Namespace, "tasks", "start", "-d", req.ID)
}

func doKill(req request) error {
	sig := req.Signal
	if sig == "" {
		sig = "SIGTERM"
	}
	if err := runCtr(req.Namespace, "tasks", "kill", "-s", sig, req.ID); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(req.TimeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		if !taskRunning(req.Namespace, req.ID) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	// escalate
	return runCtr(req.Namespace, "tasks", "kill", "-s", "SIGKILL", req.ID)
}

func doList(ns string) ([]map[string]interface{}, error) {
	if ns == "" {
		return nil, fmt.Errorf("namespace required")
	}
	// For v1, list returns what `ctr containers list -q` gives us. The
	// spec-label round-trip would need to parse `ctr containers info`
	// per entry; we'll fetch labels with a follow-up info call.
	idsOut, err := runCtrOut(ns, "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	var items []map[string]interface{}
	for _, id := range strings.Split(strings.TrimSpace(idsOut), "\n") {
		if id == "" {
			continue
		}
		items = append(items, map[string]interface{}{
			"id":         id,
			"status":     containerStatus(ns, id),
			"spec_label": containerSpecLabel(ns, id),
		})
	}
	return items, nil
}

func containerStatus(ns, id string) string {
	out, err := runCtrOut(ns, "tasks", "list")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == id {
			switch strings.ToUpper(fields[2]) {
			case "RUNNING":
				return "running"
			case "STOPPED":
				return "stopped"
			}
		}
	}
	return "unknown"
}

func containerSpecLabel(ns, id string) string {
	// Parse labels via `ctr containers info`. Format is JSON; we look
	// for the "mycelium.spec" label.
	out, err := runCtrOut(ns, "containers", "info", id)
	if err != nil {
		return ""
	}
	var info struct {
		Labels map[string]string `json:"Labels"`
	}
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		return ""
	}
	return info.Labels["mycelium.spec"]
}

func imagePresent(ns, image string) bool {
	out, err := runCtrOut(ns, "images", "list", "-q")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == image {
			return true
		}
	}
	return false
}

func taskRunning(ns, id string) bool {
	return containerStatus(ns, id) == "running"
}

// watchExit polls task status after a start and emits a task_exit event
// when the task transitions out of running. v2 will replace this with
// containerd's events API.
func watchExit(out *json.Encoder, ns, id string) {
	for {
		time.Sleep(200 * time.Millisecond)
		status := containerStatus(ns, id)
		if status == "running" {
			continue
		}
		// Reap the task: this simultaneously (a) releases containerd's
		// hold so the container can be recreated on restart, and
		// (b) surfaces the task's exit status as ctr's own exit code.
		code := reapTaskExitCode(ns, id)
		r := response{
			Type:     "event",
			Kind:     "task_exit",
			ID:       id,
			ExitCode: &code,
			At:       time.Now().UTC().Format(time.RFC3339Nano),
		}
		write(out, r)
		return
	}
}

// reapTaskExitCode deletes the task and returns its exit code.
//
// `ctr tasks delete` terminates with an exit status equal to the task's
// own exit status (containerd source: cmd/ctr/commands/tasks/delete.go).
// We rely on that: Go's exec.ExitError.ExitCode() gives us the task's
// exit code directly. When the delete itself fails (e.g. task already
// reaped by another call), ctr also exits non-zero, so we disambiguate
// via stderr and treat "not found" as a clean (already-reaped) exit.
func reapTaskExitCode(ns, id string) int {
	cmd := exec.Command("ctr", "-n", ns, "tasks", "delete", id)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return 0
	}
	se := stderr.String()
	if exitErr, ok := err.(*exec.ExitError); ok {
		if strings.Contains(se, "not found") || strings.Contains(se, "NotFound") {
			return 0
		}
		_, _ = os.Stderr.WriteString(se)
		return exitErr.ExitCode()
	}
	_, _ = os.Stderr.WriteString(se)
	return -1
}

func runCtr(ns string, args ...string) error {
	a := append([]string{"-n", ns}, args...)
	cmd := exec.Command("ctr", a...)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runCtrOut(ns string, args ...string) (string, error) {
	a := append([]string{"-n", ns}, args...)
	cmd := exec.Command("ctr", a...)
	out, err := cmd.Output()
	return string(out), err
}

// runCtrQuiet runs ctr without forwarding stderr. For best-effort calls
// where "not found" and similar conditions are expected and noisy.
func runCtrQuiet(ns string, args ...string) error {
	a := append([]string{"-n", ns}, args...)
	cmd := exec.Command("ctr", a...)
	return cmd.Run()
}
