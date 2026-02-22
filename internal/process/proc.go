package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

type ManagedProcess struct {
	Name    string
	Command string
	Args    []string

	Cmd    *exec.Cmd
	Stdout io.ReadCloser
	Stderr io.ReadCloser
	Done   chan struct{}

	mu      sync.Mutex
	ExitErr error
	started bool
}

func NewManagedProcess(name string, command string, args []string) *ManagedProcess {
	return &ManagedProcess{
		Name:    name,
		Command: command,
		Args:    append([]string(nil), args...),
		Done:    make(chan struct{}),
	}
}

func (p *ManagedProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return fmt.Errorf("process %q already started", p.Name)
	}

	cmd := exec.CommandContext(ctx, p.Command, p.Args...)

	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start process %q: %w", p.Name, err)
	}

	p.Cmd = cmd
	p.Stdout = stdoutReader
	p.Stderr = stderrReader
	p.started = true

	go func() {
		err := cmd.Wait()
		_ = stdoutWriter.Close()
		_ = stderrWriter.Close()

		p.mu.Lock()
		p.ExitErr = err
		p.mu.Unlock()

		close(p.Done)
	}()

	return nil
}

func (p *ManagedProcess) Signal(sig os.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started || p.Cmd == nil || p.Cmd.Process == nil {
		return errors.New("process not running")
	}

	return p.Cmd.Process.Signal(sig)
}

func (p *ManagedProcess) Wait() error {
	<-p.Done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ExitErr
}

func (p *ManagedProcess) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started || p.Cmd == nil || p.Cmd.Process == nil || p.Cmd.ProcessState != nil {
		return false
	}

	return true
}
