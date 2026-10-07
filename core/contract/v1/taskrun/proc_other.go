//go:build !unix

package taskrun

import "os/exec"

func setProcessGroup(*exec.Cmd) {}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func termGroup(cmd *exec.Cmd) { killGroup(cmd) }
