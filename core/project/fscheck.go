package project

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Network or user-space remote filesystems where SQLite locking and WAL are
// unreliable (ADR 0001).
var networkFS = map[string]bool{
	"nfs": true, "nfs4": true, "cifs": true, "smb3": true, "smbfs": true,
	"9p": true, "afs": true, "ceph": true, "glusterfs": true, "lustre": true,
	"davfs": true, "fuse.sshfs": true, "fuse.rclone": true, "fuse.s3fs": true,
	"fuse.gcsfuse": true, "fuse.davfs2": true, "fuse.glusterfs": true,
}

// Path fragments of folders synchronised by a cloud client (compared lowercase).
var syncedMarkers = []string{
	"dropbox", "onedrive", "icloud", "mobile documents", "google drive",
	"googledrive", "google-drive", "library/cloudstorage", "nextcloud",
	"owncloud", "syncthing", "megasync", "pcloud",
}

// CheckLocalFolder returns an error if path lives on a network filesystem or
// in a cloud-synchronised folder. The filesystem check reads
// /proc/self/mountinfo and is skipped where it does not exist.
func CheckLocalFolder(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if real, err := evalExisting(abs); err == nil {
		abs = real
	}
	if err := checkSyncedPath(abs); err != nil {
		return err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil // not Linux: no filesystem detection
	}
	defer func() { _ = f.Close() }()
	return checkMounts(abs, f)
}

// evalExisting resolves symlinks of the longest existing prefix of p.
func evalExisting(p string) (string, error) {
	rest := ""
	for cur := p; ; cur = filepath.Dir(cur) {
		if real, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(real, rest), nil
		}
		if filepath.Dir(cur) == cur {
			return p, nil
		}
		rest = filepath.Join(filepath.Base(cur), rest)
	}
}

func checkSyncedPath(abs string) error {
	lower := strings.ToLower(filepath.ToSlash(abs)) + "/"
	for _, m := range syncedMarkers {
		if strings.Contains(lower, "/"+m+"/") || strings.Contains(lower, "/"+m+" ") || strings.Contains(lower, "/"+m+"-") {
			return fmt.Errorf("%s looks like a cloud-synchronised folder (%q): SQLite can corrupt there", abs, m)
		}
	}
	return nil
}

// checkMounts finds the mount that contains abs (longest mount point) in
// mountinfo format and rejects network filesystem types.
func checkMounts(abs string, mountinfo io.Reader) error {
	best, bestFS := "", ""
	sc := bufio.NewScanner(mountinfo)
	for sc.Scan() {
		// id parent major:minor root mountpoint opts [optional...] - fstype source super
		fields := strings.Fields(sc.Text())
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+1 >= len(fields) || len(fields) < 5 {
			continue
		}
		mp := unescapeMount(fields[4])
		if !isUnder(abs, mp) || len(mp) < len(best) {
			continue
		}
		best, bestFS = mp, fields[sep+1]
	}
	if networkFS[bestFS] {
		return fmt.Errorf("%s is on a network filesystem (%s mounted at %s): SQLite locking and WAL are unreliable there", abs, bestFS, best)
	}
	return sc.Err()
}

func isUnder(path, mount string) bool {
	if mount == "/" {
		return true
	}
	return path == mount || strings.HasPrefix(path, mount+"/")
}

// unescapeMount decodes the octal escapes (\040 for space) of mountinfo.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			var n int
			if _, err := fmt.Sscanf(s[i+1:i+4], "%03o", &n); err == nil {
				b.WriteByte(byte(n)) //nolint:gosec // 3 octal digits fit a byte
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
