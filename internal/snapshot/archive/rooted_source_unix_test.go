//go:build linux

/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package archive

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

const linuxMountHelperScenario = "D8_ARCHIVE_MOUNT_HELPER"

type testArchiveFSID struct {
	Val [2]int32
}

type testArchiveMountStat struct {
	Fsid        testArchiveFSID
	Mntonname   [64]byte
	Mntfromname [64]byte
	Fstypename  [16]byte
}

func TestArchiveMountIdentityValidation(t *testing.T) {
	valid := testArchiveMountStat{
		Fsid: testArchiveFSID{Val: [2]int32{17, 23}},
	}
	setTestArchiveMountField(t, valid.Mntonname[:], "/archive")
	setTestArchiveMountField(t, valid.Mntfromname[:], "/dev/disk0s1")
	setTestArchiveMountField(t, valid.Fstypename[:], "apfs")

	tests := []struct {
		name      string
		mutate    func(stat *testArchiveMountStat)
		wantError error
	}{
		{
			name:   "same mount",
			mutate: func(_ *testArchiveMountStat) {},
		},
		{
			name: "different filesystem ID",
			mutate: func(stat *testArchiveMountStat) {
				stat.Fsid.Val = [2]int32{19, 29}
			},
			wantError: ErrNonRegularArchiveArtifact,
		},
		{
			name: "colliding filesystem ID on different mount",
			mutate: func(stat *testArchiveMountStat) {
				setTestArchiveMountField(t, stat.Mntonname[:], "/archive/nested")
			},
			wantError: ErrNonRegularArchiveArtifact,
		},
		{
			name: "zero filesystem ID",
			mutate: func(stat *testArchiveMountStat) {
				stat.Fsid.Val = [2]int32{}
			},
			wantError: ErrArchiveMountBoundaryUnsupported,
		},
		{
			name: "empty mount point",
			mutate: func(stat *testArchiveMountStat) {
				stat.Mntonname = [64]byte{}
			},
			wantError: ErrArchiveMountBoundaryUnsupported,
		},
		{
			name: "malformed unterminated mount point",
			mutate: func(stat *testArchiveMountStat) {
				for index := range stat.Mntonname {
					stat.Mntonname[index] = 'x'
				}
			},
			wantError: ErrArchiveMountBoundaryUnsupported,
		},
	}

	parent, err := archiveMountIdentityFromStat(valid)
	if err != nil {
		t.Fatalf("archiveMountIdentityFromStat parent: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			childStat := valid
			tc.mutate(&childStat)

			child, childErr := archiveMountIdentityFromStat(childStat)
			if childErr == nil {
				childErr = verifyArchiveMountIdentities(parent, child, "archive/payload")
			}

			if tc.wantError == nil {
				if childErr != nil {
					t.Fatalf("mount identity verification: %v", childErr)
				}

				return
			}

			if !errors.Is(childErr, tc.wantError) {
				t.Fatalf("mount identity error = %v, want %v", childErr, tc.wantError)
			}
		})
	}
}

func TestArchiveMountIdentityUnavailableClosesChildDescriptor(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "payload"), "inside")

	parent, err := os.Open(root)
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	defer func() { _ = parent.Close() }()

	calls := 0
	childFD := -1
	mountStat := func(fd int) (any, error) {
		calls++
		if calls == 1 {
			return newTestArchiveMountStat(t, [2]int32{17, 23}, "/archive"), nil
		}

		childFD = fd

		return nil, errors.New("descriptor mount statistics unavailable")
	}

	file, err := openArchiveAtUnix(parent, "payload", filepath.Join(root, "payload"), false, mountStat)
	if file != nil {
		_ = file.Close()
	}

	if !errors.Is(err, ErrArchiveMountBoundaryUnsupported) {
		t.Fatalf("openArchiveAtUnix error = %v, want ErrArchiveMountBoundaryUnsupported", err)
	}

	if childFD < 0 {
		t.Fatal("child descriptor was not inspected")
	}

	if closeErr := unix.Close(childFD); !errors.Is(closeErr, unix.EBADF) {
		t.Fatalf("rejected child descriptor remained open: close error = %v", closeErr)
	}
}

func newTestArchiveMountStat(t *testing.T, fsID [2]int32, mountPoint string) testArchiveMountStat {
	t.Helper()

	stat := testArchiveMountStat{
		Fsid: testArchiveFSID{Val: fsID},
	}
	setTestArchiveMountField(t, stat.Mntonname[:], mountPoint)
	setTestArchiveMountField(t, stat.Mntfromname[:], "/dev/disk0s1")
	setTestArchiveMountField(t, stat.Fstypename[:], "apfs")

	return stat
}

func setTestArchiveMountField(t *testing.T, field []byte, value string) {
	t.Helper()

	clear(field)
	if len(value) >= len(field) {
		t.Fatalf("test mount field %q is too long", value)
	}

	copy(field, value)
}

func TestLinuxArchiveOpenUsesKernelMountBoundaryPolicy(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "payload"), "inside")

	parent, err := os.Open(root)
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	defer func() { _ = parent.Close() }()

	var captured unix.OpenHow
	openat2 := func(dirfd int, path string, how *unix.OpenHow) (int, error) {
		captured = *how

		return unix.Openat(dirfd, path, int(how.Flags), 0)
	}

	file, err := openArchiveAtLinux(parent, "payload", filepath.Join(root, "payload"), false,
		openat2, unix.Openat, readLinuxMountID)
	if err != nil {
		t.Fatalf("openArchiveAtLinux: %v", err)
	}
	defer func() { _ = file.Close() }()

	if captured.Resolve != archiveResolveFlags {
		t.Errorf("resolve flags = %#x, want %#x", captured.Resolve, uint64(archiveResolveFlags))
	}

	required := uint64(unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK)
	if captured.Flags&required != required {
		t.Errorf("open flags = %#x, missing %#x", captured.Flags, required)
	}
}

func TestLinuxArchiveOpenFallbackValidatesMountIdentity(t *testing.T) {
	tests := []struct {
		name      string
		mountID   archiveMountIDFunc
		wantError error
	}{
		{
			name: "same mount",
			mountID: func(_ int) (uint64, error) {
				return 17, nil
			},
		},
		{
			name: "different mount",
			mountID: func(fd int) (uint64, error) {
				if fd == -1 {
					return 0, errors.New("unreachable")
				}

				return uint64(fd), nil
			},
			wantError: ErrNonRegularArchiveArtifact,
		},
		{
			name: "unsupported identity",
			mountID: func(_ int) (uint64, error) {
				return 0, errors.New("mount metadata unavailable")
			},
			wantError: ErrArchiveMountBoundaryUnsupported,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "payload"), "inside")

			parent, err := os.Open(root)
			if err != nil {
				t.Fatalf("open parent: %v", err)
			}
			defer func() { _ = parent.Close() }()

			childFD := -1
			openat := func(dirfd int, path string, flags int, mode uint32) (int, error) {
				fd, openErr := unix.Openat(dirfd, path, flags, mode)
				childFD = fd

				return fd, openErr
			}
			unavailableOpenat2 := func(_ int, _ string, _ *unix.OpenHow) (int, error) {
				return -1, unix.ENOSYS
			}

			file, err := openArchiveAtLinux(parent, "payload", filepath.Join(root, "payload"), false,
				unavailableOpenat2, openat, tc.mountID)
			if file != nil {
				_ = file.Close()
			}

			if tc.wantError == nil {
				if err != nil {
					t.Fatalf("openArchiveAtLinux: %v", err)
				}

				return
			}

			if !errors.Is(err, tc.wantError) {
				t.Fatalf("openArchiveAtLinux error = %v, want %v", err, tc.wantError)
			}

			if childFD < 0 {
				t.Fatal("fallback did not open a child descriptor")
			}

			if closeErr := unix.Close(childFD); !errors.Is(closeErr, unix.EBADF) {
				t.Fatalf("rejected child descriptor remained open: close error = %v", closeErr)
			}
		})
	}
}

func TestLinuxArchiveOpenDoesNotFallbackOnPolicyRejection(t *testing.T) {
	root := t.TempDir()

	parent, err := os.Open(root)
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	defer func() { _ = parent.Close() }()

	for _, openat2Err := range []error{unix.EXDEV, unix.ELOOP, unix.EINVAL, unix.EPERM} {
		t.Run(openat2Err.Error(), func(t *testing.T) {
			fallbackCalls := 0
			openat := func(_ int, _ string, _ int, _ uint32) (int, error) {
				fallbackCalls++

				return -1, errors.New("unexpected fallback")
			}
			openat2 := func(_ int, _ string, _ *unix.OpenHow) (int, error) {
				return -1, openat2Err
			}

			_, err := openArchiveAtLinux(parent, "payload", filepath.Join(root, "payload"), false,
				openat2, openat, readLinuxMountID)
			if err == nil {
				t.Fatal("openArchiveAtLinux succeeded")
			}

			if fallbackCalls != 0 {
				t.Fatalf("fallback calls = %d, want 0", fallbackCalls)
			}

			if errors.Is(openat2Err, unix.EXDEV) || errors.Is(openat2Err, unix.ELOOP) {
				if !errors.Is(err, ErrNonRegularArchiveArtifact) {
					t.Fatalf("openArchiveAtLinux error = %v, want ErrNonRegularArchiveArtifact", err)
				}
			}
		})
	}
}

func TestParseLinuxMountID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      uint64
		wantError bool
	}{
		{name: "valid", input: "pos:\t0\nmnt_id:\t42\nflags:\t0100000\n", want: 42},
		{name: "malformed", input: "mnt_id:\tnot-a-number\n", wantError: true},
		{name: "absent", input: "pos:\t0\nflags:\t0100000\n", wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLinuxMountID(strings.NewReader(tc.input))
			if tc.wantError {
				if err == nil {
					t.Fatalf("parseLinuxMountID succeeded with %d", got)
				}

				return
			}

			if err != nil {
				t.Fatalf("parseLinuxMountID: %v", err)
			}

			if got != tc.want {
				t.Errorf("mount ID = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLinuxMountedDescendantsCannotEscapeChecksum(t *testing.T) {
	for _, scenario := range []string{"directory", "regular-file"} {
		t.Run(scenario, func(t *testing.T) {
			runLinuxMountHelper(t, scenario)
		})
	}
}

func TestLinuxMountedArchiveEscapeHelper(t *testing.T) {
	scenario := os.Getenv(linuxMountHelperScenario)
	if scenario == "" {
		return
	}

	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		fmt.Printf("mount namespace unavailable: %v\n", err)

		return
	}

	root := makeNodeDir(t)
	manifestDir := filepath.Join(root, ManifestsDirName)
	manifestPath := filepath.Join(manifestDir, "configmap_app.yaml")
	writeFile(t, manifestPath, "kind: ConfigMap\nmetadata:\n  name: inside\n")

	sourcePath := ""
	targetPath := ""

	switch scenario {
	case "directory":
		sourcePath = filepath.Join(t.TempDir(), "outside-manifests")
		if err := os.Mkdir(sourcePath, 0o755); err != nil {
			t.Fatalf("mkdir outside manifests: %v", err)
		}

		writeFile(t, filepath.Join(sourcePath, "configmap_app.yaml"),
			"kind: Secret\nmetadata:\n  name: escaped\n")
		targetPath = manifestDir
	case "regular-file":
		sourcePath = filepath.Join(t.TempDir(), "outside.yaml")
		writeFile(t, sourcePath, "kind: Secret\nmetadata:\n  name: escaped\n")
		targetPath = manifestPath
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}

	mounted := false
	var mountErr error

	source, err := OpenRootedSourceWithHook(root, func(path string) {
		if mounted || mountErr != nil || path != targetPath {
			return
		}

		mountErr = unix.Mount(sourcePath, targetPath, "", unix.MS_BIND, "")
		mounted = mountErr == nil
	})
	if err != nil {
		t.Fatalf("OpenRootedSourceWithHook: %v", err)
	}
	defer func() { _ = source.Close() }()

	_, err = computeNodeChecksum(source)
	if mountErr != nil {
		fmt.Printf("mount namespace unavailable: %v\n", mountErr)

		return
	}

	if !mounted {
		t.Fatal("mount hook was not reached")
	}
	defer func() { _ = unix.Unmount(targetPath, unix.MNT_DETACH) }()

	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("computeNodeChecksum error = %v, want ErrNonRegularArchiveArtifact", err)
	}
}

func runLinuxMountHelper(t *testing.T, scenario string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestLinuxMountedArchiveEscapeHelper$")
	cmd.Env = append(os.Environ(), linuxMountHelperScenario+"="+scenario)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: unix.CLONE_NEWUSER | unix.CLONE_NEWNS,
		UidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Getuid(),
			Size:        1,
		}},
		GidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Getgid(),
			Size:        1,
		}},
		GidMappingsEnableSetgroups: false,
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skipf("isolated mount namespace is unavailable: %v", err)
		}

		t.Fatalf("mount helper failed: %v\n%s", err, output)
	}

	if strings.Contains(string(output), "mount namespace unavailable:") {
		t.Skipf("isolated bind mounts are unavailable:\n%s", output)
	}
}
