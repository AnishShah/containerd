/*
   Copyright The containerd Authors.

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

package client

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/containerd/api/types/runc/options"
	. "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

// TestDaemonRuntimeRoot ensures plugin.linux.runtime_root is not ignored
func TestDaemonRuntimeRoot(t *testing.T) {
	runtimeRoot := t.TempDir()
	configTOML := `
version = 2
[plugins]
 [plugins."io.containerd.grpc.v1.cri"]
   stream_server_port = "0"
`

	client, _, cleanup := newDaemonWithConfig(t, configTOML)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()
	// FIXME(AkihiroSuda): import locally frozen image?
	image, err := client.Pull(ctx, testImage, WithPullUnpack)
	if err != nil {
		t.Fatal(err)
	}

	id := t.Name()
	container, err := client.NewContainer(ctx, id, WithNewSnapshot(id, image), WithNewSpec(oci.WithImageConfig(image), withProcessArgs("top")), WithRuntime(plugins.RuntimeRuncV2, &options.Options{
		Root: runtimeRoot,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer container.Delete(ctx, WithSnapshotCleanup)

	task, err := container.NewTask(ctx, empty())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Delete(ctx)

	status, err := task.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	containerPath := filepath.Join(runtimeRoot, testNamespace, id)
	if _, err = os.Stat(containerPath); err != nil {
		t.Errorf("error while getting stat for %s: %v", containerPath, err)
	}

	if err = task.Kill(ctx, syscall.SIGKILL); err != nil {
		t.Error(err)
	}
	<-status
}

// code most copy from https://github.com/opencontainers/runc
func getCgroupPath() (map[string]string, error) {
	cgroupPath := make(map[string]string)
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		text := scanner.Text()
		fields := strings.Split(text, " ")
		// Safe as mountinfo encodes mountpoints with spaces as \040.
		_, after, _ := strings.Cut(text, " - ")
		postSeparatorFields := strings.Fields(after)
		numPostFields := len(postSeparatorFields)

		// This is an error as we can't detect if the mount is for "cgroup"
		if numPostFields == 0 {
			continue
		}

		if postSeparatorFields[0] == "cgroup" {
			// Check that the mount is properly formatted.
			if numPostFields < 3 {
				continue
			}
			cgroupPath[filepath.Base(fields[4])] = fields[4]
		}
	}

	return cgroupPath, nil
}

// TestDaemonCustomCgroup ensures plugin.cgroup.path is not ignored
func TestDaemonCustomCgroup(t *testing.T) {
	if cgroups.Mode() == cgroups.Unified {
		t.Skip("test requires cgroup1")
	}
	cgroupPath, err := getCgroupPath()
	if err != nil {
		t.Fatal(err)
	}
	if len(cgroupPath) == 0 {
		t.Skip("skip TestDaemonCustomCgroup since no cgroup path available")
	}

	customCgroup := strconv.Itoa(time.Now().Nanosecond())
	configTOML := `
version = 2
[cgroup]
  path = "` + customCgroup + `"`

	_, _, cleanup := newDaemonWithConfig(t, configTOML)

	defer func() {
		// do cgroup path clean
		for _, v := range cgroupPath {
			if _, err := os.Stat(filepath.Join(v, customCgroup)); err == nil {
				if err := os.RemoveAll(filepath.Join(v, customCgroup)); err != nil {
					t.Logf("failed to remove cgroup path %s", filepath.Join(v, customCgroup))
				}
			}
		}
	}()

	defer cleanup()

	paths := []string{
		"devices",
		"memory",
		"cpu",
		"blkio",
	}

	for _, p := range paths {
		v := cgroupPath[p]
		if v == "" {
			continue
		}
		path := filepath.Join(v, customCgroup)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				t.Fatalf("custom cgroup path %s should exist, actually not", path)
			}
		}
	}
}

func TestDaemonSecondaryRoots(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()

	preloadSecondaryRoot := func(t *testing.T, rootDir string) string {
		t.Helper()
		preloadConfig := fmt.Sprintf(`
version = 4
root = %q
[plugins."io.containerd.cri.v1.runtime"]
  stream_server_port = "0"
`, rootDir)
		preloadClient, _, preloadCleanup := newDaemonWithConfig(t, preloadConfig)
		defer preloadCleanup()

		img, err := preloadClient.Pull(ctx, testImage, WithPullUnpack)
		require.NoError(t, err)
		return filepath.Join(
			rootDir,
			"io.containerd.content.v1.content",
			"blobs",
			img.Target().Digest.Algorithm().String(),
			img.Target().Digest.Encoded(),
		)
	}

	runContainerFromImage := func(t *testing.T, client *Client, img Image, id string) {
		t.Helper()
		container, err := client.NewContainer(
			ctx,
			id,
			WithNewSnapshot(id, img),
			WithNewSpec(oci.WithImageConfig(img), withExitStatus(0)),
		)
		require.NoError(t, err)
		defer container.Delete(ctx, WithSnapshotCleanup)

		task, err := container.NewTask(ctx, empty())
		require.NoError(t, err)
		defer task.Delete(ctx)

		statusC, err := task.Wait(ctx)
		require.NoError(t, err)

		require.NoError(t, task.Start(ctx))
		status := <-statusC
		code, _, err := status.Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, code)
	}

	t.Run("WritableSecondaryRoot", func(t *testing.T) {
		secRoot := t.TempDir()
		primaryRoot := t.TempDir()

		blobPath := preloadSecondaryRoot(t, secRoot)
		_, err := os.Stat(blobPath)
		require.NoError(t, err)

		primaryConfig := fmt.Sprintf(`
version = 4
root = %q
secondary_roots = [%q]
[plugins."io.containerd.cri.v1.runtime"]
  stream_server_port = "0"
`, primaryRoot, secRoot)
		client, _, cleanup := newDaemonWithConfig(t, primaryConfig)
		defer cleanup()

		// Image is imported from writable secondary root without pulling
		img, err := client.GetImage(ctx, testImage)
		require.NoError(t, err)
		unpacked, err := img.IsUnpacked(ctx, "")
		require.NoError(t, err)
		require.True(t, unpacked)

		// Container runs using lowerdirs from secondary root and upperdir/workdir on primary root
		runContainerFromImage(t, client, img, "ctr-writable-secroot")

		// Deleting the image removes it from primary DB and records tombstones while leaving secondary root files untouched
		require.NoError(t, client.ImageService().Delete(ctx, testImage, images.SynchronousDelete()))
		_, err = client.GetImage(ctx, testImage)
		require.True(t, errdefs.IsNotFound(err))

		_, err = os.Stat(blobPath)
		require.NoError(t, err, "expected backing blob on secondary root to remain untouched on disk")

		entries, err := os.ReadDir(filepath.Join(secRoot, "io.containerd.snapshotter.v1.overlayfs", "snapshots"))
		require.NoError(t, err)
		require.NotEmpty(t, entries, "expected backing snapshots on secondary root to remain untouched on disk")
	})

	t.Run("ReadOnlySecondaryRoot", func(t *testing.T) {
		secBackingRoot := t.TempDir()
		secROMount := t.TempDir()
		primaryRoot := t.TempDir()

		_ = preloadSecondaryRoot(t, secBackingRoot)

		// Bind-mount secBackingRoot onto secROMount as a read-only filesystem
		require.NoError(t, syscall.Mount(secBackingRoot, secROMount, "none", syscall.MS_BIND|syscall.MS_REC, ""))
		t.Cleanup(func() {
			_ = syscall.Unmount(secROMount, syscall.MNT_DETACH)
		})
		require.NoError(t, syscall.Mount("none", secROMount, "", syscall.MS_REMOUNT|syscall.MS_BIND|syscall.MS_RDONLY, ""))

		primaryConfig := fmt.Sprintf(`
version = 4
root = %q
secondary_roots = [%q]
[plugins."io.containerd.cri.v1.runtime"]
  stream_server_port = "0"
`, primaryRoot, secROMount)
		client, _, cleanup := newDaemonWithConfig(t, primaryConfig)

		// Image is imported from read-only secondary root
		img, err := client.GetImage(ctx, testImage)
		require.NoError(t, err)
		unpacked, err := img.IsUnpacked(ctx, "")
		require.NoError(t, err)
		require.True(t, unpacked)

		roBlobPath := filepath.Join(
			secROMount,
			"io.containerd.content.v1.content",
			"blobs",
			img.Target().Digest.Algorithm().String(),
			img.Target().Digest.Encoded(),
		)

		// Container runs using read-only secondary root layers
		runContainerFromImage(t, client, img, "ctr-readonly-secroot")

		// Deleting the image succeeds (EROFS ignored) and records tombstones in primary root
		require.NoError(t, client.ImageService().Delete(ctx, testImage, images.SynchronousDelete()))
		_, err = client.GetImage(ctx, testImage)
		require.True(t, errdefs.IsNotFound(err))

		// Backing files remain on the read-only secondary root
		_, err = os.Stat(roBlobPath)
		require.NoError(t, err, "expected backing blob on read-only secondary root to remain on disk")

		cleanup()

		// Restarting daemon against the same primaryRoot and read-only secondary root must not resurrect the deleted image
		client2, _, cleanup2 := newDaemonWithConfig(t, primaryConfig)
		defer cleanup2()

		_, err = client2.GetImage(ctx, testImage)
		require.True(t, errdefs.IsNotFound(err), "tombstoned image from read-only secondary root must not be re-imported after restart")
	})
}
