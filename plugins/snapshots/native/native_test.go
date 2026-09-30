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

package native

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/core/snapshots/testsuite"
	"github.com/containerd/containerd/v2/pkg/testutil"
	"github.com/containerd/errdefs"
)

func newSnapshotter(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
	snapshotter, err := NewSnapshotter(root)
	if err != nil {
		return nil, nil, err
	}

	return snapshotter, func() error { return snapshotter.Close() }, nil
}

func TestNative(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Native snapshotter not implemented on windows")
	}
	testutil.RequiresRoot(t)
	testsuite.SnapshotterSuite(t, "Native", newSnapshotter)
}

func TestNativeSecondaryRoots(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Native snapshotter not implemented on windows")
	}
	ctx := t.Context()
	primaryRoot := t.TempDir()
	secRoot := t.TempDir()

	// 1. Populate secondary root snapshotter with a committed layer ("base")
	secSn, err := NewSnapshotter(secRoot)
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := secSn.Prepare(ctx, "prep-base", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mounts[0].Source, "hello.txt"), []byte("from-secondary"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := secSn.Commit(ctx, "base", "prep-base"); err != nil {
		t.Fatal(err)
	}
	if err := secSn.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Read secondary snapshots and import "base" into primary snapshotter
	records, err := storage.ReadSecondarySnapshots(filepath.Join(secRoot, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := records["base"]
	if !ok {
		t.Fatalf("expected 'base' record from secondary root, got %+v", records)
	}

	priSn, err := NewSnapshotter(primaryRoot, WithSecondaryRoots([]string{secRoot}))
	if err != nil {
		t.Fatal(err)
	}
	defer priSn.Close()

	importer := priSn.(*snapshotter)
	if !importer.SnapshotDirExists(secRoot, rec.ID) {
		t.Fatalf("expected snapshot dir for ID %s to exist in secondary root", rec.ID)
	}
	if err := importer.ImportCommittedSnapshot(ctx, rec.Info.Name, rec.Info, rec.Usage, secRoot, rec.ID); err != nil {
		t.Fatal(err)
	}

	// 3. View on imported secondary-root snapshot points directly to <secRoot>/snapshots/<id> with "ro"
	viewMounts, err := priSn.View(ctx, "view-base", "base")
	if err != nil {
		t.Fatal(err)
	}
	expectedSecDir := filepath.Join(secRoot, "snapshots", rec.ID)
	if viewMounts[0].Source != expectedSecDir {
		t.Fatalf("expected view source %q, got %q", expectedSecDir, viewMounts[0].Source)
	}
	if !slices.Contains(viewMounts[0].Options, "ro") {
		t.Fatalf("expected view mount options to contain 'ro', got %v", viewMounts[0].Options)
	}
	if err := priSn.Remove(ctx, "view-base"); err != nil {
		t.Fatal(err)
	}

	// 4. Prepare on top of imported secondary-root snapshot copies files into <primaryRoot>/snapshots/<new_id>
	activeMounts, err := priSn.Prepare(ctx, "active-child", "base")
	if err != nil {
		t.Fatal(err)
	}
	copiedData, err := os.ReadFile(filepath.Join(activeMounts[0].Source, "hello.txt"))
	if err != nil {
		t.Fatalf("expected hello.txt to be copied into active snapshot on primary root: %v", err)
	}
	if string(copiedData) != "from-secondary" {
		t.Fatalf("expected copied content 'from-secondary', got %q", string(copiedData))
	}
	if err := priSn.Remove(ctx, "active-child"); err != nil {
		t.Fatal(err)
	}

	// 5. Remove on imported secondary-root snapshot leaves <secRoot>/snapshots/<id> untouched on disk
	if err := priSn.Remove(ctx, "base"); err != nil {
		t.Fatal(err)
	}
	if _, err := priSn.Stat(ctx, "base"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected base snapshot to be removed from primary metadata, got: %v", err)
	}
	if _, err := os.Stat(expectedSecDir); err != nil {
		t.Fatalf("expected secondary snapshot directory %q to remain untouched on disk: %v", expectedSecDir, err)
	}

	// 6. Re-import "base", create a view, then remove secondary root and verify Stat, Usage, Mounts, and Prepare return ErrNotFound
	if err := importer.ImportCommittedSnapshot(ctx, rec.Info.Name, rec.Info, rec.Usage, secRoot, rec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := priSn.View(ctx, "view-before-disappear", "base"); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(secRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := priSn.Stat(ctx, "base"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected Stat to return ErrNotFound after secondary root disappeared, got: %v", err)
	}
	if _, err := priSn.Usage(ctx, "base"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected Usage to return ErrNotFound after secondary root disappeared, got: %v", err)
	}
	if _, err := priSn.Mounts(ctx, "view-before-disappear"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected Mounts to return ErrNotFound after secondary root disappeared, got: %v", err)
	}
	if _, err := priSn.Prepare(ctx, "active-after-disappear", "base"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected Prepare to return ErrNotFound after secondary root disappeared, got: %v", err)
	}
}
