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

package metadata

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/containerd/log/logtest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/metadata/boltutil"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/native"
)

type rootLayout struct {
	root       string
	metaDir    string
	contentDir string
	snapDir    string
}

func newRootLayout(base string) rootLayout {
	return rootLayout{
		root:       base,
		metaDir:    filepath.Join(base, "io.containerd.metadata.v1.bolt"),
		contentDir: filepath.Join(base, "io.containerd.content.v1.content"),
		snapDir:    filepath.Join(base, "io.containerd.snapshotter.v1.native"),
	}
}

func openRootDB(t *testing.T, ctx context.Context, primary rootLayout, secondaries ...rootLayout) (*DB, func()) {
	t.Helper()
	require.NoError(t, os.MkdirAll(primary.metaDir, 0o755))

	var secMetaDirs, secContentDirs, secSnapDirs []string
	for _, sec := range secondaries {
		secMetaDirs = append(secMetaDirs, sec.metaDir)
		secContentDirs = append(secContentDirs, sec.contentDir)
		secSnapDirs = append(secSnapDirs, sec.snapDir)
	}

	sn, err := native.NewSnapshotter(primary.snapDir, native.WithSecondaryRoots(secSnapDirs))
	require.NoError(t, err)

	cs, err := local.NewStoreWithSecondaryRoots(primary.contentDir, secContentDirs)
	require.NoError(t, err)

	bdb, err := bolt.Open(filepath.Join(primary.metaDir, "meta.db"), 0o644, nil)
	require.NoError(t, err)

	db := NewDB(bdb, cs, map[string]snapshots.Snapshotter{"native": sn}, WithSecondaryRoots(secMetaDirs))
	require.NoError(t, db.Init(ctx))

	cleanup := func() {
		assert.NoError(t, db.Close())
		assert.NoError(t, sn.Close())
	}
	return db, cleanup
}

func writeTestBlob(t *testing.T, ctx context.Context, cs content.Store, data string, labels map[string]string) ocispec.Descriptor {
	t.Helper()
	b := []byte(data)
	dgst := digest.FromBytes(b)
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    dgst,
		Size:      int64(len(b)),
	}
	var opts []content.Opt
	if len(labels) > 0 {
		opts = append(opts, content.WithLabels(labels))
	}
	err := content.WriteBlob(ctx, cs, dgst.String(), bytes.NewReader(b), desc, opts...)
	require.NoError(t, err)
	return desc
}

func commitTestSnapshot(t *testing.T, ctx context.Context, sn snapshots.Snapshotter, name, parent, fileName, fileData string) {
	t.Helper()
	prepKey := "prep-" + name
	mounts, err := sn.Prepare(ctx, prepKey, parent, snapshots.WithLabels(map[string]string{
		snapshots.LabelSnapshotRef: name,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, mounts)
	if fileName != "" {
		require.NoError(t, os.WriteFile(filepath.Join(mounts[0].Source, fileName), []byte(fileData), 0o644))
	}
	require.NoError(t, sn.Commit(ctx, name, prepKey))
}

func TestSecondaryRootsImportAndPrecedence(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	sec1Layout := newRootLayout(t.TempDir())
	sec2Layout := newRootLayout(t.TempDir())

	// Populate secondary_roots[0] (sec1)
	sec1DB, closeSec1 := openRootDB(t, ctx, sec1Layout)
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-base", "", "base.txt", "from-sec1")
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-app1", "layer-base", "app1.txt", "app1-data")

	descApp1 := writeTestBlob(t, ctx, sec1DB.ContentStore(), "manifest-app1-sec1", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-app1",
	})
	descSharedSec1 := writeTestBlob(t, ctx, sec1DB.ContentStore(), "manifest-shared-sec1", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-base",
	})

	_, err := NewImageStore(sec1DB).Create(ctx, images.Image{
		Name:   "docker.io/library/app1:v1",
		Target: descApp1,
	})
	require.NoError(t, err)
	_, err = NewImageStore(sec1DB).Create(ctx, images.Image{
		Name:   "docker.io/library/shared:latest",
		Target: descSharedSec1,
	})
	require.NoError(t, err)
	closeSec1()

	// Populate secondary_roots[1] (sec2)
	// Includes duplicate "layer-base" and duplicate tag "docker.io/library/shared:latest",
	// plus unique "layer-app2" built on top of "layer-base" (testing cross-root layer sharing).
	sec2DB, closeSec2 := openRootDB(t, ctx, sec2Layout)
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-base", "", "base.txt", "from-sec2")
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-app2", "layer-base", "app2.txt", "app2-data")

	descApp2 := writeTestBlob(t, ctx, sec2DB.ContentStore(), "manifest-app2-sec2", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-app2",
	})
	descSharedSec2 := writeTestBlob(t, ctx, sec2DB.ContentStore(), "manifest-shared-sec2", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-base",
	})

	_, err = NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/app2:v1",
		Target: descApp2,
	})
	require.NoError(t, err)
	_, err = NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/shared:latest",
		Target: descSharedSec2,
	})
	require.NoError(t, err)
	closeSec2()

	// Open primary DB with [sec1, sec2]
	priDB, closePri := openRootDB(t, ctx, priLayout, sec1Layout, sec2Layout)
	defer closePri()

	is := NewImageStore(priDB)
	cs := priDB.ContentStore()
	sn := priDB.Snapshotter("native")

	// 1. Verify duplicate image tag precedence: sec1 wins over sec2
	sharedImg, err := is.Get(ctx, "docker.io/library/shared:latest")
	require.NoError(t, err)
	assert.Equal(t, descSharedSec1.Digest, sharedImg.Target.Digest)

	// 2. Verify unique images from both sec1 and sec2 are imported
	app1Img, err := is.Get(ctx, "docker.io/library/app1:v1")
	require.NoError(t, err)
	assert.Equal(t, descApp1.Digest, app1Img.Target.Digest)

	app2Img, err := is.Get(ctx, "docker.io/library/app2:v1")
	require.NoError(t, err)
	assert.Equal(t, descApp2.Digest, app2Img.Target.Digest)

	// 3. Verify blobs are readable via ContentStore while remaining in secondary roots
	_, err = cs.Info(ctx, descApp1.Digest)
	require.NoError(t, err)
	_, err = cs.Info(ctx, descApp2.Digest)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(priLayout.contentDir, "blobs", "sha256", descApp1.Digest.Encoded()))
	assert.True(t, os.IsNotExist(err), "blob should not be copied to primary root")

	// 4. Verify snapshots and cross-root parent chain ("layer-app2" in sec2 -> "layer-base" in sec1)
	infoApp2, err := sn.Stat(ctx, "layer-app2")
	require.NoError(t, err)
	assert.Equal(t, "layer-base", infoApp2.Parent)

	// Prepare active container snapshot on primary root on top of imported secondary snapshot
	mounts, err := sn.Prepare(ctx, "active-container", "layer-app2")
	require.NoError(t, err)
	require.NotEmpty(t, mounts)
	assert.Contains(t, mounts[0].Source, priLayout.snapDir, "active snapshot must be created on primary root")
	require.NoError(t, sn.Remove(ctx, "active-container"))

	// 5. Verify garbage collection retains all imported resources
	_, err = priDB.GarbageCollect(ctx)
	require.NoError(t, err)
	_, err = is.Get(ctx, "docker.io/library/app1:v1")
	require.NoError(t, err)
	_, err = is.Get(ctx, "docker.io/library/app2:v1")
	require.NoError(t, err)
	_, err = sn.Stat(ctx, "layer-app1")
	require.NoError(t, err)
	_, err = sn.Stat(ctx, "layer-app2")
	require.NoError(t, err)
}

func TestSecondaryRootsDeleteWritableAndReadOnlyTombstones(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	secWritable := newRootLayout(t.TempDir())
	secReadOnly := newRootLayout(t.TempDir())

	// Populate writable secondary root
	secWDB, closeSecW := openRootDB(t, ctx, secWritable)
	commitTestSnapshot(t, ctx, secWDB.Snapshotter("native"), "snap-writable", "", "w.txt", "writable")
	descW := writeTestBlob(t, ctx, secWDB.ContentStore(), "blob-writable", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-writable",
	})
	_, err := NewImageStore(secWDB).Create(ctx, images.Image{
		Name:   "docker.io/library/writable:v1",
		Target: descW,
	})
	require.NoError(t, err)
	closeSecW()

	// Populate read-only secondary root
	secRODB, closeSecRO := openRootDB(t, ctx, secReadOnly)
	commitTestSnapshot(t, ctx, secRODB.Snapshotter("native"), "snap-readonly", "", "ro.txt", "readonly")
	descRO := writeTestBlob(t, ctx, secRODB.ContentStore(), "blob-readonly", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-readonly",
	})
	_, err = NewImageStore(secRODB).Create(ctx, images.Image{
		Name:   "docker.io/library/readonly:v1",
		Target: descRO,
	})
	require.NoError(t, err)
	closeSecRO()

	// Make secReadOnly directories read-only if running as non-root on Unix
	enforceReadOnly := runtime.GOOS != "windows" && os.Getuid() != 0
	if enforceReadOnly {
		roBlobDir := filepath.Join(secReadOnly.contentDir, "blobs", "sha256")
		roSnapDir := filepath.Join(secReadOnly.snapDir, "snapshots")
		require.NoError(t, os.Chmod(roBlobDir, 0o555))
		require.NoError(t, os.Chmod(roSnapDir, 0o555))
		defer func() {
			_ = os.Chmod(roBlobDir, 0o755)
			_ = os.Chmod(roSnapDir, 0o755)
		}()
	}

	// Open primary DB with [secWritable, secReadOnly]
	priDB, closePri := openRootDB(t, ctx, priLayout, secWritable, secReadOnly)

	is := NewImageStore(priDB)
	cs := priDB.ContentStore()
	sn := priDB.Snapshotter("native")

	// Delete both images and run GC
	require.NoError(t, is.Delete(ctx, "docker.io/library/writable:v1"))
	require.NoError(t, is.Delete(ctx, "docker.io/library/readonly:v1"))
	_, err = priDB.GarbageCollect(ctx)
	require.NoError(t, err)

	// Verify both images, blobs, and snapshots are gone from primary metadata
	_, err = is.Get(ctx, "docker.io/library/writable:v1")
	assert.True(t, errdefs.IsNotFound(err))
	_, err = is.Get(ctx, "docker.io/library/readonly:v1")
	assert.True(t, errdefs.IsNotFound(err))

	_, err = cs.Info(ctx, descW.Digest)
	assert.True(t, errdefs.IsNotFound(err))
	_, err = cs.Info(ctx, descRO.Digest)
	assert.True(t, errdefs.IsNotFound(err))

	_, err = sn.Stat(ctx, "snap-writable")
	assert.True(t, errdefs.IsNotFound(err))
	_, err = sn.Stat(ctx, "snap-readonly")
	assert.True(t, errdefs.IsNotFound(err))

	// Secondary roots are treated as strictly read-only by containerd: backing blob and snapshot files remain untouched on disk
	_, err = os.Stat(filepath.Join(secWritable.contentDir, "blobs", "sha256", descW.Digest.Encoded()))
	assert.NoError(t, err, "writable secondary blob file should remain untouched on disk")
	_, err = os.Stat(filepath.Join(secWritable.snapDir, "snapshots", "1"))
	assert.NoError(t, err, "writable secondary snapshot dir should remain untouched on disk")

	_, err = os.Stat(filepath.Join(secReadOnly.contentDir, "blobs", "sha256", descRO.Digest.Encoded()))
	assert.NoError(t, err, "read-only secondary blob file should remain on disk")
	_, err = os.Stat(filepath.Join(secReadOnly.snapDir, "snapshots", "1"))
	assert.NoError(t, err, "read-only secondary snapshot dir should remain on disk")

	closePri()

	// Restart primary DB and verify tombstones prevent re-importing deleted resources from either secondary root!
	priDB2, closePri2 := openRootDB(t, ctx, priLayout, secWritable, secReadOnly)

	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/writable:v1")
	assert.True(t, errdefs.IsNotFound(err), "tombstoned writable-root image must not be re-imported on restart")
	_, err = priDB2.ContentStore().Info(ctx, descW.Digest)
	assert.True(t, errdefs.IsNotFound(err), "tombstoned writable-root blob must not be re-imported on restart")
	_, err = priDB2.cs.Store.Info(ctx, descW.Digest)
	assert.True(t, errdefs.IsNotFound(err), "underlying local content store must also hide tombstoned writable-root blob after restart")
	_, err = priDB2.Snapshotter("native").Stat(ctx, "snap-writable")
	assert.True(t, errdefs.IsNotFound(err), "tombstoned writable-root snapshot must not be re-imported on restart")

	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/readonly:v1")
	assert.True(t, errdefs.IsNotFound(err), "tombstoned image must not be re-imported on restart")
	_, err = priDB2.ContentStore().Info(ctx, descRO.Digest)
	assert.True(t, errdefs.IsNotFound(err), "tombstoned blob must not be re-imported on restart")
	_, err = priDB2.cs.Store.Info(ctx, descRO.Digest)
	assert.True(t, errdefs.IsNotFound(err), "underlying local content store must also hide tombstoned blob after restart")
	_, err = priDB2.Snapshotter("native").Stat(ctx, "snap-readonly")
	assert.True(t, errdefs.IsNotFound(err), "tombstoned snapshot must not be re-imported on restart")

	// Re-pulling the tombstoned read-only image after restart writes blob and snapshot to primary root and clears tombstones
	commitTestSnapshot(t, ctx, priDB2.Snapshotter("native"), "snap-readonly", "", "ro.txt", "readonly")
	descRORepulled := writeTestBlob(t, ctx, priDB2.ContentStore(), "blob-readonly", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-readonly",
	})
	_, err = NewImageStore(priDB2).Create(ctx, images.Image{
		Name:   "docker.io/library/readonly:v1",
		Target: descRORepulled,
	})
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(priLayout.contentDir, "blobs", "sha256", descRO.Digest.Encoded()))
	require.NoError(t, err, "re-pulled tombstoned blob must be written to primary contentDir")
	closePri2()

	// Verify the re-pulled image survives a restart with NO secondary_roots configured
	priDB3, closePri3 := openRootDB(t, ctx, priLayout)
	_, err = NewImageStore(priDB3).Get(ctx, "docker.io/library/readonly:v1")
	require.NoError(t, err)
	_, err = priDB3.ContentStore().Info(ctx, descRO.Digest)
	require.NoError(t, err)
	_, err = priDB3.Snapshotter("native").Stat(ctx, "snap-readonly")
	require.NoError(t, err)
	closePri3()

	// Now restart with secReadOnly again, delete the promoted primary-root image, run GC,
	// and verify tombstones are recorded even though source_root was cleared on promotion!
	priDB4, closePri4 := openRootDB(t, ctx, priLayout, secReadOnly)
	require.NoError(t, NewImageStore(priDB4).Delete(ctx, "docker.io/library/readonly:v1"))
	_, err = priDB4.GarbageCollect(ctx)
	require.NoError(t, err)
	closePri4()

	priDB5, closePri5 := openRootDB(t, ctx, priLayout, secReadOnly)
	defer closePri5()
	_, err = NewImageStore(priDB5).Get(ctx, "docker.io/library/readonly:v1")
	assert.True(t, errdefs.IsNotFound(err), "promoted then GC'd image must not be re-imported from secondary root")
	_, err = priDB5.ContentStore().Info(ctx, descRO.Digest)
	assert.True(t, errdefs.IsNotFound(err), "promoted then GC'd blob must not be re-imported from secondary root")
	_, err = priDB5.Snapshotter("native").Stat(ctx, "snap-readonly")
	assert.True(t, errdefs.IsNotFound(err), "promoted then GC'd snapshot must not be re-imported from secondary root")
}

func TestSecondaryRootsDetachedOnStartupAndRuntimeDisappearance(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	sec1Layout := newRootLayout(t.TempDir())
	sec2Layout := newRootLayout(t.TempDir())

	// Populate sec1
	sec1DB, closeSec1 := openRootDB(t, ctx, sec1Layout)
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "snap-sec1", "", "f1.txt", "sec1")
	desc1 := writeTestBlob(t, ctx, sec1DB.ContentStore(), "blob-sec1", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-sec1",
	})
	_, err := NewImageStore(sec1DB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec1:v1",
		Target: desc1,
	})
	require.NoError(t, err)
	closeSec1()

	// Populate sec2
	sec2DB, closeSec2 := openRootDB(t, ctx, sec2Layout)
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "snap-sec2", "", "f2.txt", "sec2")
	desc2 := writeTestBlob(t, ctx, sec2DB.ContentStore(), "blob-sec2", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-sec2",
	})
	_, err = NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec2:v1",
		Target: desc2,
	})
	require.NoError(t, err)
	closeSec2()

	// 1. Start primary with both [sec1, sec2]
	priDB, closePri := openRootDB(t, ctx, priLayout, sec1Layout, sec2Layout)
	_, err = NewImageStore(priDB).Get(ctx, "docker.io/library/sec1:v1")
	require.NoError(t, err)
	_, err = NewImageStore(priDB).Get(ctx, "docker.io/library/sec2:v1")
	require.NoError(t, err)
	closePri()

	// 2. Restart primary with only [sec1] (sec2 removed from config)
	priDB2, closePri2 := openRootDB(t, ctx, priLayout, sec1Layout)
	defer closePri2()

	// sec2 entries must be automatically pruned on startup
	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/sec2:v1")
	assert.True(t, errdefs.IsNotFound(err), "detached secondary root image should be pruned on startup")
	_, err = priDB2.ContentStore().Info(ctx, desc2.Digest)
	assert.True(t, errdefs.IsNotFound(err), "detached secondary root blob should be pruned on startup")
	_, err = priDB2.Snapshotter("native").Stat(ctx, "snap-sec2")
	assert.True(t, errdefs.IsNotFound(err), "detached secondary root snapshot should be pruned on startup")

	// sec1 entries are still present
	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/sec1:v1")
	require.NoError(t, err)

	// Create a committed child snapshot on primary root before sec1 disappears at runtime
	commitTestSnapshot(t, ctx, priDB2.Snapshotter("native"), "pri-child-on-sec1", "snap-sec1", "child.txt", "child-data")

	// 3. Simulate runtime disappearance of sec1 while containerd is running
	require.NoError(t, os.RemoveAll(sec1Layout.root))

	// ImageStore, ContentStore, and Snapshotter detect the disappeared files and fall back cleanly
	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/sec1:v1")
	assert.True(t, errdefs.IsNotFound(err), "disappeared secondary root image should return ErrNotFound")

	imgs, err := NewImageStore(priDB2).List(ctx)
	require.NoError(t, err)
	assert.Empty(t, imgs, "disappeared secondary root image should not be listed")

	_, err = priDB2.ContentStore().Info(ctx, desc1.Digest)
	assert.True(t, errdefs.IsNotFound(err), "disappeared secondary root blob should return ErrNotFound")

	_, err = priDB2.Snapshotter("native").Stat(ctx, "snap-sec1")
	assert.True(t, errdefs.IsNotFound(err), "disappeared secondary root snapshot should return ErrNotFound")

	_, err = priDB2.Snapshotter("native").Stat(ctx, "pri-child-on-sec1")
	assert.True(t, errdefs.IsNotFound(err), "child snapshot depending on disappeared secondary parent should be removed")

	// 4. Fallback: re-downloading/unpacking the disappeared image onto the primary root succeeds!
	commitTestSnapshot(t, ctx, priDB2.Snapshotter("native"), "snap-sec1", "", "f1.txt", "sec1")
	desc1Re := writeTestBlob(t, ctx, priDB2.ContentStore(), "blob-sec1", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "snap-sec1",
	})
	_, err = NewImageStore(priDB2).Create(ctx, images.Image{
		Name:   "docker.io/library/sec1:v1",
		Target: desc1Re,
	})
	require.NoError(t, err)

	img, err := NewImageStore(priDB2).Get(ctx, "docker.io/library/sec1:v1")
	require.NoError(t, err)
	assert.Equal(t, desc1.Digest, img.Target.Digest)
	_, err = os.Stat(filepath.Join(priLayout.contentDir, "blobs", "sha256", desc1.Digest.Encoded()))
	assert.NoError(t, err, "re-downloaded blob must exist on primary root")
	_, err = priDB2.Snapshotter("native").Stat(ctx, "snap-sec1")
	assert.NoError(t, err, "re-unpacked snapshot must exist on primary root")
}

func TestSecondaryRootsMultiLayerPrependAndPrimaryPrecedence(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	sec1Layout := newRootLayout(t.TempDir())
	sec2Layout := newRootLayout(t.TempDir())

	// Populate sec2 with a 2-layer snapshot chain ("layer-base" -> "layer-app") and a primary-conflict snapshot ("pri-snap")
	sec2DB, closeSec2 := openRootDB(t, ctx, sec2Layout)
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-base", "", "who.txt", "from-sec2-base")
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-app", "layer-base", "app.txt", "from-sec2-app")
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "pri-snap", "", "pri.txt", "from-sec2")
	descSec2 := writeTestBlob(t, ctx, sec2DB.ContentStore(), "manifest-sec2", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-app",
	})
	_, err := NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/app:latest",
		Target: descSec2,
	})
	require.NoError(t, err)
	_, err = NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/pri-img:latest",
		Target: descSec2,
	})
	require.NoError(t, err)
	closeSec2()

	// Populate sec1 with a higher-priority 2-layer chain ("layer-base" -> "layer-app")
	sec1DB, closeSec1 := openRootDB(t, ctx, sec1Layout)
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-base", "", "who.txt", "from-sec1-base")
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-app", "layer-base", "app.txt", "from-sec1-app")
	descSec1 := writeTestBlob(t, ctx, sec1DB.ContentStore(), "manifest-sec1", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "layer-app",
	})
	_, err = NewImageStore(sec1DB).Create(ctx, images.Image{
		Name:   "docker.io/library/app:latest",
		Target: descSec1,
	})
	require.NoError(t, err)
	closeSec1()

	// 1. Populate primary root first (before attaching secondary roots) with "pri-snap" and "pri-img:latest"
	priDB0, closePri0 := openRootDB(t, ctx, priLayout)
	commitTestSnapshot(t, ctx, priDB0.Snapshotter("native"), "pri-snap", "", "pri.txt", "from-primary")
	descPri := writeTestBlob(t, ctx, priDB0.ContentStore(), "manifest-primary", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "pri-snap",
	})
	_, err = NewImageStore(priDB0).Create(ctx, images.Image{
		Name:   "docker.io/library/pri-img:latest",
		Target: descPri,
	})
	require.NoError(t, err)
	closePri0()

	// 2. Start primary with [sec2] (importing sec2's 2-layer chain while preserving primary's pri-snap and pri-img:latest)
	_, closePri1 := openRootDB(t, ctx, priLayout, sec2Layout)
	closePri1()

	// 3. Restart primary with [sec1, sec2] (sec1 prepended ahead of sec2)
	priDB2, closePri2 := openRootDB(t, ctx, priLayout, sec1Layout, sec2Layout)
	defer closePri2()

	// Verify sec1 overrode sec2 for the 2-layer chain ("layer-base" -> "layer-app")
	viewMounts, err := priDB2.Snapshotter("native").View(ctx, "view-app", "layer-app")
	require.NoError(t, err)
	appContent, err := os.ReadFile(filepath.Join(viewMounts[0].Source, "app.txt"))
	require.NoError(t, err)
	assert.Equal(t, "from-sec1-app", string(appContent))
	baseContent, err := os.ReadFile(filepath.Join(viewMounts[0].Source, "who.txt"))
	require.NoError(t, err)
	assert.Equal(t, "from-sec1-base", string(baseContent))
	require.NoError(t, priDB2.Snapshotter("native").Remove(ctx, "view-app"))

	// Verify primary root's "pri-snap" and "pri-img:latest" were NOT overwritten by sec2
	priImg, err := NewImageStore(priDB2).Get(ctx, "docker.io/library/pri-img:latest")
	require.NoError(t, err)
	assert.Equal(t, descPri.Digest, priImg.Target.Digest)

	priView, err := priDB2.Snapshotter("native").View(ctx, "view-pri", "pri-snap")
	require.NoError(t, err)
	priData, err := os.ReadFile(filepath.Join(priView[0].Source, "pri.txt"))
	require.NoError(t, err)
	assert.Equal(t, "from-primary", string(priData))
	require.NoError(t, priDB2.Snapshotter("native").Remove(ctx, "view-pri"))
}

func TestSecondaryRootsImageUpdatePreservesSourceRootAndChildSnapshotCleanup(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	secLayout := newRootLayout(t.TempDir())

	// Populate secondary root
	secDB, closeSec := openRootDB(t, ctx, secLayout)
	commitTestSnapshot(t, ctx, secDB.Snapshotter("native"), "sec-parent-snap", "", "p.txt", "parent")
	descSec := writeTestBlob(t, ctx, secDB.ContentStore(), "blob-sec", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "sec-parent-snap",
	})
	_, err := NewImageStore(secDB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec-update:v1",
		Target: descSec,
	})
	require.NoError(t, err)
	closeSec()

	// Start primary with [secLayout]
	priDB1, closePri1 := openRootDB(t, ctx, priLayout, secLayout)
	is1 := NewImageStore(priDB1)

	// 1. Update labels on the imported secondary-root image (Item 4.5.4)
	img, err := is1.Get(ctx, "docker.io/library/sec-update:v1")
	require.NoError(t, err)
	img.Labels = map[string]string{"foo": "bar"}
	_, err = is1.Update(ctx, img, "labels.foo")
	require.NoError(t, err)

	// 2. Create a committed child snapshot on the primary root whose parent is "sec-parent-snap" (Item 4.5.6)
	commitTestSnapshot(t, ctx, priDB1.Snapshotter("native"), "pri-child-snap", "sec-parent-snap", "c.txt", "child")

	// Find the on-disk directory created under <priLayout.snapDir>/snapshots/ for "pri-child-snap"
	entriesBefore, err := os.ReadDir(filepath.Join(priLayout.snapDir, "snapshots"))
	require.NoError(t, err)
	require.Len(t, entriesBefore, 1)
	closePri1()

	// 3. Restart primary WITHOUT secLayout
	priDB2, closePri2 := openRootDB(t, ctx, priLayout)
	defer closePri2()

	// The label-updated secondary image must still be pruned on detach because its target blob is on secLayout
	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/sec-update:v1")
	assert.True(t, errdefs.IsNotFound(err), "secondary image whose labels were updated must still be pruned when secondary root is detached")

	// Both "sec-parent-snap" and "pri-child-snap" must be removed from metadata AND the primary child's on-disk directory must be cleaned up
	_, err = priDB2.Snapshotter("native").Stat(ctx, "pri-child-snap")
	assert.True(t, errdefs.IsNotFound(err))
	entriesAfter, err := os.ReadDir(filepath.Join(priLayout.snapDir, "snapshots"))
	require.NoError(t, err)
	assert.Empty(t, entriesAfter, "primary-root child snapshot directory must be removed from disk when secondary parent is pruned")
}

func TestSecondaryRootsMultiNamespaceIsolationAndNamespaceLabels(t *testing.T) {
	baseCtx := logtest.WithT(t.Context(), t)
	ctxNS1 := namespaces.WithNamespace(baseCtx, "ns1")
	ctxNS2 := namespaces.WithNamespace(baseCtx, "ns2")

	priLayout := newRootLayout(t.TempDir())
	secLayout := newRootLayout(t.TempDir())

	// Populate secondary root with shared blob in both ns1 and ns2, plus namespace labels
	secDB, closeSec := openRootDB(t, ctxNS1, secLayout)
	require.NoError(t, secDB.Update(func(tx *bolt.Tx) error {
		nsStore := NewNamespaceStore(tx)
		if err := nsStore.SetLabel(ctxNS1, "ns1", "sec-label", "from-sec"); err != nil {
			return err
		}
		if err := nsStore.SetLabel(ctxNS1, "ns1", "shared-key", "from-sec"); err != nil {
			return err
		}
		return nil
	}))
	descShared := writeTestBlob(t, ctxNS1, secDB.ContentStore(), "shared-multi-ns-blob", nil)
	_, err := NewImageStore(secDB).Create(ctxNS1, images.Image{
		Name:   "docker.io/library/shared:v1",
		Target: descShared,
	})
	require.NoError(t, err)

	descSharedNS2 := writeTestBlob(t, ctxNS2, secDB.ContentStore(), "shared-multi-ns-blob", nil)
	_, err = NewImageStore(secDB).Create(ctxNS2, images.Image{
		Name:   "docker.io/library/shared:v1",
		Target: descSharedNS2,
	})
	require.NoError(t, err)
	closeSec()

	// Pre-populate primary root with a label on ns1 to verify primary namespace labels take precedence
	priDB0, closePri0 := openRootDB(t, ctxNS1, priLayout)
	require.NoError(t, priDB0.Update(func(tx *bolt.Tx) error {
		return NewNamespaceStore(tx).SetLabel(ctxNS1, "ns1", "shared-key", "from-primary")
	}))
	closePri0()

	// Open primary with [secLayout]
	priDB1, closePri1 := openRootDB(t, ctxNS1, priLayout, secLayout)

	// 1. Verify namespace labels import and primary precedence (Item 4.5.8)
	require.NoError(t, priDB1.View(func(tx *bolt.Tx) error {
		labels, err := NewNamespaceStore(tx).Labels(ctxNS1, "ns1")
		require.NoError(t, err)
		assert.Equal(t, "from-sec", labels["sec-label"])
		assert.Equal(t, "from-primary", labels["shared-key"], "existing primary namespace label must not be overwritten")
		return nil
	}))

	// 2. Delete the image in ns1 and run GC (tombstoning descShared in ns1 while ns2 still references it) (Item 4.5.7)
	require.NoError(t, NewImageStore(priDB1).Delete(ctxNS1, "docker.io/library/shared:v1"))
	_, err = priDB1.GarbageCollect(ctxNS1)
	require.NoError(t, err)

	// Verify ns1 sees ErrNotFound while ns2 can still Info and ReaderAt the blob
	_, err = priDB1.ContentStore().Info(ctxNS1, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err))
	_, err = priDB1.ContentStore().Info(ctxNS2, descShared.Digest)
	require.NoError(t, err)

	// Re-write descShared in ns1 (writing a copy to the primary root because ns1 tombstoned it),
	// then delete it in ns1 and run GC: the orphaned primary copy must be removed from disk
	// even though ns2 still references the same digest from the secondary root!
	_ = writeTestBlob(t, ctxNS1, priDB1.ContentStore(), "shared-multi-ns-blob", nil)
	priBlobPath := filepath.Join(priLayout.contentDir, "blobs", "sha256", descShared.Digest.Encoded())
	_, err = os.Stat(priBlobPath)
	require.NoError(t, err, "re-written tombstoned blob in ns1 must exist on primary root")
	require.NoError(t, priDB1.ContentStore().Delete(ctxNS1, descShared.Digest))
	_, err = priDB1.GarbageCollect(ctxNS1)
	require.NoError(t, err)
	_, err = os.Stat(priBlobPath)
	assert.True(t, os.IsNotExist(err), "orphaned primary copy must be garbage collected even when ns2 references secondary root")
	_, err = priDB1.ContentStore().Info(ctxNS1, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err), "ns1 must remain tombstoned after primary copy is GC'd")
	_, err = priDB1.ContentStore().Info(ctxNS2, descShared.Digest)
	require.NoError(t, err, "ns2 must still access secondary-root blob after orphaned primary copy is GC'd")
	closePri1()

	// 3. Restart primary DB and verify ns2 can still Info and ReaderAt the blob after syncDeletedDigests runs on startup
	priDB2, closePri2 := openRootDB(t, ctxNS1, priLayout, secLayout)

	_, err = priDB2.ContentStore().Info(ctxNS1, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err), "blob must remain deleted in ns1 after restart")

	_, err = priDB2.ContentStore().Info(ctxNS2, descShared.Digest)
	require.NoError(t, err, "blob must remain accessible in ns2 after restart")
	ra, err := priDB2.ContentStore().ReaderAt(ctxNS2, descShared)
	require.NoError(t, err)
	require.NoError(t, ra.Close())

	// 4. Now also delete the image in ns2 and run GC so NO namespace currently has descShared active (only tombstones exist in ns1 and ns2)
	require.NoError(t, NewImageStore(priDB2).Delete(ctxNS2, "docker.io/library/shared:v1"))
	_, err = priDB2.GarbageCollect(ctxNS2)
	require.NoError(t, err)

	// Verify that a new namespace (ns4) writing the same blob at runtime shares it from the secondary root
	// without copying it to the primary root, while ns1 and ns2 remain tombstoned.
	ctxNS4 := namespaces.WithNamespace(baseCtx, "ns4")
	_ = writeTestBlob(t, ctxNS4, priDB2.ContentStore(), "shared-multi-ns-blob", nil)
	_, err = priDB2.ContentStore().Info(ctxNS4, descShared.Digest)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(priLayout.contentDir, "blobs", "sha256", descShared.Digest.Encoded()))
	assert.True(t, os.IsNotExist(err), "blob shared into ns4 from secondary root must not be copied to primary root")
	_, err = priDB2.ContentStore().Info(ctxNS1, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err))
	require.NoError(t, priDB2.ContentStore().Delete(ctxNS4, descShared.Digest))
	_, err = priDB2.GarbageCollect(ctxNS4)
	require.NoError(t, err)
	closePri2()

	// Add an image referencing the same descShared in ns3 on a second secondary root, then restart primary DB:
	// ns3 must be able to import and read descShared even though ns1 and ns2 previously tombstoned it!
	ctxNS3 := namespaces.WithNamespace(baseCtx, "ns3")
	secLayout2 := newRootLayout(t.TempDir())
	secDB2, closeSec2 := openRootDB(t, ctxNS3, secLayout2)
	descSharedNS3 := writeTestBlob(t, ctxNS3, secDB2.ContentStore(), "shared-multi-ns-blob", nil)
	_, err = NewImageStore(secDB2).Create(ctxNS3, images.Image{
		Name:   "docker.io/library/shared-ns3:v1",
		Target: descSharedNS3,
	})
	require.NoError(t, err)
	closeSec2()

	priDB3, closePri3 := openRootDB(t, ctxNS1, priLayout, secLayout, secLayout2)
	defer closePri3()

	_, err = NewImageStore(priDB3).Get(ctxNS3, "docker.io/library/shared-ns3:v1")
	require.NoError(t, err, "ns3 must import image whose blob was previously tombstoned in ns1 and ns2")
	_, err = priDB3.ContentStore().Info(ctxNS3, descShared.Digest)
	require.NoError(t, err, "ns3 must be able to Info blob imported from secondary root")
	_, err = priDB3.ContentStore().Info(ctxNS1, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err), "blob must still be tombstoned in ns1")
	_, err = priDB3.ContentStore().Info(ctxNS2, descShared.Digest)
	assert.True(t, errdefs.IsNotFound(err), "blob must still be tombstoned in ns2")
}

func TestSecondaryRootsNoPrimaryTombstoneLeakAndFullTreeValidation(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	secLayout := newRootLayout(t.TempDir())

	// Populate secondary root with a full image tree (manifest -> config -> snapshot)
	// and a broken image whose referenced child layer blob is deleted from disk before import.
	secDB, closeSec := openRootDB(t, ctx, secLayout)
	commitTestSnapshot(t, ctx, secDB.Snapshotter("native"), "sec-snap", "", "f.txt", "sec")
	descConfig := writeTestBlob(t, ctx, secDB.ContentStore(), "config-sec", map[string]string{
		"containerd.io/gc.ref.snapshot.native": "sec-snap",
	})
	descManifest := writeTestBlob(t, ctx, secDB.ContentStore(), "manifest-sec", map[string]string{
		"containerd.io/gc.ref.content.0": descConfig.Digest.String(),
	})
	_, err := NewImageStore(secDB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec-full:v1",
		Target: descManifest,
	})
	require.NoError(t, err)

	// Also populate a multi-arch OCI Index referencing a pulled manifest (m.0) and an unpulled platform manifest (m.1)
	unpulledPlatformDigest := digest.FromString("unpulled-arm64-manifest")
	descIndex := writeTestBlob(t, ctx, secDB.ContentStore(), "multi-arch-index", map[string]string{
		"containerd.io/gc.ref.content.m.0": descManifest.Digest.String(),
		"containerd.io/gc.ref.content.m.1": unpulledPlatformDigest.String(),
	})
	_, err = NewImageStore(secDB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec-multiarch:v1",
		Target: descIndex,
	})
	require.NoError(t, err)

	descMissingChild := writeTestBlob(t, ctx, secDB.ContentStore(), "missing-child-blob", nil)
	descBrokenManifest := writeTestBlob(t, ctx, secDB.ContentStore(), "broken-manifest", map[string]string{
		"containerd.io/gc.ref.content.l.0": descMissingChild.Digest.String(),
	})
	_, err = NewImageStore(secDB).Create(ctx, images.Image{
		Name:   "docker.io/library/sec-broken:v1",
		Target: descBrokenManifest,
	})
	require.NoError(t, err)
	closeSec()

	// Remove the child blob of sec-broken:v1 from the secondary root before starting primary
	require.NoError(t, os.Remove(filepath.Join(secLayout.contentDir, "blobs", "sha256", descMissingChild.Digest.Encoded())))

	priDB, closePri := openRootDB(t, ctx, priLayout, secLayout)
	defer closePri()

	is := NewImageStore(priDB)
	cs := priDB.ContentStore()
	sn := priDB.Snapshotter("native")

	// 1. Verify sec-broken:v1 was NOT imported because its child blob is missing on disk,
	// while both sec-full:v1 and multi-arch sec-multiarch:v1 WERE imported and are available.
	_, err = is.Get(ctx, "docker.io/library/sec-broken:v1")
	assert.True(t, errdefs.IsNotFound(err), "image with missing child blob must not be imported")
	_, err = is.Get(ctx, "docker.io/library/sec-full:v1")
	require.NoError(t, err)
	_, err = is.Get(ctx, "docker.io/library/sec-multiarch:v1")
	require.NoError(t, err, "multi-arch index image with one pulled platform manifest must be imported and accessible")

	// 2. Create and remove primary-only container snapshots, blobs, and images, then verify NO tombstones are created
	mounts, err := sn.Prepare(ctx, "container-rw-1", "sec-snap")
	require.NoError(t, err)
	require.NotEmpty(t, mounts)
	require.NoError(t, sn.Remove(ctx, "container-rw-1"))

	priDesc := writeTestBlob(t, ctx, cs, "primary-only-blob", nil)
	_, err = is.Create(ctx, images.Image{
		Name:   "docker.io/library/pri-only:v1",
		Target: priDesc,
	})
	require.NoError(t, err)
	require.NoError(t, is.Delete(ctx, "docker.io/library/pri-only:v1"))
	_, err = priDB.GarbageCollect(ctx)
	require.NoError(t, err)

	require.NoError(t, priDB.View(func(tx *bolt.Tx) error {
		assert.False(t, isSnapshotTombstoned(tx, "testing", "native", "container-rw-1"), "primary-only container snapshot must not leave a tombstone")
		assert.False(t, isContentTombstoned(tx, "testing", priDesc.Digest), "primary-only blob must not leave a tombstone")
		assert.False(t, isImageTombstoned(tx, "testing", "docker.io/library/pri-only:v1"), "primary-only image must not leave a tombstone")
		return nil
	}))

	// Verify committing a Writer opened without WithDescriptor for an available secondary blob
	// returns ErrAlreadyExists and preserves the secondary blob's existing labels and source_root.
	dupW, err := cs.Writer(ctx, content.WithRef("dup-without-desc"))
	require.NoError(t, err)
	_, err = dupW.Write([]byte("config-sec"))
	require.NoError(t, err)
	err = dupW.Commit(ctx, int64(len("config-sec")), descConfig.Digest)
	assert.True(t, errdefs.IsAlreadyExists(err), "Committing an already-available secondary blob must return ErrAlreadyExists")
	_ = dupW.Close()
	infoAfterDup, err := cs.Info(ctx, descConfig.Digest)
	require.NoError(t, err)
	assert.Equal(t, "sec-snap", infoAfterDup.Labels["containerd.io/gc.ref.snapshot.native"], "Existing secondary blob labels must be preserved")
	_, statDupErr := os.Stat(filepath.Join(priLayout.contentDir, "blobs", "sha256", descConfig.Digest.Encoded()))
	assert.True(t, os.IsNotExist(statDupErr), "Committing an already-available secondary blob must not write a duplicate file to primary root")

	// 3. Remove secondary snapshot directory on disk at runtime:
	// - Stat inside a read-only View transaction must return ErrNotFound (not ErrTxNotWritable)
	// - ImageStore.Get and ImageStore.Update must detect the missing snapshot in the image tree and return ErrNotFound
	require.NoError(t, os.RemoveAll(filepath.Join(secLayout.snapDir, "snapshots")))

	require.NoError(t, priDB.View(func(tx *bolt.Tx) error {
		_, statErr := sn.Stat(boltutil.WithTransaction(ctx, tx), "sec-snap")
		assert.True(t, errdefs.IsNotFound(statErr), "Stat inside read-only View tx must return ErrNotFound")
		return nil
	}))

	_, err = is.Get(ctx, "docker.io/library/sec-full:v1")
	assert.True(t, errdefs.IsNotFound(err), "Get must return ErrNotFound when snapshot in image tree disappears")

	_, err = is.Update(ctx, images.Image{
		Name:   "docker.io/library/sec-full:v1",
		Target: descManifest,
		Labels: map[string]string{"k": "v"},
	}, "labels.k")
	assert.True(t, errdefs.IsNotFound(err), "Update must return ErrNotFound when secondary image tree is unavailable")
	require.NoError(t, priDB.View(func(tx *bolt.Tx) error {
		bkt := getImagesBucket(tx, "testing")
		if bkt != nil {
			assert.Nil(t, bkt.Bucket([]byte("docker.io/library/sec-full:v1")), "Update must persist cleanup of unavailable secondary image bucket")
		}
		return nil
	}))

	// 4. Re-pull the missing snapshot onto the primary root while secLayout.metaDir still exists on disk,
	// and verify ImageStore.Create succeeds for sec-multiarch:v1 (whose bucket was still in metadata).
	commitTestSnapshot(t, ctx, sn, "sec-snap", "", "f.txt", "sec")
	_, err = is.Create(ctx, images.Image{
		Name:   "docker.io/library/sec-multiarch:v1",
		Target: descIndex,
	})
	require.NoError(t, err, "ImageStore.Create must succeed when re-pulling an image after partial secondary snapshot disappearance")
}

func TestSecondaryRootsSharedBaseLayerGCAndAbortedWriter(t *testing.T) {
	ctx := namespaces.WithNamespace(logtest.WithT(t.Context(), t), "testing")

	priLayout := newRootLayout(t.TempDir())
	sec1Layout := newRootLayout(t.TempDir())
	sec2Layout := newRootLayout(t.TempDir())

	// Populate sec1 with Image A (base layer L1 -> app layer LA)
	sec1DB, closeSec1 := openRootDB(t, ctx, sec1Layout)
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-L1", "", "l1.txt", "base")
	commitTestSnapshot(t, ctx, sec1DB.Snapshotter("native"), "layer-LA", "layer-L1", "la.txt", "appA")
	descL1 := writeTestBlob(t, ctx, sec1DB.ContentStore(), "blob-L1", nil)
	descManifestA := writeTestBlob(t, ctx, sec1DB.ContentStore(), "manifest-A", map[string]string{
		"containerd.io/gc.ref.content.0":       descL1.Digest.String(),
		"containerd.io/gc.ref.snapshot.native": "layer-LA",
	})
	_, err := NewImageStore(sec1DB).Create(ctx, images.Image{
		Name:   "docker.io/library/img-a:v1",
		Target: descManifestA,
	})
	require.NoError(t, err)
	closeSec1()

	// Populate sec2 with Image B sharing base layer L1 and descL1 (layer-L1 -> layer-LB)
	sec2DB, closeSec2 := openRootDB(t, ctx, sec2Layout)
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-L1", "", "l1.txt", "base")
	commitTestSnapshot(t, ctx, sec2DB.Snapshotter("native"), "layer-LB", "layer-L1", "lb.txt", "appB")
	_ = writeTestBlob(t, ctx, sec2DB.ContentStore(), "blob-L1", nil)
	descManifestB := writeTestBlob(t, ctx, sec2DB.ContentStore(), "manifest-B", map[string]string{
		"containerd.io/gc.ref.content.0":       descL1.Digest.String(),
		"containerd.io/gc.ref.snapshot.native": "layer-LB",
	})
	_, err = NewImageStore(sec2DB).Create(ctx, images.Image{
		Name:   "docker.io/library/img-b:v1",
		Target: descManifestB,
	})
	require.NoError(t, err)
	closeSec2()

	// 1. Start primary with [sec1], delete img-a:v1 and run GC (GC-tombstoning descL1 and layer-L1)
	priDB1, closePri1 := openRootDB(t, ctx, priLayout, sec1Layout)
	require.NoError(t, NewImageStore(priDB1).Delete(ctx, "docker.io/library/img-a:v1"))
	_, err = priDB1.GarbageCollect(ctx)
	require.NoError(t, err)

	// Verify opening and aborting a Writer for tombstoned descManifestA does NOT unmask it in the underlying store
	w, err := priDB1.ContentStore().Writer(ctx,
		content.WithRef("aborted-write"),
		content.WithDescriptor(descManifestA),
	)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	_, err = priDB1.cs.Store.Info(ctx, descManifestA.Digest)
	assert.True(t, errdefs.IsNotFound(err), "aborted Writer must not unmask tombstoned secondary blob")
	closePri1()

	// 2. Restart primary with [sec1, sec2]: img-b:v1 in sec2 must be imported along with shared base layer-L1 and descL1,
	// while img-a:v1 and layer-LA remain tombstoned!
	priDB2, closePri2 := openRootDB(t, ctx, priLayout, sec1Layout, sec2Layout)
	defer closePri2()

	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/img-a:v1")
	assert.True(t, errdefs.IsNotFound(err), "deleted img-a:v1 must remain tombstoned")
	_, err = priDB2.Snapshotter("native").Stat(ctx, "layer-LA")
	assert.True(t, errdefs.IsNotFound(err), "layer-LA exclusive to deleted img-a:v1 must remain tombstoned")

	_, err = NewImageStore(priDB2).Get(ctx, "docker.io/library/img-b:v1")
	require.NoError(t, err, "img-b:v1 sharing GC-tombstoned base layer-L1 must be imported")
	_, err = priDB2.ContentStore().Info(ctx, descL1.Digest)
	require.NoError(t, err)
	_, err = priDB2.Snapshotter("native").Stat(ctx, "layer-L1")
	require.NoError(t, err)
	_, err = priDB2.Snapshotter("native").Stat(ctx, "layer-LB")
	require.NoError(t, err)
}
