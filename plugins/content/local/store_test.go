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

package local

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	_ "crypto/sha256" // required for digest package
	_ "crypto/sha512" // required for sha512 digest support
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/content/testsuite"
	"github.com/containerd/containerd/v2/internal/fsverity"
	"github.com/containerd/containerd/v2/internal/randutil"
	"github.com/containerd/containerd/v2/pkg/testutil"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
)

type memoryLabelStore struct {
	l      sync.Mutex
	labels map[digest.Digest]map[string]string
}

func newMemoryLabelStore() LabelStore {
	return &memoryLabelStore{
		labels: map[digest.Digest]map[string]string{},
	}
}

func (mls *memoryLabelStore) Get(d digest.Digest) (map[string]string, error) {
	mls.l.Lock()
	labels := mls.labels[d]
	mls.l.Unlock()

	return labels, nil
}

func (mls *memoryLabelStore) Set(d digest.Digest, labels map[string]string) error {
	mls.l.Lock()
	mls.labels[d] = labels
	mls.l.Unlock()

	return nil
}

func (mls *memoryLabelStore) Update(d digest.Digest, update map[string]string) (map[string]string, error) {
	mls.l.Lock()
	labels, ok := mls.labels[d]
	if !ok {
		labels = map[string]string{}
	}
	for k, v := range update {
		if v == "" {
			delete(labels, k)
		} else {
			labels[k] = v
		}
	}
	mls.labels[d] = labels
	mls.l.Unlock()

	return labels, nil
}

func TestContent(t *testing.T) {
	testsuite.ContentSuite(t, "fs", func(ctx context.Context, root string) (context.Context, content.Store, func() error, error) {
		cs, err := NewLabeledStore(root, newMemoryLabelStore())
		assert.NoError(t, err)
		return ctx, cs, func() error {
			return nil
		}, nil
	})
}

func TestContentRootDir(t *testing.T) {
	// test dir exist
	dirExist := t.TempDir()
	_, err := NewLabeledStore(dirExist, newMemoryLabelStore())
	assert.NoError(t, err)
	// test dir doesn't exist
	dir := filepath.Join(t.TempDir(), "test_dir001")
	_, err = NewLabeledStore(dir, newMemoryLabelStore())
	assert.NoError(t, err)
	_, err = os.Stat(dir)
	assert.NoError(t, err)
}

func TestInvalidPermissionRootDir(t *testing.T) {
	// test dir permissions are invalid
	if os.Getuid() != 0 {
		t.Skip("skipping test that requires root")
	}
	_, err := exec.LookPath("chattr")
	if err != nil {
		t.Skip("skipping test that requires chattr command")
	}
	dirBadPermission := t.TempDir()
	cmd := exec.Command("chattr", "+i", dirBadPermission)
	_, err = cmd.CombinedOutput()
	assert.NoError(t, err)
	defer func() {
		cmd := exec.Command("chattr", "-i", dirBadPermission)
		_, err = cmd.CombinedOutput()
		assert.NoError(t, err)
	}()
	_, err = fsverity.IsSupported(dirBadPermission)
	if err == nil {
		t.Fatal(fmt.Errorf("err can't be nil"))
	}
}

func TestContentWriter(t *testing.T) {
	for _, alg := range []digest.Algorithm{digest.SHA256, digest.SHA512} {
		t.Run(alg.String(), func(t *testing.T) {
			ctx, tmpdir, cs, cleanup := contentStoreEnv(t)
			defer cleanup()
			defer testutil.DumpDirOnFailure(t, tmpdir)

			cw, err := cs.Writer(ctx, content.WithRef("myref"))
			if err != nil {
				t.Fatal(err)
			}
			if err := cw.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := os.Stat(filepath.Join(tmpdir, "ingest")); os.IsNotExist(err) {
				t.Fatal("ingest dir should be created", err)
			}

			// reopen, so we can test things
			cw, err = cs.Writer(ctx, content.WithRef("myref"))
			if err != nil {
				t.Fatal(err)
			}

			// make sure that second resume also fails
			if _, err = cs.Writer(ctx, content.WithRef("myref")); err == nil {
				// TODO(stevvooe): This also works across processes. Need to find a way
				// to test that, as well.
				t.Fatal("no error on second resume")
			}

			// we should also see this as an active ingestion
			ingestions, err := cs.ListStatuses(ctx, "")
			if err != nil {
				t.Fatal(err)
			}

			// clear out the time and meta cause we don't care for this test
			for i := range ingestions {
				ingestions[i].UpdatedAt = time.Time{}
				ingestions[i].StartedAt = time.Time{}
			}

			if !reflect.DeepEqual(ingestions, []content.Status{
				{
					Ref:    "myref",
					Offset: 0,
				},
			}) {
				t.Fatalf("unexpected ingestion set: %v", ingestions)
			}

			p := make([]byte, 4<<20)
			if _, err := rand.Read(p); err != nil {
				t.Fatal(err)
			}
			expected := alg.FromBytes(p)

			checkCopy(t, int64(len(p)), cw, bufio.NewReader(io.NopCloser(bytes.NewReader(p))))

			if err := cw.Commit(ctx, int64(len(p)), expected); err != nil {
				t.Fatal(err)
			}

			if err := cw.Close(); err != nil {
				t.Fatal(err)
			}

			cw, err = cs.Writer(ctx, content.WithRef("aref"))
			if err != nil {
				t.Fatal(err)
			}

			// now, attempt to write the same data again
			checkCopy(t, int64(len(p)), cw, bufio.NewReader(io.NopCloser(bytes.NewReader(p))))
			if err := cw.Commit(ctx, int64(len(p)), expected); err == nil {
				t.Fatal("expected already exists error")
			} else if !errdefs.IsAlreadyExists(err) {
				t.Fatal(err)
			}

			path := checkBlobPath(t, cs, expected)

			// read the data back, make sure its the same
			pp, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(p, pp) {
				t.Fatal("mismatched data written to disk")
			}

			// ensure fsverity is enabled on blob if fsverity is supported
			ok, err := fsverity.IsSupported(tmpdir)
			if !ok || err != nil {
				t.Log("fsverity not supported, skipping fsverity check")
				return
			}

			ok, err = fsverity.IsEnabled(path)
			if !ok || err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWalkBlobs(t *testing.T) {
	ctx, _, cs, cleanup := contentStoreEnv(t)
	defer cleanup()

	const (
		nblobs  = 79
		maxsize = 4 << 10
	)
	var (
		blobs    = populateBlobStore(ctx, t, cs, nblobs, maxsize)
		expected = map[digest.Digest]struct{}{}
		found    = map[digest.Digest]struct{}{}
	)

	for dgst := range blobs {
		expected[dgst] = struct{}{}
	}

	if err := cs.Walk(ctx, func(bi content.Info) error {
		found[bi.Digest] = struct{}{}
		checkBlobPath(t, cs, bi.Digest)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, found) {
		t.Fatalf("expected did not match found: %v != %v", found, expected)
	}
}

// BenchmarkIngests checks the insertion time over varying blob sizes.
//
// Note that at the time of writing there is roughly a 4ms insertion overhead
// for blobs. This seems to be due to the number of syscalls and file io we do
// coordinating the ingestion.
func BenchmarkIngests(b *testing.B) {
	ctx, _, cs, cleanup := contentStoreEnv(b)
	defer cleanup()

	for _, size := range []int64{
		1 << 10,
		4 << 10,
		512 << 10,
		1 << 20,
	} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.StopTimer()
			blobs := generateBlobs(b, int64(b.N), size)

			var bytes int64
			for _, blob := range blobs {
				bytes += int64(len(blob))
			}
			b.SetBytes(bytes)

			b.StartTimer()

			for dgst, p := range blobs {
				checkWrite(ctx, b, cs, dgst, p)
			}
		})
	}
}

type checker interface {
	Fatal(args ...any)
}

func generateBlobs(t checker, nblobs, maxsize int64) map[digest.Digest][]byte {
	blobs := map[digest.Digest][]byte{}

	for range nblobs {
		p := make([]byte, randutil.Int63n(maxsize))

		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}

		for _, alg := range []digest.Algorithm{digest.SHA256, digest.SHA512} {
			blobs[alg.FromBytes(p)] = p
		}
	}

	return blobs
}

func populateBlobStore(ctx context.Context, t checker, cs content.Store, nblobs, maxsize int64) map[digest.Digest][]byte {
	blobs := generateBlobs(t, nblobs, maxsize)

	for dgst, p := range blobs {
		checkWrite(ctx, t, cs, dgst, p)
	}

	return blobs
}

func checkCopy(t checker, size int64, dst io.Writer, src io.Reader) {
	nn, err := io.Copy(dst, src)
	if err != nil {
		t.Fatal(err)
	}

	if nn != size {
		t.Fatal("incorrect number of bytes copied")
	}
}

func checkBlobPath(t *testing.T, cs content.Store, dgst digest.Digest) string {
	path, err := cs.(*store).blobPath(dgst)
	if err != nil {
		t.Fatalf("failed to calculate blob path: %v", err)
	}

	if path != filepath.Join(cs.(*store).root, "blobs", dgst.Algorithm().String(), dgst.Encoded()) {
		t.Fatalf("unexpected path: %q", path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("error stating blob path: %v", err)
	}

	if runtime.GOOS != "windows" {
		// ensure that only read bits are set.
		if ((fi.Mode() & os.ModePerm) & 0333) != 0 {
			t.Fatalf("incorrect permissions: %v", fi.Mode())
		}
	}

	return path
}

func checkWrite(ctx context.Context, t checker, cs content.Store, dgst digest.Digest, p []byte) digest.Digest {
	if err := content.WriteBlob(ctx, cs, dgst.String(), bytes.NewReader(p),
		ocispec.Descriptor{Size: int64(len(p)), Digest: dgst}); err != nil {
		t.Fatal(err)
	}

	return dgst
}

func TestWriterTruncateRecoversFromIncompleteWrite(t *testing.T) {
	cs, err := NewStore(t.TempDir())
	assert.NoError(t, err)

	ctx := t.Context()

	ref := "ref"
	contentB := []byte("this is the content")
	total := int64(len(contentB))
	setupIncompleteWrite(ctx, t, cs, ref, total)

	writer, err := cs.Writer(ctx, content.WithRef(ref), content.WithDescriptor(ocispec.Descriptor{Size: total}))
	assert.NoError(t, err)

	assert.Nil(t, writer.Truncate(0))

	_, err = writer.Write(contentB)
	assert.NoError(t, err)

	dgst := digest.FromBytes(contentB)
	err = writer.Commit(ctx, total, dgst)
	assert.NoError(t, err)
}

func setupIncompleteWrite(ctx context.Context, t *testing.T, cs content.Store, ref string, total int64) {
	writer, err := cs.Writer(ctx, content.WithRef(ref), content.WithDescriptor(ocispec.Descriptor{Size: total}))
	assert.NoError(t, err)

	_, err = writer.Write([]byte("bad data"))
	assert.NoError(t, err)

	assert.Nil(t, writer.Close())
}

func TestWriteReadEmptyFileTimestamp(t *testing.T) {
	root := t.TempDir()

	emptyFile := filepath.Join(root, "updatedat")
	if err := writeTimestampFile(emptyFile, time.Time{}); err != nil {
		t.Errorf("failed to write Zero Time to file: %v", err)
	}

	timestamp, err := readFileTimestamp(emptyFile)
	if err != nil {
		t.Errorf("read empty timestamp file should success, but got error: %v", err)
	}
	if !timestamp.IsZero() {
		t.Errorf("read empty timestamp file should return time.Time{}, but got: %v", timestamp)
	}
}

func TestSecondaryRoots(t *testing.T) {
	ctx := t.Context()
	primaryDir := t.TempDir()
	sec1Dir := t.TempDir()
	sec2Dir := t.TempDir()

	sec1Store, err := NewStore(sec1Dir)
	assert.NoError(t, err)
	sec2Store, err := NewStore(sec2Dir)
	assert.NoError(t, err)

	blob1 := []byte("blob-in-sec1")
	dgst1 := digest.FromBytes(blob1)
	checkWrite(ctx, t, sec1Store, dgst1, blob1)

	blob2 := []byte("blob-in-sec2")
	dgst2 := digest.FromBytes(blob2)
	checkWrite(ctx, t, sec2Store, dgst2, blob2)

	blobBoth := []byte("blob-in-both-sec1-and-sec2")
	dgstBoth := digest.FromBytes(blobBoth)
	checkWrite(ctx, t, sec1Store, dgstBoth, blobBoth)
	checkWrite(ctx, t, sec2Store, dgstBoth, blobBoth)

	cs, err := NewLabeledStoreWithSecondaryRoots(primaryDir, []string{sec1Dir, sec2Dir}, newMemoryLabelStore())
	assert.NoError(t, err)

	// Verify blobs from secondary roots are accessible via Info and ReaderAt
	info1, err := cs.Info(ctx, dgst1)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(blob1)), info1.Size)

	ra1, err := cs.ReaderAt(ctx, ocispec.Descriptor{Digest: dgst1, Size: int64(len(blob1))})
	assert.NoError(t, err)
	buf := make([]byte, len(blob1))
	_, err = ra1.ReadAt(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, blob1, buf)
	assert.NoError(t, ra1.Close())

	// Verify Walk deduplicates across roots and finds all blobs
	walked := map[digest.Digest]int{}
	err = cs.Walk(ctx, func(bi content.Info) error {
		walked[bi.Digest]++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[digest.Digest]int{
		dgst1:    1,
		dgst2:    1,
		dgstBoth: 1,
	}, walked)

	// Writing a new blob goes to primaryDir, not secondary roots
	blobPrimary := []byte("blob-in-primary")
	dgstPrimary := digest.FromBytes(blobPrimary)
	checkWrite(ctx, t, cs, dgstPrimary, blobPrimary)
	_, err = os.Stat(filepath.Join(primaryDir, "blobs", dgstPrimary.Algorithm().String(), dgstPrimary.Encoded()))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(sec1Dir, "blobs", dgstPrimary.Algorithm().String(), dgstPrimary.Encoded()))
	assert.True(t, os.IsNotExist(err))

	// Writer with expected digest present in secondary root returns ErrAlreadyExists
	_, err = cs.Writer(ctx, content.WithRef("ref-exists"), content.WithDescriptor(ocispec.Descriptor{Digest: dgst1, Size: int64(len(blob1))}))
	assert.True(t, errdefs.IsAlreadyExists(err))

	// Deleting a blob on a secondary root marks it deleted in the store while leaving the secondary root file untouched
	err = cs.Delete(ctx, dgst1)
	assert.NoError(t, err)
	_, err = cs.Info(ctx, dgst1)
	assert.True(t, errdefs.IsNotFound(err))
	_, err = os.Stat(filepath.Join(sec1Dir, "blobs", dgst1.Algorithm().String(), dgst1.Encoded()))
	assert.NoError(t, err)

	// Once tombstoned, Writer with expected digest dgst1 succeeds and writes to primaryDir
	checkWrite(ctx, t, cs, dgst1, blob1)
	_, err = os.Stat(filepath.Join(primaryDir, "blobs", dgst1.Algorithm().String(), dgst1.Encoded()))
	assert.NoError(t, err)

	// Deleting a duplicate blob present in both sec1Dir and sec2Dir hides it across all secondary roots
	err = cs.Delete(ctx, dgstBoth)
	assert.NoError(t, err)
	_, err = cs.Info(ctx, dgstBoth)
	assert.True(t, errdefs.IsNotFound(err))
	_, err = cs.ReaderAt(ctx, ocispec.Descriptor{Digest: dgstBoth, Size: int64(len(blobBoth))})
	assert.True(t, errdefs.IsNotFound(err))
	walkedAfterDelete := map[digest.Digest]int{}
	err = cs.Walk(ctx, func(bi content.Info) error {
		walkedAfterDelete[bi.Digest]++
		return nil
	})
	assert.NoError(t, err)
	assert.NotContains(t, walkedAfterDelete, dgstBoth)

	// SetDeletedDigests hides secondary-root blobs (e.g. dgst2, dgstBoth) while still serving blobs present in primaryDir (e.g. dgst1, dgstPrimary)
	cs.(*store).SetDeletedDigests([]digest.Digest{dgst1, dgst2, dgstBoth})
	_, err = cs.Info(ctx, dgst2)
	assert.True(t, errdefs.IsNotFound(err))
	_, err = cs.ReaderAt(ctx, ocispec.Descriptor{Digest: dgst2, Size: int64(len(blob2))})
	assert.True(t, errdefs.IsNotFound(err))
	_, err = cs.Info(ctx, dgst1)
	assert.NoError(t, err, "dgst1 is in primaryDir so SetDeletedDigests must not hide it")
	walkedAfterSet := map[digest.Digest]int{}
	err = cs.Walk(ctx, func(bi content.Info) error {
		walkedAfterSet[bi.Digest]++
		return nil
	})
	assert.NoError(t, err)
	assert.NotContains(t, walkedAfterSet, dgst2)
	assert.NotContains(t, walkedAfterSet, dgstBoth)
	assert.Contains(t, walkedAfterSet, dgst1)
	assert.Contains(t, walkedAfterSet, dgstPrimary)

	// Clear deleted digests so dgst2 is visible again from sec2Dir
	cs.(*store).SetDeletedDigests(nil)
	_, err = cs.Info(ctx, dgst2)
	assert.NoError(t, err)

	// Simulate read-only secondary root on non-Windows/non-root
	if runtime.GOOS != "windows" && os.Getuid() != 0 {
		algDir := filepath.Join(sec2Dir, "blobs", dgst2.Algorithm().String())
		assert.NoError(t, os.Chmod(algDir, 0o555))
		defer os.Chmod(algDir, 0o755)

		// Update on a secondary-root blob succeeds without mutating the read-only secondary root file
		updatedInfo, err := cs.Update(ctx, content.Info{
			Digest: dgst2,
			Labels: map[string]string{"test-label": "val"},
		}, "labels.test-label")
		assert.NoError(t, err)
		assert.Equal(t, "val", updatedInfo.Labels["test-label"])

		err = cs.Delete(ctx, dgst2)
		assert.NoError(t, err)

		// Backing file still exists on read-only disk, and store treats it as deleted
		_, err = os.Stat(filepath.Join(algDir, dgst2.Encoded()))
		assert.NoError(t, err)
		_, err = cs.Info(ctx, dgst2)
		assert.True(t, errdefs.IsNotFound(err))

		// Re-writing the deleted blob writes it to primaryDir and clears deleted marker
		checkWrite(ctx, t, cs, dgst2, blob2)
		_, err = cs.Info(ctx, dgst2)
		assert.NoError(t, err)
	}

	// Simulate secondary root disappearing at runtime
	blobDisappear := []byte("blob-in-disappearing-sec")
	dgstDisappear := digest.FromBytes(blobDisappear)
	sec3Dir := t.TempDir()
	sec3Store, err := NewStore(sec3Dir)
	assert.NoError(t, err)
	checkWrite(ctx, t, sec3Store, dgstDisappear, blobDisappear)

	cs3, err := NewStoreWithSecondaryRoots(primaryDir, []string{sec3Dir})
	assert.NoError(t, err)
	_, err = cs3.Info(ctx, dgstDisappear)
	assert.NoError(t, err)

	assert.NoError(t, os.RemoveAll(sec3Dir))
	_, err = cs3.Info(ctx, dgstDisappear)
	assert.True(t, errdefs.IsNotFound(err))

	// Can now write the disappeared blob to primaryDir
	checkWrite(ctx, t, cs3, dgstDisappear, blobDisappear)
	_, err = cs3.Info(ctx, dgstDisappear)
	assert.NoError(t, err)
}
