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
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/metadata/boltutil"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	digest "github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
)

type secondaryRootSnapshotter interface {
	SecondaryRoots() []string
	SnapshotDirExists(sourceRoot, sourceID string) bool
	ImportCommittedSnapshot(ctx context.Context, key string, info snapshots.Info, usage snapshots.Usage, sourceRoot, sourceID string) error
	RemoveMetadata(ctx context.Context, key string) error
}

type batchSecondaryRootSnapshotter interface {
	ImportCommittedSnapshots(ctx context.Context, imports []storage.CommittedSnapshotImport) error
}

type deletedDigestsStore interface {
	SetDeletedDigests(digests []digest.Digest)
}

type secondaryRootBlobChecker interface {
	HasBlobInSecondaryRoot(index int, dgst digest.Digest) bool
}

type secondaryRootBlobInspector interface {
	InspectSecondaryRootBlob(dgst digest.Digest) (content.Info, int, bool)
}

type secondaryRootBlobRestorer interface {
	RestoreFromSecondaryRoot(dgst digest.Digest) (content.Info, int, bool)
}

type secondaryRootBlobResolver interface {
	ResolveBlobSecondaryRoot(dgst digest.Digest) (int, bool)
}

func (m *DB) inspectSecondaryRootBlob(dgst digest.Digest) (content.Info, string, bool) {
	if m.cs == nil || m.cs.Store == nil {
		return content.Info{}, "", false
	}
	if inspector, ok := m.cs.Store.(secondaryRootBlobInspector); ok {
		st, idx, ok := inspector.InspectSecondaryRootBlob(dgst)
		if !ok || idx < 0 || idx >= len(m.dbopts.secondaryRoots) {
			return content.Info{}, "", false
		}
		return st, filepath.Clean(m.dbopts.secondaryRoots[idx]), true
	}
	return m.restoreSecondaryRootBlob(dgst)
}

func (m *DB) restoreSecondaryRootBlob(dgst digest.Digest) (content.Info, string, bool) {
	if m.cs == nil || m.cs.Store == nil {
		return content.Info{}, "", false
	}
	restorer, ok := m.cs.Store.(secondaryRootBlobRestorer)
	if !ok {
		return content.Info{}, "", false
	}
	st, idx, ok := restorer.RestoreFromSecondaryRoot(dgst)
	if !ok || idx < 0 || idx >= len(m.dbopts.secondaryRoots) {
		return content.Info{}, "", false
	}
	return st, filepath.Clean(m.dbopts.secondaryRoots[idx]), true
}

func (m *DB) resolveBlobSourceRoot(dgst digest.Digest) string {
	if m.cs == nil || m.cs.Store == nil {
		return ""
	}
	resolver, ok := m.cs.Store.(secondaryRootBlobResolver)
	if !ok {
		return ""
	}
	idx, ok := resolver.ResolveBlobSecondaryRoot(dgst)
	if !ok || idx < 0 || idx >= len(m.dbopts.secondaryRoots) {
		return ""
	}
	return filepath.Clean(m.dbopts.secondaryRoots[idx])
}

func (m *DB) hasSecondaryRoots() bool {
	return len(m.dbopts.secondaryRoots) > 0
}

func hasSecondaryOrigin(bkt *bolt.Bucket) bool {
	return bkt != nil && len(bkt.Get(bucketKeyFromSecondary)) > 0
}

func markFromSecondary(bkt *bolt.Bucket) error {
	return bkt.Put(bucketKeyFromSecondary, []byte{1})
}

func putImageTombstone(tx *bolt.Tx, ns, name string) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectImages)
	if err != nil {
		return err
	}
	return bkt.Put([]byte(name), []byte{1})
}

func clearImageTombstone(tx *bolt.Tx, ns, name string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectImages)
	if bkt == nil || len(bkt.Get([]byte(name))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(name))
	return true
}

func isImageTombstoned(tx *bolt.Tx, ns, name string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectImages)
	if bkt == nil {
		return false
	}
	return len(bkt.Get([]byte(name))) > 0
}

func putContentTombstone(tx *bolt.Tx, ns string, dgst digest.Digest) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectContent)
	if err != nil {
		return err
	}
	if v := bkt.Get([]byte(dgst.String())); len(v) > 0 && v[0] == 2 {
		return nil
	}
	return bkt.Put([]byte(dgst.String()), []byte{1})
}

func putExplicitContentTombstone(tx *bolt.Tx, ns string, dgst digest.Digest) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectContent)
	if err != nil {
		return err
	}
	return bkt.Put([]byte(dgst.String()), []byte{2})
}

func clearContentTombstone(tx *bolt.Tx, ns string, dgst digest.Digest) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectContent)
	if bkt == nil || len(bkt.Get([]byte(dgst.String()))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(dgst.String()))
	return true
}

func isContentTombstoned(tx *bolt.Tx, ns string, dgst digest.Digest) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectContent)
	if bkt == nil {
		return false
	}
	return len(bkt.Get([]byte(dgst.String()))) > 0
}

func isExplicitContentTombstoned(tx *bolt.Tx, ns string, dgst digest.Digest) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectContent)
	if bkt == nil {
		return false
	}
	v := bkt.Get([]byte(dgst.String()))
	return len(v) > 0 && v[0] == 2
}

func putSnapshotTombstone(tx *bolt.Tx, ns, snapshotter, key string) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectSnapshots, []byte(snapshotter))
	if err != nil {
		return err
	}
	if v := bkt.Get([]byte(key)); len(v) > 0 && v[0] == 2 {
		return nil
	}
	return bkt.Put([]byte(key), []byte{1})
}

func putExplicitSnapshotTombstone(tx *bolt.Tx, ns, snapshotter, key string) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectSnapshots, []byte(snapshotter))
	if err != nil {
		return err
	}
	return bkt.Put([]byte(key), []byte{2})
}

func clearSnapshotTombstone(tx *bolt.Tx, ns, snapshotter, key string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectSnapshots, []byte(snapshotter))
	if bkt == nil || len(bkt.Get([]byte(key))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(key))
	return true
}

func isSnapshotTombstoned(tx *bolt.Tx, ns, snapshotter, key string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectSnapshots, []byte(snapshotter))
	if bkt == nil {
		return false
	}
	return len(bkt.Get([]byte(key))) > 0
}

func isExplicitSnapshotTombstoned(tx *bolt.Tx, ns, snapshotter, key string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyObjectSnapshots, []byte(snapshotter))
	if bkt == nil {
		return false
	}
	v := bkt.Get([]byte(key))
	return len(v) > 0 && v[0] == 2
}

func putSnapshotOrigin(tx *bolt.Tx, ns, snapshotter, key string) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectSnapshots, []byte(snapshotter))
	if err != nil {
		return err
	}
	return bkt.Put([]byte(key), []byte{1})
}

func consumeSnapshotOrigin(tx *bolt.Tx, ns, snapshotter, key string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectSnapshots, []byte(snapshotter))
	if bkt == nil || len(bkt.Get([]byte(key))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(key))
	return true
}

func putImageOrigin(tx *bolt.Tx, ns, name string) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectImages)
	if err != nil {
		return err
	}
	return bkt.Put([]byte(name), []byte{1})
}

func consumeImageOrigin(tx *bolt.Tx, ns, name string) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectImages)
	if bkt == nil || len(bkt.Get([]byte(name))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(name))
	return true
}

func putContentOrigin(tx *bolt.Tx, ns string, dgst digest.Digest) error {
	bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectContent)
	if err != nil {
		return err
	}
	return bkt.Put([]byte(dgst.String()), []byte{1})
}

func hasContentOrigin(tx *bolt.Tx, ns string, dgst digest.Digest) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectContent)
	if bkt == nil {
		return false
	}
	return len(bkt.Get([]byte(dgst.String()))) > 0
}

func consumeContentOrigin(tx *bolt.Tx, ns string, dgst digest.Digest) bool {
	bkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectTombstones, bucketKeyFromSecondary, bucketKeyObjectContent)
	if bkt == nil || len(bkt.Get([]byte(dgst.String()))) == 0 {
		return false
	}
	_ = bkt.Delete([]byte(dgst.String()))
	return true
}

func (m *DB) findSharedContentSourceRoot(ctx context.Context, tx *bolt.Tx, dgst digest.Digest) (foundActive bool, sourceRoot string) {
	v1bkt := tx.Bucket(bucketKeyVersion)
	if v1bkt == nil {
		return false, ""
	}
	v1c := v1bkt.Cursor()
	for nk, nv := v1c.First(); nk != nil; nk, nv = v1c.Next() {
		if nv != nil {
			continue
		}
		bkt := getBlobBucket(tx, string(nk), dgst)
		if bkt == nil || !m.isSecondaryBlobAvailable(ctx, bkt, dgst) {
			continue
		}
		foundActive = true
		sr := string(bkt.Get(bucketKeySourceRoot))
		if sr == "" {
			return true, ""
		}
		if sourceRoot == "" {
			sourceRoot = sr
		}
	}
	return foundActive, sourceRoot
}

var (
	labelGCContentRefManifest = []byte("containerd.io/gc.ref.content.m.")
	labelGCContentRefLayer    = []byte("containerd.io/gc.ref.content.l.")
)

type imageAvailCache struct {
	roots     map[string]bool
	manifests map[digest.Digest]bool
	blobs     map[digest.Digest]bool
	snaps     map[string]bool
}

func newImageAvailCache() *imageAvailCache {
	return &imageAvailCache{
		roots:     map[string]bool{},
		manifests: map[digest.Digest]bool{},
		blobs:     map[digest.Digest]bool{},
		snaps:     map[string]bool{},
	}
}

func (m *DB) isSecondaryImageAvailable(ctx context.Context, tx *bolt.Tx, ns string, ibkt *bolt.Bucket, targetDigest digest.Digest) bool {
	return m.isSecondaryImageAvailableCached(ctx, tx, ns, ibkt, targetDigest, nil)
}

func (m *DB) isSecondaryImageAvailableCached(ctx context.Context, tx *bolt.Tx, ns string, ibkt *bolt.Bucket, targetDigest digest.Digest, cache *imageAvailCache) bool {
	sr := string(ibkt.Get(bucketKeySourceRoot))
	if sr == "" {
		return true
	}
	if cache != nil {
		if avail, ok := cache.roots[sr]; ok {
			if !avail {
				return false
			}
		} else {
			_, err := os.Stat(sr)
			avail := err == nil
			cache.roots[sr] = avail
			if !avail {
				return false
			}
		}
	} else if _, err := os.Stat(sr); err != nil {
		return false
	}
	if m.cs != nil && m.cs.Store != nil && targetDigest != "" {
		if cache == nil {
			cache = newImageAvailCache()
		}
		return m.isImageTreeAvailableVisited(ctx, tx, ns, targetDigest, true, nil, map[digest.Digest]bool{}, cache)
	}
	return true
}

func (m *DB) isImageTreeAvailable(ctx context.Context, tx *bolt.Tx, ns string, targetDigest digest.Digest, verifyStorage bool, expectedBlobs map[digest.Digest]struct{}) bool {
	var cache *imageAvailCache
	if expectedBlobs == nil {
		cache = newImageAvailCache()
	}
	return m.isImageTreeAvailableVisited(ctx, tx, ns, targetDigest, verifyStorage, expectedBlobs, map[digest.Digest]bool{}, cache)
}

func (m *DB) isImageTreeAvailableVisited(ctx context.Context, tx *bolt.Tx, ns string, targetDigest digest.Digest, verifyStorage bool, expectedBlobs map[digest.Digest]struct{}, inProgress map[digest.Digest]bool, cache *imageAvailCache) (avail bool) {
	if targetDigest == "" || inProgress[targetDigest] {
		return true
	}
	if cache != nil {
		if cached, ok := cache.manifests[targetDigest]; ok {
			return cached
		}
		defer func() {
			cache.manifests[targetDigest] = avail
		}()
	}
	inProgress[targetDigest] = true
	defer delete(inProgress, targetDigest)

	visited := map[digest.Digest]bool{targetDigest: true}
	queue := []digest.Digest{targetDigest}
	seenSnaps := map[string]bool{}
	hasSnapRef := false
	missingLayerBlob := false

	for len(queue) > 0 {
		dgst := queue[0]
		queue = queue[1:]

		bbkt := getBlobBucket(tx, ns, dgst)
		if bbkt == nil {
			return false
		}
		if verifyStorage {
			if cache != nil {
				blobOK, ok := cache.blobs[dgst]
				if !ok {
					blobOK = m.isSecondaryBlobAvailable(ctx, bbkt, dgst)
					cache.blobs[dgst] = blobOK
				}
				if !blobOK {
					return false
				}
			} else if !m.isSecondaryBlobAvailable(ctx, bbkt, dgst) {
				return false
			}
		}
		lbkt := bbkt.Bucket(bucketKeyObjectLabels)
		if lbkt == nil {
			continue
		}
		validRefs := true
		hasManifestRefs := false
		foundManifestRef := false
		_ = lbkt.ForEach(func(k, v []byte) error {
			if !validRefs {
				return nil
			}
			if bytes.HasPrefix(k, labelGCContentRef) {
				if len(k) == len(labelGCContentRef) || k[len(labelGCContentRef)] == '.' || k[len(labelGCContentRef)] == '/' {
					childDgst, err := digest.Parse(string(v))
					if err != nil {
						validRefs = false
						return nil
					}
					if bytes.HasPrefix(k, labelGCContentRefManifest) {
						hasManifestRefs = true
						if isContentTombstoned(tx, ns, childDgst) {
							return nil
						}
						if expectedBlobs != nil {
							if _, expected := expectedBlobs[childDgst]; expected && getBlobBucket(tx, ns, childDgst) == nil {
								validRefs = false
								return nil
							}
						}
						if getBlobBucket(tx, ns, childDgst) == nil {
							return nil
						}
						if m.isImageTreeAvailableVisited(ctx, tx, ns, childDgst, verifyStorage, expectedBlobs, inProgress, cache) {
							foundManifestRef = true
						}
						return nil
					} else if bytes.HasPrefix(k, labelGCContentRefLayer) {
						if isContentTombstoned(tx, ns, childDgst) {
							validRefs = false
							return nil
						}
						if expectedBlobs != nil {
							if _, expected := expectedBlobs[childDgst]; expected && getBlobBucket(tx, ns, childDgst) == nil {
								validRefs = false
								return nil
							}
						}
						if getBlobBucket(tx, ns, childDgst) == nil {
							if hasContentOrigin(tx, ns, childDgst) {
								validRefs = false
								return nil
							}
							missingLayerBlob = true
							return nil
						}
					}
					if !visited[childDgst] {
						visited[childDgst] = true
						queue = append(queue, childDgst)
					}
				}
				return nil
			}
			if bytes.HasPrefix(k, labelGCSnapRef) {
				snName := string(k[len(labelGCSnapRef):])
				if i := strings.IndexByte(snName, '/'); i >= 0 {
					snName = snName[:i]
				}
				snapKey := string(v)
				if snName == "" || snapKey == "" {
					return nil
				}
				if len(m.ss) > 0 && m.ss[snName] == nil {
					return nil
				}
				ssbkt := getSnapshotterBucket(tx, ns, snName)
				if ssbkt == nil {
					validRefs = false
					return nil
				}
				hasSnapRef = true
				stattedChain := false
				curr := snapKey
				for curr != "" && !seenSnaps[snName+"/"+curr] {
					seenSnaps[snName+"/"+curr] = true
					sbkt := ssbkt.Bucket([]byte(curr))
					if sbkt == nil {
						validRefs = false
						return nil
					}
					if verifyStorage && !stattedChain && len(sbkt.Get(bucketKeySourceRoot)) > 0 {
						stattedChain = true
						if sn := m.ss[snName]; sn != nil {
							if bkey := string(sbkt.Get(bucketKeyName)); bkey != "" {
								if cache != nil {
									snapOK, ok := cache.snaps[snName+"/"+bkey]
									if !ok {
										_, err := sn.Snapshotter.Stat(ctx, bkey)
										snapOK = err == nil
										cache.snaps[snName+"/"+bkey] = snapOK
									}
									if !snapOK {
										validRefs = false
										return nil
									}
								} else if _, err := sn.Snapshotter.Stat(ctx, bkey); err != nil {
									validRefs = false
									return nil
								}
							}
						}
					}
					curr = string(sbkt.Get(bucketKeyParent))
				}
			}
			return nil
		})
		if !validRefs || (hasManifestRefs && !foundManifestRef) {
			return false
		}
	}
	if missingLayerBlob && !hasSnapRef {
		return false
	}
	return true
}

func hasGCRefLabel(labels map[string]string) bool {
	for k := range labels {
		if strings.HasPrefix(k, string(labelGCContentRef)) || strings.HasPrefix(k, string(labelGCSnapRef)) {
			return true
		}
	}
	return false
}

func (m *DB) updateImageSourceRoot(ctx context.Context, tx *bolt.Tx, ns string, ibkt *bolt.Bucket, targetDigest digest.Digest, verifyStorage bool) {
	currentSR := string(ibkt.Get(bucketKeySourceRoot))
	if currentSR == "" {
		return
	}
	newSR := m.findImageTreeSourceRoot(ctx, tx, ns, currentSR, targetDigest, verifyStorage)
	if newSR == currentSR {
		return
	}
	if !m.isImageTreeAvailable(ctx, tx, ns, targetDigest, verifyStorage, nil) {
		return
	}
	if newSR == "" {
		_ = ibkt.Delete(bucketKeySourceRoot)
	} else {
		_ = ibkt.Put(bucketKeySourceRoot, []byte(newSR))
	}
}

func (m *DB) markUnavailableSecondaryImages(ctx context.Context, tx *bolt.Tx, ns string) {
	if !m.hasSecondaryRoots() {
		return
	}
	ibkt := getImagesBucket(tx, ns)
	if ibkt == nil {
		return
	}
	cache := newImageAvailCache()
	_ = ibkt.ForEach(func(k, v []byte) error {
		if v != nil {
			return nil
		}
		b := ibkt.Bucket(k)
		if b == nil || len(b.Get(bucketKeySourceRoot)) == 0 {
			return nil
		}
		var img images.Image
		if err := readImage(&img, b); err != nil {
			return nil
		}
		if img.Target.Digest != "" && !m.isImageTreeAvailableVisited(ctx, tx, ns, img.Target.Digest, false, nil, map[digest.Digest]bool{}, cache) {
			_ = putImageOrigin(tx, ns, string(k))
		}
		return nil
	})
}

func (m *DB) refreshNamespaceImageSourceRoots(ctx context.Context, tx *bolt.Tx, ns string) {
	if !m.hasSecondaryRoots() {
		return
	}
	ibkt := getImagesBucket(tx, ns)
	if ibkt == nil {
		return
	}
	srCache := map[digest.Digest]string{}
	_ = ibkt.ForEach(func(k, v []byte) error {
		if v != nil {
			return nil
		}
		b := ibkt.Bucket(k)
		if b == nil || hasSecondaryOrigin(b) {
			return nil
		}
		currentSR := string(b.Get(bucketKeySourceRoot))
		var img images.Image
		if err := readImage(&img, b); err != nil || img.Target.Digest == "" {
			return nil
		}
		if currentSR == "" {
			sr, ok := srCache[img.Target.Digest]
			if !ok {
				sr = m.findImageTreeSourceRoot(ctx, tx, ns, "", img.Target.Digest, false)
				srCache[img.Target.Digest] = sr
			}
			if sr != "" {
				_ = b.Put(bucketKeySourceRoot, []byte(sr))
			}
		} else {
			m.updateImageSourceRoot(ctx, tx, ns, b, img.Target.Digest, false)
		}
		return nil
	})
}

func (m *DB) findImageTreeSourceRoot(ctx context.Context, tx *bolt.Tx, ns, preferredSR string, targetDigest digest.Digest, verifyStorage bool) string {
	if (!m.hasSecondaryRoots() && preferredSR == "") || targetDigest == "" {
		return ""
	}
	queue := []digest.Digest{targetDigest}
	visited := map[digest.Digest]bool{targetDigest: true}
	seenSnaps := map[string]bool{}
	var firstSR string

	for len(queue) > 0 {
		dgst := queue[0]
		queue = queue[1:]

		bbkt := getBlobBucket(tx, ns, dgst)
		if bbkt == nil {
			continue
		}
		if sr := string(bbkt.Get(bucketKeySourceRoot)); sr != "" {
			if sr == preferredSR {
				return preferredSR
			}
			if firstSR == "" {
				firstSR = sr
			}
		}
		lbkt := bbkt.Bucket(bucketKeyObjectLabels)
		if lbkt == nil {
			continue
		}
		matchedPreferred := false
		_ = lbkt.ForEach(func(k, v []byte) error {
			if matchedPreferred {
				return nil
			}
			if bytes.HasPrefix(k, labelGCContentRef) {
				if len(k) == len(labelGCContentRef) || k[len(labelGCContentRef)] == '.' || k[len(labelGCContentRef)] == '/' {
					if childDgst, err := digest.Parse(string(v)); err == nil && !visited[childDgst] {
						if bytes.HasPrefix(k, labelGCContentRefManifest) {
							if !m.isImageTreeAvailable(ctx, tx, ns, childDgst, verifyStorage, nil) {
								return nil
							}
						}
						visited[childDgst] = true
						queue = append(queue, childDgst)
					}
				}
				return nil
			}
			if bytes.HasPrefix(k, labelGCSnapRef) {
				snName := string(k[len(labelGCSnapRef):])
				if i := strings.IndexByte(snName, '/'); i >= 0 {
					snName = snName[:i]
				}
				snapKey := string(v)
				if ssbkt := getSnapshotterBucket(tx, ns, snName); ssbkt != nil {
					curr := snapKey
					for curr != "" && !seenSnaps[snName+"/"+curr] {
						seenSnaps[snName+"/"+curr] = true
						sbkt := ssbkt.Bucket([]byte(curr))
						if sbkt == nil {
							break
						}
						if sr := string(sbkt.Get(bucketKeySourceRoot)); sr != "" {
							if sr == preferredSR {
								matchedPreferred = true
								break
							}
							if firstSR == "" {
								firstSR = sr
							}
						}
						curr = string(sbkt.Get(bucketKeyParent))
					}
				}
			}
			return nil
		})
		if matchedPreferred {
			return preferredSR
		}
	}
	return firstSR
}

func (m *DB) isSecondaryBlobAvailable(ctx context.Context, bbkt *bolt.Bucket, dgst digest.Digest) bool {
	sr := bbkt.Get(bucketKeySourceRoot)
	if len(sr) == 0 {
		return true
	}
	if m.cs != nil && m.cs.Store != nil {
		if _, err := m.cs.Store.Info(ctx, dgst); err == nil {
			return true
		}
		return false
	}
	if _, err := os.Stat(string(sr)); err != nil {
		return false
	}
	return true
}

type resolvedSecondaryRoot struct {
	index      int
	metaRoot   string
	metaDBPath string
	snRoots    map[string]string
}

func (m *DB) resolveSecondaryRoots(ctx context.Context) ([]resolvedSecondaryRoot, map[string]int) {
	var active []resolvedSecondaryRoot
	priority := make(map[string]int, len(m.dbopts.secondaryRoots))

	for i, rawRoot := range m.dbopts.secondaryRoots {
		if rawRoot == "" {
			continue
		}
		cleanRoot := filepath.Clean(rawRoot)
		var metaDBPath string
		p1 := filepath.Join(cleanRoot, "meta.db")
		p2 := filepath.Join(cleanRoot, "io.containerd.metadata.v1.bolt", "meta.db")
		if st, err := os.Stat(p1); err == nil && !st.IsDir() && st.Size() > 0 {
			metaDBPath = p1
		} else if st, err := os.Stat(p2); err == nil && !st.IsDir() && st.Size() > 0 {
			metaDBPath = p2
		} else {
			log.G(ctx).WithField("secondary_root", cleanRoot).Debug("secondary root metadata.db not found or unreachable")
			continue
		}

		snRoots := make(map[string]string, len(m.ss))
		for snName, sn := range m.ss {
			if srs, ok := sn.Snapshotter.(secondaryRootSnapshotter); ok {
				roots := srs.SecondaryRoots()
				if i < len(roots) && roots[i] != "" {
					snRoots[snName] = roots[i]
				}
			}
		}

		priority[cleanRoot] = i
		active = append(active, resolvedSecondaryRoot{
			index:      i,
			metaRoot:   cleanRoot,
			metaDBPath: metaDBPath,
			snRoots:    snRoots,
		})
	}

	return active, priority
}

func priorityOf(sourceRoot string, activePriority map[string]int) int {
	if sourceRoot == "" {
		return -1
	}
	if p, ok := activePriority[sourceRoot]; ok {
		return p
	}
	return math.MaxInt
}

func (m *DB) syncSecondaryRoots(ctx context.Context) error {
	activeRoots, activePriority := m.resolveSecondaryRoots(ctx)

	if err := m.pruneDetachedSecondaryRoots(ctx, activePriority); err != nil {
		return err
	}

	rootData := make([]map[string]*secondaryNamespaceData, len(activeRoots))
	mergedByNS := map[string]*secondaryNamespaceData{}
	for i, sr := range activeRoots {
		nsMap, err := readSecondaryMetaDB(ctx, sr.metaDBPath)
		if err != nil {
			log.G(ctx).WithError(err).WithField("secondary_root", sr.metaRoot).Warn("failed to import secondary root")
			continue
		}
		rootData[i] = nsMap
		for ns, data := range nsMap {
			merged := mergedByNS[ns]
			if merged == nil {
				merged = &secondaryNamespaceData{
					blobDigests: map[digest.Digest]struct{}{},
					snapshots:   map[string][]secondarySnapshotEntry{},
				}
				mergedByNS[ns] = merged
			}
			merged.blobs = append(merged.blobs, data.blobs...)
			for d := range data.blobDigests {
				merged.blobDigests[d] = struct{}{}
			}
			for snName, list := range data.snapshots {
				merged.snapshots[snName] = append(merged.snapshots[snName], list...)
			}
			merged.images = append(merged.images, data.images...)
		}
	}

	if len(mergedByNS) > 0 {
		_ = m.db.Update(func(tx *bolt.Tx) error {
			for ns, merged := range mergedByNS {
				var activeImages []images.Image
				seenImages := make(map[string]bool, len(merged.images))
				for _, imgEntry := range merged.images {
					if !seenImages[imgEntry.image.Name] && !isImageTombstoned(tx, ns, imgEntry.image.Name) {
						seenImages[imgEntry.image.Name] = true
						activeImages = append(activeImages, imgEntry.image)
					}
				}
				if len(activeImages) == 0 {
					continue
				}
				refBlobs, refSnaps := collectReferencedByImages(tx, ns, merged, activeImages)
				for dgst := range refBlobs {
					if isContentTombstoned(tx, ns, dgst) && !isExplicitContentTombstoned(tx, ns, dgst) {
						clearContentTombstone(tx, ns, dgst)
					}
				}
				for snName, snaps := range refSnaps {
					for snapKey := range snaps {
						if isSnapshotTombstoned(tx, ns, snName, snapKey) && !isExplicitSnapshotTombstoned(tx, ns, snName, snapKey) {
							clearSnapshotTombstone(tx, ns, snName, snapKey)
						}
					}
				}
			}
			return nil
		})
	}

	for i, sr := range activeRoots {
		if rootData[i] == nil {
			continue
		}
		if err := m.importSecondaryRoot(ctx, sr, rootData[i], activePriority); err != nil {
			log.G(ctx).WithError(err).WithField("secondary_root", sr.metaRoot).Warn("failed to import secondary root")
		}
	}

	return m.syncDeletedDigests()
}

func (m *DB) syncDeletedDigests() error {
	if m.cs == nil || m.cs.Store == nil {
		return nil
	}
	dds, ok := m.cs.Store.(deletedDigestsStore)
	if !ok {
		return nil
	}
	m.deletedDigestsMu.Lock()
	defer m.deletedDigestsMu.Unlock()
	deleted := map[digest.Digest]struct{}{}
	active := map[digest.Digest]struct{}{}
	err := m.db.View(func(tx *bolt.Tx) error {
		v1bkt := tx.Bucket(bucketKeyVersion)
		if v1bkt == nil {
			return nil
		}
		var namespaces []string
		if err := v1bkt.ForEach(func(nsKey, nsVal []byte) error {
			if nsVal != nil {
				return nil
			}
			namespaces = append(namespaces, string(nsKey))
			cbkt := getBucket(tx, bucketKeyVersion, nsKey, bucketKeyObjectTombstones, bucketKeyObjectContent)
			if cbkt == nil {
				return nil
			}
			return cbkt.ForEach(func(k, _ []byte) error {
				if d, err := digest.Parse(string(k)); err == nil {
					deleted[d] = struct{}{}
				}
				return nil
			})
		}); err != nil {
			return err
		}
		if len(deleted) == 0 {
			return nil
		}
		for _, ns := range namespaces {
			if bbkt := getBlobsBucket(tx, ns); bbkt != nil {
				for d := range deleted {
					if bbkt.Bucket([]byte(d.String())) != nil {
						active[d] = struct{}{}
					}
				}
			}
			if ibkt := getIngestsBucket(tx, ns); ibkt != nil {
				_ = ibkt.ForEach(func(k, v []byte) error {
					if v == nil {
						if ing := ibkt.Bucket(k); ing != nil {
							if exp := ing.Get(bucketKeyExpected); len(exp) > 0 {
								if d, err := digest.Parse(string(exp)); err == nil {
									if _, isDel := deleted[d]; isDel {
										active[d] = struct{}{}
									}
								}
							}
						}
					}
					return nil
				})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	var digests []digest.Digest
	for d := range deleted {
		if _, isActive := active[d]; !isActive {
			digests = append(digests, d)
		}
	}
	dds.SetDeletedDigests(digests)
	return nil
}

type pendingSnapRemove struct {
	sn         *snapshotter
	bkey       string
	sourceRoot string
}

func (p pendingSnapRemove) execute(ctx context.Context) {
	if p.sn == nil || p.bkey == "" {
		return
	}
	if p.sourceRoot == "" {
		_ = p.sn.Snapshotter.Remove(ctx, p.bkey)
	} else if srs, ok := p.sn.Snapshotter.(secondaryRootSnapshotter); ok {
		_ = srs.RemoveMetadata(ctx, p.bkey)
	} else {
		_ = p.sn.Snapshotter.Remove(ctx, p.bkey)
	}
}

type pendingSnapImport struct {
	srs    secondaryRootSnapshotter
	bkey   string
	info   snapshots.Info
	usage  snapshots.Usage
	snRoot string
	id     string
}

func hasAnySecondarySourceRoot(tx *bolt.Tx) bool {
	v1bkt := tx.Bucket(bucketKeyVersion)
	if v1bkt == nil {
		return false
	}
	found := false
	_ = v1bkt.ForEach(func(k, v []byte) error {
		if found || v != nil {
			return nil
		}
		nbkt := v1bkt.Bucket(k)
		if nbkt == nil {
			return nil
		}
		if cbkt := nbkt.Bucket(bucketKeyObjectContent); cbkt != nil {
			if bbkt := cbkt.Bucket(bucketKeyObjectBlob); bbkt != nil {
				_ = bbkt.ForEach(func(bk, bv []byte) error {
					if !found && bv == nil {
						if b := bbkt.Bucket(bk); b != nil && len(b.Get(bucketKeySourceRoot)) > 0 {
							found = true
						}
					}
					return nil
				})
			}
		}
		if !found {
			if sbkt := nbkt.Bucket(bucketKeyObjectSnapshots); sbkt != nil {
				_ = sbkt.ForEach(func(sk, sv []byte) error {
					if !found && sv == nil {
						if ssbkt := sbkt.Bucket(sk); ssbkt != nil {
							_ = ssbkt.ForEach(func(k, v []byte) error {
								if !found && v == nil {
									if b := ssbkt.Bucket(k); b != nil && len(b.Get(bucketKeySourceRoot)) > 0 {
										found = true
									}
								}
								return nil
							})
						}
					}
					return nil
				})
			}
		}
		if !found {
			if ibkt := nbkt.Bucket(bucketKeyObjectImages); ibkt != nil {
				_ = ibkt.ForEach(func(ik, iv []byte) error {
					if !found && iv == nil {
						if b := ibkt.Bucket(ik); b != nil && len(b.Get(bucketKeySourceRoot)) > 0 {
							found = true
						}
					}
					return nil
				})
			}
		}
		if !found && nbkt.Bucket(bucketKeyObjectTombstones) != nil {
			found = true
		}
		return nil
	})
	return found
}

func (m *DB) pruneDetachedSecondaryRoots(ctx context.Context, activePriority map[string]int) error {
	if len(activePriority) == 0 {
		hasSecondary := false
		if err := m.db.View(func(tx *bolt.Tx) error {
			hasSecondary = hasAnySecondarySourceRoot(tx)
			return nil
		}); err != nil || !hasSecondary {
			return err
		}
	}
	var pendingRemovals []pendingSnapRemove
	err := m.db.Update(func(tx *bolt.Tx) error {
		v1bkt := tx.Bucket(bucketKeyVersion)
		if v1bkt == nil {
			return nil
		}

		var namespaces []string
		if err := v1bkt.ForEach(func(k, v []byte) error {
			if v == nil {
				namespaces = append(namespaces, string(k))
			}
			return nil
		}); err != nil {
			return err
		}

		for _, ns := range namespaces {
			nbkt := v1bkt.Bucket([]byte(ns))
			if nbkt == nil {
				continue
			}
			if len(m.dbopts.secondaryRoots) == 0 {
				_ = nbkt.DeleteBucket(bucketKeyObjectTombstones)
			}

			// 1. Prune content blobs from detached/removed secondary roots
			prunedBlobs := map[digest.Digest]struct{}{}
			if cbkt := nbkt.Bucket(bucketKeyObjectContent); cbkt != nil {
				if bbkt := cbkt.Bucket(bucketKeyObjectBlob); bbkt != nil {
					var toDelete []digest.Digest
					_ = bbkt.ForEach(func(k, v []byte) error {
						if v != nil {
							return nil
						}
						b := bbkt.Bucket(k)
						if b == nil {
							return nil
						}
						sr := string(b.Get(bucketKeySourceRoot))
						if sr != "" {
							dgst, err := digest.Parse(string(k))
							if err != nil {
								return nil
							}
							if _, ok := activePriority[sr]; !ok {
								toDelete = append(toDelete, dgst)
							} else if m.cs != nil && m.cs.Store != nil {
								if _, err := m.cs.Store.Info(ctx, dgst); err != nil && errdefs.IsNotFound(err) {
									toDelete = append(toDelete, dgst)
								}
							}
						}
						return nil
					})
					for _, dgst := range toDelete {
						prunedBlobs[dgst] = struct{}{}
						deleteContentFromAllLeases(tx, ns, dgst)
						_ = bbkt.DeleteBucket([]byte(dgst.String()))
					}
				}
			}

			// 2. Prune snapshots from detached/removed secondary roots
			if sbkt := nbkt.Bucket(bucketKeyObjectSnapshots); sbkt != nil {
				var snNames []string
				_ = sbkt.ForEach(func(k, v []byte) error {
					if v == nil {
						snNames = append(snNames, string(k))
					}
					return nil
				})

				for _, snName := range snNames {
					ssbkt := sbkt.Bucket([]byte(snName))
					if ssbkt == nil {
						continue
					}
					sn := m.ss[snName]
					if err := pruneDetachedSnapshotsInBucket(ctx, tx, ns, snName, sn, ssbkt, activePriority, &pendingRemovals); err != nil {
						return err
					}
				}
			}

			// 3. Prune images from detached/removed secondary roots or missing target blobs/snapshots
			if ibkt := nbkt.Bucket(bucketKeyObjectImages); ibkt != nil {
				var toDelete []string
				_ = ibkt.ForEach(func(k, v []byte) error {
					if v != nil {
						return nil
					}
					b := ibkt.Bucket(k)
					if b == nil {
						return nil
					}
					sr := string(b.Get(bucketKeySourceRoot))
					if sr != "" {
						var img images.Image
						err := readImage(&img, b)
						if err == nil && m.isImageTreeAvailable(ctx, tx, ns, img.Target.Digest, true, prunedBlobs) && (!hasSecondaryOrigin(b) || m.findImageTreeSourceRoot(ctx, tx, ns, "", img.Target.Digest, false) == "") {
							m.updateImageSourceRoot(ctx, tx, ns, b, img.Target.Digest, false)
							sr = string(b.Get(bucketKeySourceRoot))
						}
						if sr != "" {
							if _, ok := activePriority[sr]; !ok {
								toDelete = append(toDelete, string(k))
								return nil
							}
							if err == nil {
								if _, statErr := os.Stat(sr); statErr != nil || !m.isImageTreeAvailable(ctx, tx, ns, img.Target.Digest, true, prunedBlobs) {
									toDelete = append(toDelete, string(k))
								}
							}
						}
					}
					return nil
				})
				for _, name := range toDelete {
					deleteImageFromAllLeases(tx, ns, name)
					_ = ibkt.DeleteBucket([]byte(name))
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	for _, r := range pendingRemovals {
		r.execute(ctx)
	}
	return nil
}

func deleteContentFromAllLeases(tx *bolt.Tx, ns string, dgst digest.Digest) {
	lbkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectLeases)
	if lbkt == nil {
		return
	}
	_ = lbkt.ForEach(func(k, v []byte) error {
		if v == nil {
			if l := lbkt.Bucket(k); l != nil {
				if cbkt := l.Bucket(bucketKeyObjectContent); cbkt != nil {
					_ = cbkt.Delete([]byte(dgst.String()))
				}
			}
		}
		return nil
	})
}

func deleteSnapshotFromAllLeases(tx *bolt.Tx, ns, snName, key string) {
	lbkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectLeases)
	if lbkt == nil {
		return
	}
	_ = lbkt.ForEach(func(k, v []byte) error {
		if v == nil {
			if l := lbkt.Bucket(k); l != nil {
				if sbkt := l.Bucket(bucketKeyObjectSnapshots); sbkt != nil {
					if ssbkt := sbkt.Bucket([]byte(snName)); ssbkt != nil {
						_ = ssbkt.Delete([]byte(key))
					}
				}
			}
		}
		return nil
	})
}

func deleteImageFromAllLeases(tx *bolt.Tx, ns, name string) {
	lbkt := getBucket(tx, bucketKeyVersion, []byte(ns), bucketKeyObjectLeases)
	if lbkt == nil {
		return
	}
	_ = lbkt.ForEach(func(k, v []byte) error {
		if v == nil {
			if l := lbkt.Bucket(k); l != nil {
				if ibkt := l.Bucket(bucketKeyObjectImages); ibkt != nil {
					_ = ibkt.Delete([]byte(name))
				}
			}
		}
		return nil
	})
}

type snapMetaNode struct {
	key        string
	bkey       string
	parent     string
	sourceRoot string
	children   []string
	prune      bool
}

func pruneDetachedSnapshotsInBucket(ctx context.Context, tx *bolt.Tx, ns, snName string, sn *snapshotter, ssbkt *bolt.Bucket, activePriority map[string]int, pendingRemovals *[]pendingSnapRemove) error {
	nodes := map[string]*snapMetaNode{}
	_ = ssbkt.ForEach(func(k, v []byte) error {
		if v != nil {
			return nil
		}
		b := ssbkt.Bucket(k)
		if b == nil {
			return nil
		}
		key := string(k)
		node := &snapMetaNode{
			key:        key,
			bkey:       string(b.Get(bucketKeyName)),
			parent:     string(b.Get(bucketKeyParent)),
			sourceRoot: string(b.Get(bucketKeySourceRoot)),
		}
		if cbkt := b.Bucket(bucketKeyChildren); cbkt != nil {
			_ = cbkt.ForEach(func(ck, _ []byte) error {
				node.children = append(node.children, string(ck))
				return nil
			})
		}
		if node.sourceRoot != "" {
			if _, ok := activePriority[node.sourceRoot]; !ok {
				node.prune = true
			} else if sn != nil && node.bkey != "" {
				if _, err := sn.Snapshotter.Stat(ctx, node.bkey); err != nil && errdefs.IsNotFound(err) {
					node.prune = true
				}
			}
		}
		nodes[key] = node
		return nil
	})

	// Also link children via parent pointers in case bucketKeyChildren was incomplete
	for _, n := range nodes {
		if n.parent != "" {
			if p, ok := nodes[n.parent]; ok {
				found := false
				for _, c := range p.children {
					if c == n.key {
						found = true
						break
					}
				}
				if !found {
					p.children = append(p.children, n.key)
				}
			}
		}
	}

	// Propagate prune flag from parents to all descendants
	var markDescendants func(key string)
	markDescendants = func(key string) {
		n, ok := nodes[key]
		if !ok {
			return
		}
		n.prune = true
		for _, child := range n.children {
			if cn, ok := nodes[child]; ok && !cn.prune {
				markDescendants(child)
			}
		}
	}
	for _, n := range nodes {
		if n.prune {
			for _, child := range n.children {
				markDescendants(child)
			}
		}
	}

	// Collect pruned nodes in post-order (children before parents)
	visited := map[string]bool{}
	var postOrder []*snapMetaNode
	var visit func(key string)
	visit = func(key string) {
		if visited[key] {
			return
		}
		visited[key] = true
		n, ok := nodes[key]
		if !ok || !n.prune {
			return
		}
		for _, child := range n.children {
			visit(child)
		}
		postOrder = append(postOrder, n)
	}
	for k, n := range nodes {
		if n.prune {
			visit(k)
		}
	}

	for _, n := range postOrder {
		if sn != nil && n.bkey != "" {
			*pendingRemovals = append(*pendingRemovals, pendingSnapRemove{
				sn:         sn,
				bkey:       n.bkey,
				sourceRoot: n.sourceRoot,
			})
		}
		if n.parent != "" {
			if pbkt := ssbkt.Bucket([]byte(n.parent)); pbkt != nil {
				if cbkt := pbkt.Bucket(bucketKeyChildren); cbkt != nil {
					_ = cbkt.Delete([]byte(n.key))
				}
			}
		}
		deleteSnapshotFromAllLeases(tx, ns, snName, n.key)
		_ = ssbkt.DeleteBucket([]byte(n.key))
	}

	return nil
}

func hasActiveHigherPriorityDescendant(bkt *bolt.Bucket, rootKey string, srIndex int, activePriority map[string]int) bool {
	visited := map[string]bool{}
	var check func(key string) bool
	check = func(key string) bool {
		if visited[key] {
			return false
		}
		visited[key] = true
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return false
		}
		if key != rootKey {
			if priorityOf(string(sbkt.Get(bucketKeySourceRoot)), activePriority) <= srIndex {
				return true
			}
		}
		cbkt := sbkt.Bucket(bucketKeyChildren)
		if cbkt == nil {
			return false
		}
		found := false
		_ = cbkt.ForEach(func(ck, _ []byte) error {
			if check(string(ck)) {
				found = true
			}
			return nil
		})
		return found
	}
	return check(rootKey)
}

func hasSnapshotChainSecondaryRoot(bkt *bolt.Bucket, key string) bool {
	visited := map[string]bool{}
	curr := key
	for curr != "" && !visited[curr] {
		visited[curr] = true
		sbkt := bkt.Bucket([]byte(curr))
		if sbkt == nil {
			return false
		}
		if len(sbkt.Get(bucketKeySourceRoot)) > 0 {
			return true
		}
		curr = string(sbkt.Get(bucketKeyParent))
	}
	return false
}

func (s *snapshotter) removeStaleSnapshotTree(ctx context.Context, tx *bolt.Tx, ns string, bkt *bolt.Bucket, rootKey string) ([]pendingSnapRemove, error) {
	rems, err := s.removeSnapshotTree(ctx, tx, ns, bkt, rootKey, "")
	if err == nil {
		s.db.markUnavailableSecondaryImages(ctx, tx, ns)
	}
	return rems, err
}

func (s *snapshotter) removeSnapshotTree(ctx context.Context, tx *bolt.Tx, ns string, bkt *bolt.Bucket, rootKey string, preserveLeaseKey string) ([]pendingSnapRemove, error) {
	// Walk up parent chain to find the highest ancestor whose snapshot is also missing
	visitedAncestors := map[string]bool{}
	curr := rootKey
	for curr != "" && !visitedAncestors[curr] {
		visitedAncestors[curr] = true
		sbkt := bkt.Bucket([]byte(curr))
		if sbkt == nil {
			break
		}
		parent := string(sbkt.Get(bucketKeyParent))
		if parent == "" {
			break
		}
		pbkt := bkt.Bucket([]byte(parent))
		if pbkt == nil {
			break
		}
		parentBKey := string(pbkt.Get(bucketKeyName))
		if parentBKey == "" {
			break
		}
		if _, err := s.Snapshotter.Stat(ctx, parentBKey); err != nil && errdefs.IsNotFound(err) {
			rootKey = parent
			curr = parent
			continue
		}
		break
	}

	visited := map[string]bool{}
	var postOrder []string

	var visit func(key string)
	visit = func(key string) {
		if visited[key] {
			return
		}
		visited[key] = true
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return
		}
		if cbkt := sbkt.Bucket(bucketKeyChildren); cbkt != nil {
			_ = cbkt.ForEach(func(ck, _ []byte) error {
				visit(string(ck))
				return nil
			})
		}
		postOrder = append(postOrder, key)
	}

	visit(rootKey)

	var removals []pendingSnapRemove
	for _, key := range postOrder {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			continue
		}
		if hasSecondaryOrigin(sbkt) {
			_ = putSnapshotOrigin(tx, ns, s.name, key)
		}
		bkey := string(sbkt.Get(bucketKeyName))
		parent := string(sbkt.Get(bucketKeyParent))
		sourceRoot := string(sbkt.Get(bucketKeySourceRoot))
		if bkey != "" {
			removals = append(removals, pendingSnapRemove{
				sn:         s,
				bkey:       bkey,
				sourceRoot: sourceRoot,
			})
		}
		if parent != "" {
			if pbkt := bkt.Bucket([]byte(parent)); pbkt != nil {
				if cbkt := pbkt.Bucket(bucketKeyChildren); cbkt != nil {
					_ = cbkt.Delete([]byte(key))
				}
			}
		}
		if key != preserveLeaseKey {
			deleteSnapshotFromAllLeases(tx, ns, s.name, key)
		}
		_ = bkt.DeleteBucket([]byte(key))
	}
	return removals, nil
}

type secondaryBlobEntry struct {
	info content.Info
}

type secondarySnapshotEntry struct {
	key        string
	backendKey string
	parent     string
	info       snapshots.Info
}

type secondaryImageEntry struct {
	image images.Image
}

type secondaryNamespaceData struct {
	labels      map[string]string
	blobs       []secondaryBlobEntry
	blobDigests map[digest.Digest]struct{}
	snapshots   map[string][]secondarySnapshotEntry
	images      []secondaryImageEntry
}

func collectReferencedByImages(tx *bolt.Tx, ns string, nsData *secondaryNamespaceData, imgs []images.Image) (map[digest.Digest]bool, map[string]map[string]bool) {
	refBlobs := map[digest.Digest]bool{}
	refSnaps := map[string]map[string]bool{}
	if len(imgs) == 0 {
		return refBlobs, refSnaps
	}

	secBlobs := make(map[digest.Digest]map[string]string, len(nsData.blobs))
	seenBlobs := make(map[digest.Digest]bool, len(nsData.blobs))
	blobBackRefs := map[digest.Digest][]digest.Digest{}
	snapContentBackRefs := map[digest.Digest][][2]string{}
	snapBackRefs := map[[2]string][][2]string{}

	for _, b := range nsData.blobs {
		if seenBlobs[b.info.Digest] {
			continue
		}
		seenBlobs[b.info.Digest] = true
		if len(b.info.Labels) > 0 {
			secBlobs[b.info.Digest] = b.info.Labels
			for k, v := range b.info.Labels {
				if strings.HasPrefix(k, string(labelGCContentBackRef)) {
					if len(k) == len(labelGCContentBackRef) || k[len(labelGCContentBackRef)] == '.' || k[len(labelGCContentBackRef)] == '/' {
						if target, err := digest.Parse(v); err == nil {
							blobBackRefs[target] = append(blobBackRefs[target], b.info.Digest)
						}
					}
				}
			}
		}
	}
	secSnaps := make(map[string]map[string]secondarySnapshotEntry, len(nsData.snapshots))
	for snName, list := range nsData.snapshots {
		m := make(map[string]secondarySnapshotEntry, len(list))
		for _, e := range list {
			if _, exists := m[e.key]; exists {
				continue
			}
			m[e.key] = e
			for k, v := range e.info.Labels {
				if strings.HasPrefix(k, string(labelGCContentBackRef)) {
					if len(k) == len(labelGCContentBackRef) || k[len(labelGCContentBackRef)] == '.' || k[len(labelGCContentBackRef)] == '/' {
						if target, err := digest.Parse(v); err == nil {
							snapContentBackRefs[target] = append(snapContentBackRefs[target], [2]string{snName, e.key})
						}
					}
				} else if strings.HasPrefix(k, string(labelGCSnapBackRef)) {
					targetSn := k[len(labelGCSnapBackRef):]
					if i := strings.IndexByte(targetSn, '/'); i >= 0 {
						targetSn = targetSn[:i]
					}
					if targetSn != "" && v != "" {
						snapBackRefs[[2]string{targetSn, v}] = append(snapBackRefs[[2]string{targetSn, v}], [2]string{snName, e.key})
					}
				}
			}
		}
		secSnaps[snName] = m
	}

	var blobQueue []digest.Digest
	var markSnapChain func(snName, snapKey string)

	enqueueBlob := func(d digest.Digest) {
		if d != "" && !refBlobs[d] {
			refBlobs[d] = true
			blobQueue = append(blobQueue, d)
			for _, backBlob := range blobBackRefs[d] {
				if !refBlobs[backBlob] {
					refBlobs[backBlob] = true
					blobQueue = append(blobQueue, backBlob)
				}
			}
			for _, backSnap := range snapContentBackRefs[d] {
				markSnapChain(backSnap[0], backSnap[1])
			}
		}
	}

	markSnapChain = func(snName, snapKey string) {
		if snName == "" || snapKey == "" {
			return
		}
		if refSnaps[snName] == nil {
			refSnaps[snName] = map[string]bool{}
		}
		curr := snapKey
		for curr != "" && !refSnaps[snName][curr] {
			refSnaps[snName][curr] = true
			var nextParent string
			if e, ok := secSnaps[snName][curr]; ok {
				nextParent = e.parent
				for k, v := range e.info.Labels {
					if strings.HasPrefix(k, string(labelGCContentRef)) {
						if d, err := digest.Parse(v); err == nil {
							enqueueBlob(d)
						}
					}
				}
			}
			if nextParent == "" {
				if ssbkt := getSnapshotterBucket(tx, ns, snName); ssbkt != nil {
					if sbkt := ssbkt.Bucket([]byte(curr)); sbkt != nil {
						nextParent = string(sbkt.Get(bucketKeyParent))
					}
				}
			}
			for _, backSnap := range snapBackRefs[[2]string{snName, curr}] {
				markSnapChain(backSnap[0], backSnap[1])
			}
			curr = nextParent
		}
	}

	processLabel := func(k, v string) {
		if strings.HasPrefix(k, string(labelGCContentRef)) {
			if len(k) == len(labelGCContentRef) || k[len(labelGCContentRef)] == '.' || k[len(labelGCContentRef)] == '/' {
				if d, err := digest.Parse(v); err == nil {
					enqueueBlob(d)
				}
			}
			return
		}
		if strings.HasPrefix(k, string(labelGCSnapRef)) {
			snName := k[len(labelGCSnapRef):]
			if i := strings.IndexByte(snName, '/'); i >= 0 {
				snName = snName[:i]
			}
			markSnapChain(snName, v)
		}
	}

	for _, img := range imgs {
		enqueueBlob(img.Target.Digest)
		for k, v := range img.Labels {
			processLabel(k, v)
		}
	}

	for len(blobQueue) > 0 {
		dgst := blobQueue[0]
		blobQueue = blobQueue[1:]

		if labels, ok := secBlobs[dgst]; ok {
			for k, v := range labels {
				processLabel(k, v)
			}
		}
		if bbkt := getBlobBucket(tx, ns, dgst); bbkt != nil {
			if lbkt := bbkt.Bucket(bucketKeyObjectLabels); lbkt != nil {
				_ = lbkt.ForEach(func(k, v []byte) error {
					processLabel(string(k), string(v))
					return nil
				})
			}
		}
	}

	return refBlobs, refSnaps
}

func readSecondaryMetaDB(ctx context.Context, dbPath string) (map[string]*secondaryNamespaceData, error) {
	st, err := os.Stat(dbPath)
	if err != nil {
		return nil, err
	}
	if st.IsDir() || st.Size() == 0 {
		return map[string]*secondaryNamespaceData{}, nil
	}
	opts := *bolt.DefaultOptions
	opts.ReadOnly = true
	opts.NoStatistics = true
	opts.Timeout = time.Second
	db, err := bolt.Open(dbPath, 0600, &opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result := map[string]*secondaryNamespaceData{}
	err = db.View(func(tx *bolt.Tx) error {
		v1bkt := tx.Bucket(bucketKeyVersion)
		if v1bkt == nil {
			return nil
		}

		return v1bkt.ForEach(func(nsKey, nsVal []byte) error {
			if nsVal != nil {
				return nil
			}
			ns := string(nsKey)
			nbkt := v1bkt.Bucket(nsKey)
			if nbkt == nil {
				return nil
			}

			blobDigests := map[digest.Digest]struct{}{}
			nsData := &secondaryNamespaceData{
				labels:      map[string]string{},
				blobDigests: blobDigests,
				snapshots:   map[string][]secondarySnapshotEntry{},
			}

			if lbkt := nbkt.Bucket(bucketKeyObjectLabels); lbkt != nil {
				_ = lbkt.ForEach(func(k, v []byte) error {
					if v != nil {
						nsData.labels[string(k)] = string(v)
					}
					return nil
				})
			}

			if cbkt := nbkt.Bucket(bucketKeyObjectContent); cbkt != nil {
				if bbkt := cbkt.Bucket(bucketKeyObjectBlob); bbkt != nil {
					_ = bbkt.ForEach(func(k, v []byte) error {
						if v != nil {
							return nil
						}
						dgst, err := digest.Parse(string(k))
						if err != nil {
							return nil
						}
						b := bbkt.Bucket(k)
						if b == nil {
							return nil
						}
						info := content.Info{Digest: dgst}
						if err := readInfo(&info, b); err != nil {
							log.G(ctx).WithError(err).WithField("db", dbPath).WithField("digest", dgst).Warn("failed to read blob info from secondary root")
							return nil
						}
						blobDigests[dgst] = struct{}{}
						nsData.blobs = append(nsData.blobs, secondaryBlobEntry{info: info})
						return nil
					})
				}
			}

			if sbkt := nbkt.Bucket(bucketKeyObjectSnapshots); sbkt != nil {
				_ = sbkt.ForEach(func(snKey, snVal []byte) error {
					if snVal != nil {
						return nil
					}
					snName := string(snKey)
					ssbkt := sbkt.Bucket(snKey)
					if ssbkt == nil {
						return nil
					}
					var entries []secondarySnapshotEntry
					_ = ssbkt.ForEach(func(k, v []byte) error {
						if v != nil {
							return nil
						}
						b := ssbkt.Bucket(k)
						if b == nil {
							return nil
						}
						entry := secondarySnapshotEntry{
							key:        string(k),
							backendKey: string(b.Get(bucketKeyName)),
							parent:     string(b.Get(bucketKeyParent)),
							info: snapshots.Info{
								Name:   string(k),
								Parent: string(b.Get(bucketKeyParent)),
							},
						}
						if err := boltutil.ReadTimestamps(b, &entry.info.Created, &entry.info.Updated); err != nil {
							log.G(ctx).WithError(err).WithField("db", dbPath).WithField("key", entry.key).Warn("failed to read snapshot timestamps from secondary root")
							return nil
						}
						labels, err := boltutil.ReadLabels(b)
						if err != nil {
							log.G(ctx).WithError(err).WithField("db", dbPath).WithField("key", entry.key).Warn("failed to read snapshot labels from secondary root")
							return nil
						}
						entry.info.Labels = labels
						entries = append(entries, entry)
						return nil
					})
					nsData.snapshots[snName] = sortSnapshotsTopologically(entries)
					return nil
				})
			}

			if ibkt := nbkt.Bucket(bucketKeyObjectImages); ibkt != nil {
				_ = ibkt.ForEach(func(k, v []byte) error {
					if v != nil {
						return nil
					}
					b := ibkt.Bucket(k)
					if b == nil {
						return nil
					}
					img := images.Image{Name: string(k)}
					if err := readImage(&img, b); err != nil {
						log.G(ctx).WithError(err).WithField("db", dbPath).WithField("image", img.Name).Warn("failed to read image from secondary root")
						return nil
					}
					nsData.images = append(nsData.images, secondaryImageEntry{
						image: img,
					})
					return nil
				})
			}

			result[ns] = nsData
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func sortSnapshotsTopologically(entries []secondarySnapshotEntry) []secondarySnapshotEntry {
	byKey := make(map[string]secondarySnapshotEntry, len(entries))
	for _, e := range entries {
		byKey[e.key] = e
	}
	visited := make(map[string]bool, len(entries))
	sorted := make([]secondarySnapshotEntry, 0, len(entries))

	var visit func(key string)
	visit = func(key string) {
		if visited[key] {
			return
		}
		visited[key] = true
		e, ok := byKey[key]
		if !ok {
			return
		}
		if e.parent != "" {
			visit(e.parent)
		}
		sorted = append(sorted, e)
	}

	keys := make([]string, 0, len(entries))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		visit(k)
	}
	return sorted
}

func (m *DB) importSecondaryRoot(ctx context.Context, sr resolvedSecondaryRoot, nsMap map[string]*secondaryNamespaceData, activePriority map[string]int) error {
	secBackendSnaps := make(map[string]map[string]storage.SecondarySnapshot)
	for snName, snRoot := range sr.snRoots {
		snDBPath := filepath.Join(snRoot, "metadata.db")
		if st, err := os.Stat(snDBPath); err == nil && !st.IsDir() && st.Size() > 0 {
			snaps, err := storage.ReadSecondarySnapshots(snDBPath)
			if err != nil {
				log.G(ctx).WithError(err).WithField("secondary_root", sr.metaRoot).WithField("snapshotter", snName).WithField("path", snDBPath).Warn("failed to read secondary root snapshotter metadata")
			} else {
				secBackendSnaps[snName] = snaps
			}
		}
	}

	var pendingRemovals []pendingSnapRemove
	var pendingImports []pendingSnapImport

	err := m.db.Update(func(tx *bolt.Tx) error {
		for ns, nsData := range nsMap {
			if len(nsData.labels) > 0 {
				_ = withNamespacesLabelsBucket(tx, ns, func(lbkt *bolt.Bucket) error {
					for k, v := range nsData.labels {
						if lbkt.Get([]byte(k)) == nil {
							_ = lbkt.Put([]byte(k), []byte(v))
						}
					}
					return nil
				})
			}

			// 1. Import content blobs
			for _, blobEntry := range nsData.blobs {
				dgst := blobEntry.info.Digest
				if isContentTombstoned(tx, ns, dgst) {
					continue
				}
				if m.cs != nil && m.cs.Store != nil {
					if checker, ok := m.cs.Store.(secondaryRootBlobChecker); ok {
						if !checker.HasBlobInSecondaryRoot(sr.index, dgst) {
							log.G(ctx).WithField("secondary_root", sr.metaRoot).WithField("digest", dgst).Warn("skipping secondary root blob with missing backing file")
							continue
						}
					} else if _, err := m.cs.Store.Info(ctx, dgst); err != nil {
						log.G(ctx).WithField("secondary_root", sr.metaRoot).WithField("digest", dgst).Warn("skipping secondary root blob with missing backing file")
						continue
					}
				}
				existing := getBlobBucket(tx, ns, dgst)
				if existing != nil {
					existingPri := priorityOf(string(existing.Get(bucketKeySourceRoot)), activePriority)
					if existingPri <= sr.index {
						_ = markFromSecondary(existing)
						continue
					}
					if blobsBkt := getBlobsBucket(tx, ns); blobsBkt != nil {
						_ = blobsBkt.DeleteBucket([]byte(dgst.String()))
					}
				}
				bbkt, err := createBlobBucket(tx, ns, dgst)
				if err != nil {
					return err
				}
				if err := writeInfo(&blobEntry.info, bbkt); err != nil {
					return err
				}
				if err := bbkt.Put(bucketKeySourceRoot, []byte(sr.metaRoot)); err != nil {
					return err
				}
				if err := markFromSecondary(bbkt); err != nil {
					return err
				}
			}

			// 2. Import snapshots in topological order (parents before children)
			for snName, snapEntries := range nsData.snapshots {
				sn, ok := m.ss[snName]
				if !ok {
					continue
				}
				srs, ok := sn.Snapshotter.(secondaryRootSnapshotter)
				if !ok {
					continue
				}
				snRoot := sr.snRoots[snName]
				if snRoot == "" {
					continue
				}
				backendSnaps := secBackendSnaps[snName]
				if len(backendSnaps) == 0 {
					continue
				}

				bkt, err := createSnapshotterBucket(tx, ns, snName)
				if err != nil {
					return err
				}

				for _, snapEntry := range snapEntries {
					if isSnapshotTombstoned(tx, ns, snName, snapEntry.key) {
						continue
					}
					backendSnap, ok := backendSnaps[snapEntry.backendKey]
					if !ok || backendSnap.Info.Kind != snapshots.KindCommitted {
						log.G(ctx).WithField("secondary_root", sr.metaRoot).WithField("snapshotter", snName).WithField("key", snapEntry.key).Warn("skipping secondary root snapshot missing from snapshotter metadata")
						continue
					}
					if !srs.SnapshotDirExists(snRoot, backendSnap.ID) {
						log.G(ctx).WithField("secondary_root", sr.metaRoot).WithField("snapshotter", snName).WithField("key", snapEntry.key).Warn("skipping secondary root snapshot with missing backing directory")
						continue
					}

					if existing := bkt.Bucket([]byte(snapEntry.key)); existing != nil {
						existingBKey := string(existing.Get(bucketKeyName))
						existingPri := priorityOf(string(existing.Get(bucketKeySourceRoot)), activePriority)
						if existingPri == -1 {
							_ = markFromSecondary(existing)
							continue
						}
						if existingPri <= sr.index {
							if _, statErr := sn.Snapshotter.Stat(ctx, existingBKey); statErr == nil {
								_ = markFromSecondary(existing)
								continue
							}
						} else if hasActiveHigherPriorityDescendant(bkt, snapEntry.key, sr.index, activePriority) {
							_ = markFromSecondary(existing)
							continue
						}
						rems, _ := sn.removeSnapshotTree(ctx, tx, ns, bkt, snapEntry.key, snapEntry.key)
						pendingRemovals = append(pendingRemovals, rems...)
					}

					var bparent string
					if snapEntry.parent != "" {
						pbkt := bkt.Bucket([]byte(snapEntry.parent))
						if pbkt == nil {
							continue
						}
						bparent = string(pbkt.Get(bucketKeyName))
						if bparent == "" {
							continue
						}
					}

					sid, err := bkt.NextSequence()
					if err != nil {
						return err
					}
					bkey := createKey(sid, ns, snapEntry.key)

					bInfo := backendSnap.Info
					bInfo.Name = bkey
					bInfo.Parent = bparent
					bInfo.Kind = snapshots.KindCommitted
					pendingImports = append(pendingImports, pendingSnapImport{
						srs:    srs,
						bkey:   bkey,
						info:   bInfo,
						usage:  backendSnap.Usage,
						snRoot: snRoot,
						id:     backendSnap.ID,
					})

					sbkt, err := bkt.CreateBucket([]byte(snapEntry.key))
					if err != nil {
						return err
					}
					if snapEntry.parent != "" {
						pbkt := bkt.Bucket([]byte(snapEntry.parent))
						cbkt, err := pbkt.CreateBucketIfNotExists(bucketKeyChildren)
						if err != nil {
							return err
						}
						if err := cbkt.Put([]byte(snapEntry.key), nil); err != nil {
							return err
						}
						if err := sbkt.Put(bucketKeyParent, []byte(snapEntry.parent)); err != nil {
							return err
						}
					}
					if err := boltutil.WriteTimestamps(sbkt, snapEntry.info.Created, snapEntry.info.Updated); err != nil {
						return err
					}
					if err := boltutil.WriteLabels(sbkt, snapEntry.info.Labels); err != nil {
						return err
					}
					if err := sbkt.Put(bucketKeyName, []byte(bkey)); err != nil {
						return err
					}
					if err := sbkt.Put(bucketKeySourceRoot, []byte(sr.metaRoot)); err != nil {
						return err
					}
					if err := markFromSecondary(sbkt); err != nil {
						return err
					}
				}
			}

			// 3. Import images
			if len(nsData.images) > 0 {
				bkt, err := createImagesBucket(tx, ns)
				if err != nil {
					return err
				}
				for _, imgEntry := range nsData.images {
					img := imgEntry.image
					if isImageTombstoned(tx, ns, img.Name) {
						continue
					}
					if img.Target.Digest == "" || !m.isImageTreeAvailable(ctx, tx, ns, img.Target.Digest, false, nsData.blobDigests) {
						log.G(ctx).WithField("secondary_root", sr.metaRoot).WithField("image", img.Name).Warn("skipping secondary root image with incomplete content or snapshot tree")
						continue
					}
					if existing := bkt.Bucket([]byte(img.Name)); existing != nil {
						existingPri := priorityOf(string(existing.Get(bucketKeySourceRoot)), activePriority)
						if existingPri <= sr.index {
							_ = markFromSecondary(existing)
							continue
						}
						_ = bkt.DeleteBucket([]byte(img.Name))
					}
					ibkt, err := bkt.CreateBucket([]byte(img.Name))
					if err != nil {
						return err
					}
					if err := writeImage(ibkt, &img); err != nil {
						return err
					}
					if err := ibkt.Put(bucketKeySourceRoot, []byte(sr.metaRoot)); err != nil {
						return err
					}
					if err := markFromSecondary(ibkt); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, r := range pendingRemovals {
		r.execute(ctx)
	}
	var srsOrder []secondaryRootSnapshotter
	batches := make(map[secondaryRootSnapshotter][]storage.CommittedSnapshotImport)
	for _, imp := range pendingImports {
		if _, exists := batches[imp.srs]; !exists {
			srsOrder = append(srsOrder, imp.srs)
		}
		batches[imp.srs] = append(batches[imp.srs], storage.CommittedSnapshotImport{
			Key:        imp.bkey,
			Info:       imp.info,
			Usage:      imp.usage,
			SourceRoot: imp.snRoot,
			SourceID:   imp.id,
		})
	}
	rollbackFailedImport := func() {
		rollbackPriority := make(map[string]int, len(activePriority))
		for k, v := range activePriority {
			if k != sr.metaRoot {
				rollbackPriority[k] = v
			}
		}
		_ = m.pruneDetachedSecondaryRoots(ctx, rollbackPriority)
	}
	for _, srs := range srsOrder {
		batch := batches[srs]
		if bsrs, ok := srs.(batchSecondaryRootSnapshotter); ok {
			if err := bsrs.ImportCommittedSnapshots(ctx, batch); err != nil {
				rollbackFailedImport()
				return err
			}
			continue
		}
		for _, item := range batch {
			if err := srs.ImportCommittedSnapshot(ctx, item.Key, item.Info, item.Usage, item.SourceRoot, item.SourceID); err != nil {
				rollbackFailedImport()
				return err
			}
		}
	}
	return nil
}
