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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	"github.com/containerd/continuity/fs"
)

// SnapshotterConfig is used to configure the native snapshotter instance.
type SnapshotterConfig struct {
	secondaryRoots []string
}

// Opt is an option to configure the native snapshotter.
type Opt func(config *SnapshotterConfig) error

// WithSecondaryRoots configures ordered secondary root directories for preloaded snapshots.
func WithSecondaryRoots(roots []string) Opt {
	return func(config *SnapshotterConfig) error {
		for _, r := range roots {
			if r != "" {
				config.secondaryRoots = append(config.secondaryRoots, filepath.Clean(r))
			}
		}
		return nil
	}
}

type snapshotter struct {
	root           string
	secondaryRoots []string
	ms             *storage.MetaStore
}

// NewSnapshotter returns a Snapshotter which copies layers on the underlying
// file system. A metadata file is stored under the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	var config SnapshotterConfig
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &snapshotter{
		root:           root,
		secondaryRoots: config.secondaryRoots,
		ms:             ms,
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}
		return o.verifyStoragePaths(ctx, key)
	})
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (_ snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if err := o.verifyStoragePaths(ctx, info.Name); err != nil {
			return err
		}
		info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		return err
	})
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Usage(ctx context.Context, key string) (usage snapshots.Usage, err error) {
	var (
		id   string
		info snapshots.Info
	)

	err = o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, usage, err = storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}
		return o.verifyStoragePaths(ctx, key)
	})
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		du, err := fs.DiskUsage(ctx, o.getSnapshotDir(id))
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var s storage.Snapshot
	err = o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot mount: %w", err)
		}

		return o.verifyStoragePaths(ctx, key)
	})
	if err != nil {
		return nil, err
	}

	return o.mounts(s), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		id, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}

		usage, err := fs.DiskUsage(ctx, o.getSnapshotDir(id))
		if err != nil {
			return err
		}

		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot: %w", err)
		}
		return nil
	})
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var (
		renamed, path string
		restore       bool
	)

	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		id, _, err := storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove: %w", err)
		}

		if isSecondaryStorageID(id) {
			return nil
		}

		path = o.getSnapshotDir(id)
		renamed = filepath.Join(o.root, "snapshots", "rm-"+id)
		if err = os.Rename(path, renamed); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to rename: %w", err)
			}
			renamed = ""
		}

		restore = true
		return nil
	})

	if err != nil {
		if renamed != "" && restore {
			if err1 := os.Rename(renamed, path); err1 != nil {
				// May cause inconsistent data on disk
				log.G(ctx).WithError(err1).WithField("path", renamed).Error("failed to rename after failed commit")
			}
		}
		return err
	}
	if renamed != "" {
		if err := os.RemoveAll(renamed); err != nil {
			// Must be cleaned up, any "rm-*" could be removed if no active transactions
			log.G(ctx).WithError(err).WithField("path", renamed).Warnf("failed to remove root filesystem")
		}
	}

	return nil
}

// Walk the committed snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		var statCache, keyCache map[string]error
		if len(o.secondaryRoots) > 0 {
			statCache = make(map[string]error)
			keyCache = make(map[string]error)
		}
		return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
			if err := o.verifyStoragePathsCached(ctx, info.Name, statCache, keyCache); err != nil {
				if errdefs.IsNotFound(err) {
					return nil
				}
				return err
			}
			return fn(ctx, info)
		}, fs...)
	})
}

// WalkAll walks all snapshots in the metastore without filtering out missing secondary storage paths.
func (o *snapshotter) WalkAll(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		path, td string
		s        storage.Snapshot
	)

	if kind == snapshots.KindActive || parent == "" {
		td, err = os.MkdirTemp(filepath.Join(o.root, "snapshots"), "new-")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
		if err := os.Chmod(td, 0755); err != nil {
			return nil, fmt.Errorf("failed to chmod %s to 0755: %w", td, err)
		}
		defer func() {
			if err != nil {
				if td != "" {
					if err1 := os.RemoveAll(td); err1 != nil {
						err = fmt.Errorf("remove failed: %v: %w", err1, err)
					}
				}
				if path != "" {
					if err1 := os.RemoveAll(path); err1 != nil {
						err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
					}
				}
			}
		}()
	}

	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if parent != "" {
			if err := o.verifyStoragePaths(ctx, parent); err != nil {
				return fmt.Errorf("failed to verify parent snapshot: %w", err)
			}
		}

		s, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		if td != "" {
			if len(s.ParentIDs) > 0 {
				parent := o.getSnapshotDir(s.ParentIDs[0])
				xattrErrorHandler := func(dst, src, xattrKey string, copyErr error) error {
					// security.* xattr cannot be copied in most cases (moby/buildkit#1189)
					log.G(ctx).WithError(copyErr).Debugf("failed to copy xattr %q", xattrKey)
					return nil
				}
				copyDirOpts := []fs.CopyDirOpt{
					fs.WithXAttrErrorHandler(xattrErrorHandler),
				}
				if err = fs.CopyDir(td, parent, copyDirOpts...); err != nil {
					return fmt.Errorf("copying of parent failed: %w", err)
				}
			}

			path = o.getSnapshotDir(s.ID)
			if err = os.Rename(td, path); err != nil {
				return fmt.Errorf("failed to rename: %w", err)
			}
			td = ""
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return o.mounts(s), nil
}

func isSecondaryStorageID(id string) bool {
	return filepath.IsAbs(id) || strings.ContainsRune(id, filepath.Separator)
}

func (o *snapshotter) getSnapshotDir(id string) string {
	if isSecondaryStorageID(id) {
		return id
	}
	return filepath.Join(o.root, "snapshots", id)
}

func (o *snapshotter) verifyStoragePaths(ctx context.Context, key string) error {
	return o.verifyStoragePathsCached(ctx, key, nil, nil)
}

func (o *snapshotter) verifyStoragePathsCached(ctx context.Context, key string, statCache, keyCache map[string]error) error {
	if len(o.secondaryRoots) == 0 {
		return nil
	}
	return storage.WalkStoragePaths(ctx, key, keyCache, func(p string) error {
		if !isSecondaryStorageID(p) {
			return nil
		}
		var statErr error
		if statCache != nil {
			var cached bool
			statErr, cached = statCache[p]
			if !cached {
				_, statErr = os.Stat(o.getSnapshotDir(p))
				statCache[p] = statErr
			}
		} else {
			_, statErr = os.Stat(o.getSnapshotDir(p))
		}
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return fmt.Errorf("snapshot %s layer missing on secondary root: %w", key, errdefs.ErrNotFound)
			}
			return statErr
		}
		return nil
	})
}

// SecondaryRoots returns the configured secondary root directories for this snapshotter.
func (o *snapshotter) SecondaryRoots() []string {
	return o.secondaryRoots
}

// SnapshotDirExists reports whether the snapshot directory for sourceID exists under sourceRoot.
func (o *snapshotter) SnapshotDirExists(sourceRoot, sourceID string) bool {
	_, err := os.Stat(filepath.Join(sourceRoot, "snapshots", sourceID))
	return err == nil
}

// ImportCommittedSnapshot records a preloaded committed snapshot from a secondary root in the metastore.
func (o *snapshotter) ImportCommittedSnapshot(ctx context.Context, key string, info snapshots.Info, usage snapshots.Usage, sourceRoot, sourceID string) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		_, err := storage.PutCommittedSnapshot(ctx, key, info, usage, sourceRoot, sourceID)
		return err
	})
}

// ImportCommittedSnapshots records a batch of preloaded committed snapshots from a secondary root in a single metastore transaction.
func (o *snapshotter) ImportCommittedSnapshots(ctx context.Context, imports []storage.CommittedSnapshotImport) error {
	if len(imports) == 0 {
		return nil
	}
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		for _, imp := range imports {
			if _, err := storage.PutCommittedSnapshot(ctx, imp.Key, imp.Info, imp.Usage, imp.SourceRoot, imp.SourceID); err != nil {
				return err
			}
		}
		return nil
	})
}

// RemoveMetadata removes a snapshot entry and any of its descendants from the metastore without deleting secondary backing directories.
func (o *snapshotter) RemoveMetadata(ctx context.Context, key string) (err error) {
	type renamedDir struct {
		orig    string
		renamed string
	}
	var renamedDirs []renamedDir
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		removedIDs, err := storage.RemoveHierarchy(ctx, key)
		if err != nil {
			return err
		}
		for _, id := range removedIDs {
			path := o.getSnapshotDir(id)
			renamed := filepath.Join(o.root, "snapshots", "rm-"+id)
			if err := os.Rename(path, renamed); err != nil {
				if !os.IsNotExist(err) {
					return fmt.Errorf("failed to rename: %w", err)
				}
				continue
			}
			renamedDirs = append(renamedDirs, renamedDir{orig: path, renamed: renamed})
		}
		return nil
	})
	if err != nil {
		for _, d := range renamedDirs {
			if err1 := os.Rename(d.renamed, d.orig); err1 != nil {
				log.G(ctx).WithError(err1).WithField("path", d.renamed).Error("failed to rename after failed commit")
			}
		}
		return err
	}
	for _, d := range renamedDirs {
		if err := os.RemoveAll(d.renamed); err != nil {
			log.G(ctx).WithError(err).WithField("path", d.renamed).Warnf("failed to remove root filesystem")
		}
	}
	return nil
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	var (
		roFlag string
		source string
	)

	if s.Kind == snapshots.KindView {
		roFlag = "ro"
	} else {
		roFlag = "rw"
	}

	if len(s.ParentIDs) == 0 || s.Kind == snapshots.KindActive {
		source = o.getSnapshotDir(s.ID)
	} else {
		source = o.getSnapshotDir(s.ParentIDs[0])
	}

	return []mount.Mount{
		{
			Source:  source,
			Type:    mountType,
			Options: append(defaultMountOptions, roFlag),
		},
	}
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}
