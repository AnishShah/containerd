//go:build linux

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

package overlay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/containerd/v2/internal/userns"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay/overlayutils"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
)

// upperdirKey is a key of an optional label to each snapshot.
// This optional label of a snapshot contains the location of "upperdir" where
// the change set between this snapshot and its parent is stored.
const upperdirKey = "containerd.io/snapshot/overlay.upperdir"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	asyncRemove    bool
	upperdirLabel  bool
	ms             MetaStore
	mountOptions   []string
	remapIDs       bool
	slowChown      bool
	secondaryRoots []string
}

// Opt is an option to configure the overlay snapshotter
type Opt func(config *SnapshotterConfig) error

// AsynchronousRemove defers removal of filesystem content until
// the Cleanup method is called. Removals will make the snapshot
// referred to by the key unavailable and make the key immediately
// available for re-use.
func AsynchronousRemove(config *SnapshotterConfig) error {
	config.asyncRemove = true
	return nil
}

// WithUpperdirLabel adds as an optional label
// "containerd.io/snapshot/overlay.upperdir". This stores the location
// of the upperdir that contains the changeset between the labelled
// snapshot and its parent.
func WithUpperdirLabel(config *SnapshotterConfig) error {
	config.upperdirLabel = true
	return nil
}

// WithMountOptions defines the default mount options used for the overlay mount.
// NOTE: Options are not applied to bind mounts.
func WithMountOptions(options []string) Opt {
	return func(config *SnapshotterConfig) error {
		config.mountOptions = append(config.mountOptions, options...)
		return nil
	}
}

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

type MetaStore interface {
	TransactionContext(ctx context.Context, writable bool) (context.Context, storage.Transactor, error)
	WithTransaction(ctx context.Context, writable bool, fn storage.TransactionCallback) error
	Close() error
}

// WithMetaStore allows the MetaStore to be created outside the snapshotter
// and passed in.
func WithMetaStore(ms MetaStore) Opt {
	return func(config *SnapshotterConfig) error {
		config.ms = ms
		return nil
	}
}

func WithRemapIDs(config *SnapshotterConfig) error {
	config.remapIDs = true
	return nil
}

func WithSlowChown(config *SnapshotterConfig) error {
	config.slowChown = true
	return nil
}

type snapshotter struct {
	root           string
	secondaryRoots []string
	ms             MetaStore
	asyncRemove    bool
	upperdirLabel  bool
	options        []string
	remapIDs       bool
	slowChown      bool
}

// NewSnapshotter returns a Snapshotter which uses overlayfs. The overlayfs
// diffs are stored under the provided root. A metadata file is stored under
// the root.
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
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	if config.ms == nil {
		config.ms, err = storage.NewMetaStore(filepath.Join(root, "metadata.db"))
		if err != nil {
			return nil, err
		}
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if !hasOption(config.mountOptions, "userxattr") {
		// figure out whether "userxattr" option is recognized by the kernel && needed
		userxattr, err := overlayutils.NeedsUserXAttr(root)
		if err != nil {
			log.L.WithError(err).Warnf("cannot detect whether \"userxattr\" option needs to be used, assuming to be %v", userxattr)
		}
		if userxattr {
			config.mountOptions = append(config.mountOptions, "userxattr")
		}
	}

	// Mount options are last-wins in the kernel, so appending "index=off"
	// after a configured "index=on" would silently override it.
	if !hasOption(config.mountOptions, "index") && supportsIndex() {
		config.mountOptions = append(config.mountOptions, "index=off")
	}

	return &snapshotter{
		root:           root,
		secondaryRoots: config.secondaryRoots,
		ms:             config.ms,
		asyncRemove:    config.asyncRemove,
		upperdirLabel:  config.upperdirLabel,
		options:        config.mountOptions,
		remapIDs:       config.remapIDs,
		slowChown:      config.slowChown,
	}, nil
}

// hasOption reports whether the "key" option is present in options, either
// as a bare flag ("userxattr") or in "key=value" form ("index=on").
func hasOption(options []string, key string) bool {
	for _, option := range options {
		if option == key {
			return true
		}
		if optionKey, _, ok := strings.Cut(option, "="); ok && optionKey == key {
			return true
		}
	}
	return false
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	var id string
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}
		return o.verifyStoragePaths(ctx, key)
	}); err != nil {
		return info, err
	}

	if o.upperdirLabel {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		info.Labels[upperdirKey] = o.upperPath(id)
	}
	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if err := o.verifyStoragePaths(ctx, info.Name); err != nil {
			return err
		}
		newInfo, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		if err != nil {
			return err
		}

		if o.upperdirLabel {
			id, _, _, err := storage.GetInfo(ctx, newInfo.Name)
			if err != nil {
				return err
			}
			if newInfo.Labels == nil {
				newInfo.Labels = make(map[string]string)
			}
			newInfo.Labels[upperdirKey] = o.upperPath(id)
		}
		return nil
	})
	return newInfo, err
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
	var (
		usage snapshots.Usage
		info  snapshots.Info
		id    string
	)
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, usage, err = storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}
		return o.verifyStoragePaths(ctx, key)
	}); err != nil {
		return usage, err
	}

	if info.Kind == snapshots.KindActive {
		upperPath := o.upperPath(id)
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
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
	var info snapshots.Info
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot info: %w", err)
		}
		return o.verifyStoragePaths(ctx, key)
	}); err != nil {
		return nil, err
	}
	return o.mounts(s, info), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// grab the existing id
		id, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}

		usage, err := fs.DiskUsage(ctx, o.upperPath(id))
		if err != nil {
			return err
		}

		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		}
		return nil
	})
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var removals []string
	// Remove directories after the transaction is closed, failures must not
	// return error since the transaction is committed with the removal
	// key no longer available.
	defer func() {
		if err == nil {
			for _, dir := range removals {
				if err := os.RemoveAll(dir); err != nil {
					log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
				}
			}
		}
	}()
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		var id string
		id, _, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", key, err)
		}

		if !isSecondaryStorageID(id) && !o.asyncRemove {
			removals, err = o.getCleanupDirectories(ctx)
			if err != nil {
				return fmt.Errorf("unable to get directories for removal: %w", err)
			}
		}
		return nil
	})
}

// Walk the snapshots.
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
			if o.upperdirLabel {
				id, _, _, err := storage.GetInfo(ctx, info.Name)
				if err != nil {
					return err
				}
				if info.Labels == nil {
					info.Labels = make(map[string]string)
				}
				info.Labels[upperdirKey] = o.upperPath(id)
			}
			return fn(ctx, info)
		}, fs...)
	})
}

// WalkAll walks all snapshots in the metastore without filtering out missing secondary storage paths.
func (o *snapshotter) WalkAll(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
			if o.upperdirLabel {
				id, _, _, err := storage.GetInfo(ctx, info.Name)
				if err != nil {
					return err
				}
				if info.Labels == nil {
					info.Labels = make(map[string]string)
				}
				info.Labels[upperdirKey] = o.upperPath(id)
			}
			return fn(ctx, info)
		}, fs...)
	})
}

// Cleanup cleans up disk resources from removed or abandoned snapshots
func (o *snapshotter) Cleanup(ctx context.Context) error {
	cleanup, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	for _, dir := range cleanup {
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	return nil
}

func (o *snapshotter) cleanupDirectories(ctx context.Context) (_ []string, err error) {
	var cleanupDirs []string
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		cleanupDirs, err = o.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	return cleanupDirs, nil
}

func (o *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		s        storage.Snapshot
		td, path string
		info     snapshots.Info
	)

	defer func() {
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
				}
			}
		}
	}()

	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		if parent != "" {
			if err := o.verifyStoragePaths(ctx, parent); err != nil {
				return fmt.Errorf("failed to verify parent snapshot: %w", err)
			}
		}

		snapshotDir := filepath.Join(o.root, "snapshots")
		td, err = o.prepareDirectory(ctx, snapshotDir, kind)
		if err != nil {
			return fmt.Errorf("failed to create prepare snapshot dir: %w", err)
		}

		s, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get snapshot info: %w", err)
		}

		var (
			mappedUID, mappedGID     = -1, -1
			uidmapLabel, gidmapLabel string
			needsRemap               = false
		)
		// NOTE: if idmapped mounts' supported by hosted kernel there may be
		// no parents at all, so overlayfs will not work and snapshotter
		// will use bind mount. To be able to create file objects inside the
		// rootfs -- just chown this only bound directory according to provided
		// {uid,gid}map. In case of one/multiple parents -- chown upperdir.
		if v, ok := info.Labels[snapshots.LabelSnapshotUIDMapping]; ok {
			uidmapLabel = v
			needsRemap = true
		}
		if v, ok := info.Labels[snapshots.LabelSnapshotGIDMapping]; ok {
			gidmapLabel = v
			needsRemap = true
		}

		if needsRemap {
			var idMap userns.IDMap
			if err = idMap.Unmarshal(uidmapLabel, gidmapLabel); err != nil {
				return fmt.Errorf("failed to unmarshal snapshot ID mapped labels: %w", err)
			}
			root, err := idMap.RootPair()
			if err != nil {
				return fmt.Errorf("failed to find root pair: %w", err)
			}
			mappedUID, mappedGID = int(root.Uid), int(root.Gid)
		}

		if mappedUID == -1 || mappedGID == -1 {
			if len(s.ParentIDs) > 0 {
				st, err := os.Stat(o.upperPath(s.ParentIDs[0]))
				if err != nil {
					return fmt.Errorf("failed to stat parent: %w", err)
				}
				stat, ok := st.Sys().(*syscall.Stat_t)
				if !ok {
					return errors.New("incompatible types after stat call: *syscall.Stat_t expected")
				}
				mappedUID = int(stat.Uid)
				mappedGID = int(stat.Gid)
			}
		}

		if mappedUID != -1 && mappedGID != -1 {
			if err := os.Lchown(filepath.Join(td, "fs"), mappedUID, mappedGID); err != nil {
				return fmt.Errorf("failed to chown: %w", err)
			}
		}

		path = filepath.Join(snapshotDir, s.ID)
		if err = os.Rename(td, path); err != nil {
			return fmt.Errorf("failed to rename: %w", err)
		}
		td = ""

		return nil
	}); err != nil {
		return nil, err
	}
	return o.mounts(s, info), nil
}

func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}

	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
			return td, err
		}
	}

	return td, nil
}

func (o *snapshotter) mounts(s storage.Snapshot, info snapshots.Info) []mount.Mount {
	var options []string

	if o.remapIDs {
		if v, ok := info.Labels[snapshots.LabelSnapshotUIDMapping]; ok {
			options = append(options, "uidmap="+v)
		}
		if v, ok := info.Labels[snapshots.LabelSnapshotGIDMapping]; ok {
			options = append(options, "gidmap="+v)
		}
	}

	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}
		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: append(options,
					roFlag,
					"rbind",
				),
			},
		}
	}

	if s.Kind == snapshots.KindActive {
		options = append(options,
			"workdir="+o.workPath(s.ID),
			"upperdir="+o.upperPath(s.ID),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: append(options,
					"ro",
					"rbind",
				),
			},
		}
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(s.ParentIDs[i])
	}
	options = append(options, "lowerdir="+strings.Join(parentPaths, ":"))
	options = append(options, o.options...)

	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}
}

func isSecondaryStorageID(id string) bool {
	return filepath.IsAbs(id) || strings.ContainsRune(id, filepath.Separator)
}

func (o *snapshotter) upperPath(id string) string {
	if isSecondaryStorageID(id) {
		return filepath.Join(id, "fs")
	}
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	if isSecondaryStorageID(id) {
		return filepath.Join(id, "work")
	}
	return filepath.Join(o.root, "snapshots", id, "work")
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
				_, statErr = os.Stat(o.upperPath(p))
				statCache[p] = statErr
			}
		} else {
			_, statErr = os.Stat(o.upperPath(p))
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
	_, err := os.Stat(filepath.Join(sourceRoot, "snapshots", sourceID, "fs"))
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
func (o *snapshotter) RemoveMetadata(ctx context.Context, key string) error {
	var removedPrimaryDirs []string
	err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		removedIDs, err := storage.RemoveHierarchy(ctx, key)
		if err != nil || o.asyncRemove {
			return err
		}
		for _, id := range removedIDs {
			removedPrimaryDirs = append(removedPrimaryDirs, filepath.Join(o.root, "snapshots", id))
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, dir := range removedPrimaryDirs {
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}
	return nil
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}

// supportsIndex checks whether the "index=off" option is supported by the kernel.
func supportsIndex() bool {
	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		return true
	}
	return false
}
