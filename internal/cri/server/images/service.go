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

package images

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/transfer"
	criconfig "github.com/containerd/containerd/v2/internal/cri/config"
	imagestore "github.com/containerd/containerd/v2/internal/cri/store/image"
	snapshotstore "github.com/containerd/containerd/v2/internal/cri/store/snapshot"
	"github.com/containerd/containerd/v2/internal/cri/util"
	"github.com/containerd/containerd/v2/internal/kmutex"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/platforms"
	"golang.org/x/sync/semaphore"

	docker "github.com/distribution/reference"
	imagedigest "github.com/opencontainers/go-digest"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type imageClient interface {
	ListImages(context.Context, ...string) ([]containerd.Image, error)
	GetImage(context.Context, string) (containerd.Image, error)
	Pull(context.Context, string, ...containerd.RemoteOpt) (containerd.Image, error)
}

type ImagePlatform struct {
	Snapshotter string
	Platform    imagespec.Platform
}

type CRIImageService struct {
	runtime.UnimplementedImageServiceServer

	// config contains all image configurations.
	config criconfig.ImageConfig
	// content is the lower level content store.
	content content.Store
	// images is the lower level image store used for raw storage,
	// no event publishing should currently be assumed
	images images.Store
	// client is a subset of the containerd client
	// and will be replaced by image store and transfer service
	client imageClient
	// imageFSPaths contains path to image filesystem for snapshotters.
	imageFSPaths map[string]string
	// runtimePlatformsMu protects runtimePlatforms.
	runtimePlatformsMu sync.RWMutex
	// runtimePlatforms are the platforms configured for a runtime.
	runtimePlatforms map[string]ImagePlatform
	// imageStore stores all resources associated with images.
	imageStore *imagestore.Store
	// snapshotStore stores information of all snapshots.
	snapshotStore *snapshotstore.Store
	// snapshotters stores the backend snapshotters by name.
	snapshotters map[string]snapshots.Snapshotter
	// allSnapshotters stores all available snapshotter plugins by name.
	allSnapshotters map[string]snapshots.Snapshotter
	// transferrer is used to pull image with transfer service
	transferrer transfer.Transferrer
	// unpackDuplicationSuppressor is used to make sure that there is only
	// one in-flight fetch request or unpack handler for a given descriptor's
	// or chain ID.
	unpackDuplicationSuppressor kmutex.KeyedLocker

	// downloadLimiter is used to limit the number of concurrent downloads.
	downloadLimiter *semaphore.Weighted
}

type GRPCCRIImageService struct {
	*CRIImageService
}

type CRIImageServiceOptions struct {
	Content content.Store

	Images images.Store

	ImageFSPaths map[string]string

	RuntimePlatforms map[string]ImagePlatform

	Snapshotters map[string]snapshots.Snapshotter

	AllSnapshotters map[string]snapshots.Snapshotter

	Client imageClient

	Transferrer transfer.Transferrer
}

// NewService creates a new CRI Image Service
//
// TODO:
//  1. Generalize the image service and merge with a single higher level image service
//  2. Update the options to remove client and imageFSPath
//     - Platform configuration with Array/Map of snapshotter names + filesystem ID + platform matcher + runtime to snapshotter
//     - Transfer service implementation
//     - Image Service (from metadata)
//     - Content store (from metadata)
//  3. Separate image cache and snapshot cache to first class plugins, make the snapshot cache much more efficient and intelligent
func NewService(config criconfig.ImageConfig, options *CRIImageServiceOptions) (*CRIImageService, error) {
	var downloadLimiter *semaphore.Weighted
	if config.MaxConcurrentDownloads > 0 {
		downloadLimiter = semaphore.NewWeighted(int64(config.MaxConcurrentDownloads))
	}
	svc := CRIImageService{
		config:                      config,
		content:                     options.Content,
		images:                      options.Images,
		client:                      options.Client,
		imageStore:                  imagestore.NewStore(options.Images, options.Content, platforms.Default()),
		imageFSPaths:                options.ImageFSPaths,
		runtimePlatforms:            options.RuntimePlatforms,
		snapshotStore:               snapshotstore.NewStore(),
		snapshotters:                options.Snapshotters,
		allSnapshotters:             options.AllSnapshotters,
		transferrer:                 options.Transferrer,
		unpackDuplicationSuppressor: kmutex.New(),
		downloadLimiter:             downloadLimiter,
	}

	log.L.Info("Start snapshots syncer")
	snapshotsSyncer := newSnapshotsSyncer(
		svc.snapshotStore,
		options.Snapshotters,
		time.Duration(svc.config.StatsCollectPeriod)*time.Second,
	)
	snapshotsSyncer.start()

	return &svc, nil
}

// UpdateRuntimeSnapshotter adds or updates the snapshotter mapping for a runtime.
// This is called by the main CRI plugin after both image and runtime plugins are initialized,
// to propagate runtime-specific snapshotters configured in the runtime plugin's config.
func (c *CRIImageService) UpdateRuntimeSnapshotter(runtimeName string, imagePlatform ImagePlatform) {
	c.runtimePlatformsMu.Lock()
	defer c.runtimePlatformsMu.Unlock()
	if c.runtimePlatforms == nil {
		c.runtimePlatforms = make(map[string]ImagePlatform)
	}
	// Don't override if already configured
	if _, exists := c.runtimePlatforms[runtimeName]; exists {
		log.L.Debugf("Runtime %q already has snapshotter configured, not overriding", runtimeName)
		return
	}
	c.runtimePlatforms[runtimeName] = imagePlatform
	log.L.Infof("Registered runtime %q with snapshotter %q", runtimeName, imagePlatform.Snapshotter)
}

// LocalResolve resolves image reference locally and returns corresponding image metadata. It
// returns errdefs.ErrNotFound if the reference doesn't exist.
func (c *CRIImageService) LocalResolve(refOrID string) (imagestore.Image, error) {
	var resolvedRef string
	getImageID := func(refOrId string) string {
		if _, err := imagedigest.Parse(refOrId); err == nil {
			return refOrId
		}
		return func(ref string) string {
			// ref is not image id, try to resolve it locally.
			// TODO(random-liu): Handle this error better for debugging.
			normalized, err := docker.ParseDockerRef(ref)
			if err != nil {
				return ""
			}
			resolvedRef = normalized.String()
			id, err := c.imageStore.Resolve(resolvedRef)
			if err != nil {
				return ""
			}
			return id
		}(refOrId)
	}

	imageID := getImageID(refOrID)
	if imageID == "" {
		// Try to treat ref as imageID
		imageID = refOrID
	}
	img, err := c.imageStore.Get(imageID)
	if err != nil {
		return imagestore.Image{}, err
	}
	img, err = c.checkImageChainSnapshot(img)
	if err != nil {
		return imagestore.Image{}, err
	}
	if resolvedRef != "" && !slices.Contains(img.References, resolvedRef) {
		return imagestore.Image{}, errdefs.ErrNotFound
	}
	return img, nil
}

func (c *CRIImageService) hasSecondaryRoots() bool {
	type secondaryRootChecker interface {
		HasSecondaryRoots() bool
	}
	if hsr, ok := c.content.(secondaryRootChecker); ok {
		return hsr.HasSecondaryRoots()
	}
	if hsr, ok := c.images.(secondaryRootChecker); ok {
		return hsr.HasSecondaryRoots()
	}
	hasSR := func(sn snapshots.Snapshotter) bool {
		if sn == nil {
			return false
		}
		if hsr, ok := sn.(secondaryRootChecker); ok && hsr.HasSecondaryRoots() {
			return true
		}
		if srs, ok := sn.(interface{ SecondaryRoots() []string }); ok && len(srs.SecondaryRoots()) > 0 {
			return true
		}
		return false
	}
	for _, sn := range c.snapshotters {
		if hasSR(sn) {
			return true
		}
	}
	if len(c.allSnapshotters) > 0 {
		c.runtimePlatformsMu.RLock()
		defer c.runtimePlatformsMu.RUnlock()
		for _, rp := range c.runtimePlatforms {
			if _, inPrimary := c.snapshotters[rp.Snapshotter]; inPrimary {
				continue
			}
			if hasSR(c.allSnapshotters[rp.Snapshotter]) {
				return true
			}
		}
	}
	return false
}

func (c *CRIImageService) listAvailableCRIImages() ([]*runtime.Image, error) {
	imagesInStore := c.imageStore.List()
	if !c.hasSecondaryRoots() || len(imagesInStore) == 0 {
		out := make([]*runtime.Image, 0, len(imagesInStore))
		for _, image := range imagesInStore {
			out = append(out, toCRIImage(image))
		}
		return out, nil
	}

	var availableRefs map[string]struct{}
	if c.images != nil {
		listed, err := c.images.List(util.NamespacedContext())
		if err != nil {
			return nil, err
		}
		availableRefs = make(map[string]struct{}, len(listed))
		for _, img := range listed {
			availableRefs[img.Name] = struct{}{}
		}
	}

	var out []*runtime.Image
	for _, image := range imagesInStore {
		checked, err := c.checkImageChainSnapshotWithRefs(image, availableRefs)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		out = append(out, toCRIImage(checked))
	}
	return out, nil
}

func (c *CRIImageService) checkImageChainSnapshot(img imagestore.Image) (imagestore.Image, error) {
	return c.checkImageChainSnapshotWithRefs(img, nil)
}

func (c *CRIImageService) checkImageChainSnapshotWithRefs(img imagestore.Image, availableRefs map[string]struct{}) (imagestore.Image, error) {
	if !c.hasSecondaryRoots() {
		return img, nil
	}
	ctx := util.NamespacedContext()
	if c.images != nil && len(img.References) > 0 {
		var (
			validRefs    []string
			staleRefs    []string
			lastNotFound error
		)
		if availableRefs != nil {
			for _, ref := range img.References {
				if _, ok := availableRefs[ref]; ok {
					validRefs = append(validRefs, ref)
				} else {
					staleRefs = append(staleRefs, ref)
					lastNotFound = errdefs.ErrNotFound
				}
			}
		} else if len(img.References) == 1 {
			ref := img.References[0]
			if _, err := c.images.Get(ctx, ref); err != nil {
				if !errdefs.IsNotFound(err) {
					return imagestore.Image{}, err
				}
				staleRefs = append(staleRefs, ref)
				lastNotFound = err
			} else {
				validRefs = append(validRefs, ref)
			}
		} else {
			fs := make([]string, len(img.References))
			for i, ref := range img.References {
				fs[i] = fmt.Sprintf("name==%q", ref)
			}
			listed, err := c.images.List(ctx, fs...)
			if err != nil {
				return imagestore.Image{}, err
			}
			found := make(map[string]struct{}, len(listed))
			for _, li := range listed {
				found[li.Name] = struct{}{}
			}
			for _, ref := range img.References {
				if _, ok := found[ref]; ok {
					validRefs = append(validRefs, ref)
				} else {
					staleRefs = append(staleRefs, ref)
					lastNotFound = errdefs.ErrNotFound
				}
			}
		}
		if len(staleRefs) > 0 {
			// Refresh the in-memory CRI image cache for any reference whose backing
			// secondary-root image became unavailable without emitting an image delete event.
			if c.imageStore != nil {
				for _, ref := range staleRefs {
					_ = c.imageStore.Update(ctx, ref)
				}
			}
			if len(validRefs) == 0 && lastNotFound != nil {
				return imagestore.Image{}, lastNotFound
			}
			img.References = validRefs
		}
	}
	var runtimeSnapshotters []snapshots.Snapshotter
	if len(c.allSnapshotters) > 0 {
		c.runtimePlatformsMu.RLock()
		seenRuntimeSn := make(map[string]struct{}, len(c.runtimePlatforms))
		for _, rp := range c.runtimePlatforms {
			if rp.Snapshotter == "" || rp.Snapshotter == c.config.Snapshotter {
				continue
			}
			if _, inPrimary := c.snapshotters[rp.Snapshotter]; inPrimary {
				continue
			}
			if _, seen := seenRuntimeSn[rp.Snapshotter]; seen {
				continue
			}
			if sn := c.allSnapshotters[rp.Snapshotter]; sn != nil {
				seenRuntimeSn[rp.Snapshotter] = struct{}{}
				runtimeSnapshotters = append(runtimeSnapshotters, sn)
			}
		}
		c.runtimePlatformsMu.RUnlock()
	}
	if (len(c.snapshotters) == 0 && len(runtimeSnapshotters) == 0) || img.ChainID == "" {
		return img, nil
	}
	var lastErr error
	if sn := c.snapshotters[c.config.Snapshotter]; sn != nil {
		_, err := sn.Stat(ctx, img.ChainID)
		if err == nil {
			return img, nil
		}
		if !errdefs.IsNotFound(err) {
			return imagestore.Image{}, err
		}
		lastErr = err
	}
	for name, otherSn := range c.snapshotters {
		if name == c.config.Snapshotter || otherSn == nil {
			continue
		}
		if _, otherErr := otherSn.Stat(ctx, img.ChainID); otherErr == nil {
			return img, nil
		} else if !errdefs.IsNotFound(otherErr) {
			return imagestore.Image{}, otherErr
		} else if lastErr == nil {
			lastErr = otherErr
		}
	}
	for _, otherSn := range runtimeSnapshotters {
		if _, otherErr := otherSn.Stat(ctx, img.ChainID); otherErr == nil {
			return img, nil
		} else if !errdefs.IsNotFound(otherErr) {
			return imagestore.Image{}, otherErr
		} else if lastErr == nil {
			lastErr = otherErr
		}
	}
	if lastErr != nil {
		if c.imageStore != nil && c.images != nil {
			for _, ref := range img.References {
				_ = c.imageStore.Update(ctx, ref)
			}
		}
		return imagestore.Image{}, lastErr
	}
	return img, nil
}

// RuntimeSnapshotter overrides the default snapshotter if Snapshotter is set for this runtime.
// See https://github.com/containerd/containerd/issues/6657
// TODO: Pass in name and get back runtime platform
func (c *CRIImageService) RuntimeSnapshotter(ctx context.Context, ociRuntime criconfig.Runtime) string {
	if ociRuntime.Snapshotter == "" {
		return c.config.Snapshotter
	}

	log.G(ctx).Debugf("Set snapshotter for runtime %s to %s", ociRuntime.Type, ociRuntime.Snapshotter)
	return ociRuntime.Snapshotter
}

// GetImage gets image metadata by image id.
func (c *CRIImageService) GetImage(id string) (imagestore.Image, error) {
	img, err := c.imageStore.Get(id)
	if err != nil {
		return imagestore.Image{}, err
	}
	return c.checkImageChainSnapshot(img)
}

// GetSnapshot returns the snapshot with specified key.
func (c *CRIImageService) GetSnapshot(key, snapshotter string) (snapshotstore.Snapshot, error) {
	snapshotKey := snapshotstore.Key{
		Key:         key,
		Snapshotter: snapshotter,
	}
	return c.snapshotStore.Get(snapshotKey)
}

func (c *CRIImageService) ImageFSPaths() map[string]string {
	return c.imageFSPaths
}

// Config returns the image configuration.
func (c *CRIImageService) Config() criconfig.ImageConfig {
	return c.config
}

// GRPCService returns a new CRI Image Service grpc server.
func (c *CRIImageService) GRPCService() runtime.ImageServiceServer {
	return &GRPCCRIImageService{c}
}
