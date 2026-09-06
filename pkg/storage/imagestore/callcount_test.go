package imagestore_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	godigest "github.com/opencontainers/go-digest"
	"github.com/distribution/distribution/v3/registry/storage/driver"

	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
	testImage "zotregistry.dev/zot/v2/pkg/test/image-utils"
)

// countingDriver counts storage round-trips. On a local filesystem each is
// microseconds; on GCS each is a network call of tens of milliseconds, so the
// COUNT — not local wall-clock — is what predicts production latency.
type countingDriver struct {
	storageTypes.Driver

	reads  atomic.Int64
	writes atomic.Int64
	stats  atomic.Int64
}

func (d *countingDriver) ReadFile(path string) ([]byte, error) {
	d.reads.Add(1)

	return d.Driver.ReadFile(path)
}

func (d *countingDriver) WriteFile(path string, content []byte) (int, error) {
	d.writes.Add(1)

	return d.Driver.WriteFile(path, content)
}

func (d *countingDriver) Stat(path string) (driver.FileInfo, error) {
	d.stats.Add(1)

	return d.Driver.Stat(path)
}

func (d *countingDriver) reset() {
	d.reads.Store(0)
	d.writes.Store(0)
	d.stats.Store(0)
}

func (d *countingDriver) total() int64 {
	return d.reads.Load() + d.writes.Load() + d.stats.Load()
}

// How does the per-request storage-call count scale with the number of manifests
// already in a repo? Gini's repo holds 2402, all tagged, and pushes were taking
// 55-123s against GCS. If the count is flat, latency is not manifest-driven; if
// it grows with N, retention is the only thing that bounds it.
func TestStorageCallsScaleWithManifestCount(t *testing.T) {
	for _, n := range []int{10, 50, 200} {
		t.Run(fmt.Sprintf("manifests=%d", n), func(t *testing.T) {
			rootDir := t.TempDir()
			log := zlog.NewTestLogger()
			metrics := monitoring.NewNopMetricServer()

			counter := &countingDriver{Driver: local.New(true)}
			store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
				counter, nil, nil, nil)

			const repo = "bench-repo"

			ctrl := storage.StoreController{DefaultStore: store}

			// Fill the repo with n distinct tagged manifests, as CI does per build.
			var lastDigest godigest.Digest

			for i := range n {
				img := testImage.CreateRandomImage()
				if err := testImage.WriteImageToFileSystem(img, repo, fmt.Sprintf("build-%d", i), ctrl); err != nil {
					t.Fatalf("seeding manifest %d: %v", i, err)
				}

				lastDigest = img.ManifestDescriptor.Digest
			}

			// Measure one read of an existing manifest.
			counter.reset()

			if _, _, _, err := store.GetImageManifest(repo, lastDigest.String()); err != nil {
				t.Fatalf("get manifest: %v", err)
			}

			get := counter.total()

			// Measure one more push into the now-populated repo. Re-tag the image we
			// just wrote: its blobs already exist, so this is a valid manifest PUT —
			// exactly the shape of a CI build tagging a new commit.
			img := testImage.CreateRandomImage()
			if err := testImage.WriteImageToFileSystem(img, repo, "penultimate", ctrl); err != nil {
				t.Fatalf("seeding the image to re-tag: %v", err)
			}

			counter.reset()

			if _, _, err := store.PutImageManifest(context.Background(), repo, "newest",
				img.Manifest.MediaType, img.ManifestDescriptor.Data, nil); err != nil {
				t.Fatalf("put manifest: %v", err)
			}

			put := counter.total()

			t.Logf("N=%-4d  GET storage-calls=%-5d  PUT storage-calls=%-5d", n, get, put)
		})
	}
}
