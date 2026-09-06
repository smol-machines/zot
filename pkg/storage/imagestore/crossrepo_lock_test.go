package imagestore_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/storage"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
	testImage "zotregistry.dev/zot/v2/pkg/test/image-utils"
)

// slowDriver is a real local driver that stalls writes for one repo, standing in
// for the seconds-to-minutes GCS round-trips a remote driver makes in production.
type slowDriver struct {
	storageTypes.Driver

	slowRepo string
	delay    time.Duration
}

func (d *slowDriver) WriteFile(filepath string, content []byte) (int, error) {
	if strings.Contains(filepath, d.slowRepo) {
		time.Sleep(d.delay)
	}

	return d.Driver.WriteFile(filepath, content)
}

// A write to ONE repository must not stall reads of ANOTHER. ImageStore held a
// single store-wide RWMutex: PutImageManifest took it exclusively and held it
// across every storage round-trip, so GetImageManifest — an RLock on that same
// mutex — queued behind a write to an unrelated repo.
//
// Observed in production: a 139s manifest PUT to one tenant's repo and two GETs
// of library/alpine, started seconds apart, all completed in the same instant;
// requests past the load balancer's timeout were served to clients as 502.
func TestWriteToOneRepoDoesNotBlockReadsOfAnother(t *testing.T) {
	const (
		slowRepo = "slow-repo"
		fastRepo = "fast-repo"
		delay    = 2 * time.Second
	)

	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	driver := &slowDriver{Driver: local.New(true), slowRepo: slowRepo, delay: delay}
	store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
		driver, nil, nil, nil)

	ctx := context.Background()

	// Seed the repo we will read from, while nothing else holds the lock.
	fastImage := testImage.CreateDefaultImage()
	if err := testImage.WriteImageToFileSystem(fastImage, fastRepo, "v1",
		storage.StoreController{DefaultStore: store}); err != nil {
		t.Fatalf("seeding %s: %v", fastRepo, err)
	}

	slowImage := testImage.CreateRandomImage()
	for _, l := range slowImage.Layers {
		if _, _, err := store.FullBlobUpload(ctx, slowRepo, strings.NewReader(string(l)),
			""); err != nil {
			t.Logf("seeding layer for %s: %v", slowRepo, err)
		}
	}

	var wg sync.WaitGroup

	wg.Add(1)

	writeStarted := make(chan struct{})

	go func() {
		defer wg.Done()

		close(writeStarted)

		// Stalls for `delay` inside the storage driver, mimicking a slow GCS write.
		_, _, err := store.PutImageManifest(ctx, slowRepo, "v1",
			slowImage.Manifest.MediaType, slowImage.ManifestDescriptor.Data, nil)
		if err != nil {
			t.Logf("write to %s returned %v (the stall, not the result, is what matters)", slowRepo, err)
		}
	}()

	<-writeStarted
	// Let the writer reach the storage driver and be holding the lock.
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	_, _, _, err := store.GetImageManifest(fastRepo, "v1")
	blocked := time.Since(start)

	wg.Wait()

	if err != nil {
		t.Fatalf("reading %s: %v", fastRepo, err)
	}

	// The read touches a different repo and should not wait on the writer at all.
	// Allow generous headroom for slow CI; the failure mode is ~1.7s, not ~10ms.
	if blocked > delay/4 {
		t.Fatalf("read of %s blocked %v behind a write to %s — the store-wide lock is still shared",
			fastRepo, blocked, slowRepo)
	}

	t.Logf("read of %s completed in %v while %s was mid-write", fastRepo, blocked, slowRepo)
}
