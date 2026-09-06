package imagestore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	godigest "github.com/opencontainers/go-digest"

	zerr "zotregistry.dev/zot/v2/errors"
	"zotregistry.dev/zot/v2/pkg/api/config"
	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage"
	common "zotregistry.dev/zot/v2/pkg/storage/common"
	"zotregistry.dev/zot/v2/pkg/storage/gc"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	"zotregistry.dev/zot/v2/pkg/test/mocks"
	testImage "zotregistry.dev/zot/v2/pkg/test/image-utils"
)

// A GC pass builds the referenced set by reading every manifest and index in the
// repo from storage, one round-trip each. On a CI repo holding ~2,400 multi-arch
// builds that was ~4,800 serial GCS reads: 4-5 minutes with the repo locked, during
// which every push to it queued. Parses are content-addressed, so a second pass
// must run from memory — and must still collect exactly what the first did.
func TestGCPassDoesNotRereadEveryManifest(t *testing.T) {
	const builds = 40

	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	counter := &countingDriver{Driver: local.New(true)}
	store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
		counter, nil, nil, nil)
	ctrl := storage.StoreController{DefaultStore: store}

	const repo = "ci-repo"

	for i := range builds {
		mi := testImage.CreateRandomMultiarch()
		if err := testImage.WriteMultiArchImageToFileSystem(mi, repo, fmt.Sprintf("build-%d", i), ctrl); err != nil {
			t.Fatalf("seeding build %d: %v", i, err)
		}
	}

	// One untagged image: the thing GC exists to reclaim.
	orphan := testImage.CreateRandomImage()
	if err := testImage.WriteImageToFileSystem(orphan, repo, orphan.DigestStr(), ctrl); err != nil {
		t.Fatalf("seeding orphan: %v", err)
	}

	collector := gc.NewGarbageCollect(store, mocks.MetaDBMock{}, gc.Options{
		Delay:          0,
		ImageRetention: config.ImageRetention{Delay: 0},
	}, nil, log, metrics)

	tagsBefore := tagList(t, store, repo)

	counter.reset()

	if err := collector.CleanRepo(context.Background(), repo); err != nil {
		t.Fatalf("cold gc: %v", err)
	}

	coldReads := counter.reads.Load()

	counter.reset()

	if err := collector.CleanRepo(context.Background(), repo); err != nil {
		t.Fatalf("warm gc: %v", err)
	}

	warmReads := counter.reads.Load()

	t.Logf("builds=%d  cold gc reads=%d  warm gc reads=%d", builds, coldReads, warmReads)

	// Every tagged build must survive both passes untouched.
	tagsAfter := tagList(t, store, repo)
	if len(tagsAfter) != len(tagsBefore) {
		t.Fatalf("gc changed the tag set: before=%d after=%d", len(tagsBefore), len(tagsAfter))
	}

	// The warm pass may read index.json and the odd blob; it must not walk the
	// repo's manifests again. 40 builds × (1 index + 2 platform manifests) = 120
	// parses that are already in memory.
	if warmReads > 12 {
		t.Fatalf("warm gc read %d blobs across %d builds — the walk is not memoised", warmReads, builds)
	}
}

// Memoising a content-addressed parse is safe until the blob is deleted; after
// that a lookup must miss, not serve the stale parse.
func TestDeletedManifestIsNotServedFromCache(t *testing.T) {
	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
		local.New(true), nil, nil, nil)
	ctrl := storage.StoreController{DefaultStore: store}

	const repo = "repo"

	img := testImage.CreateRandomImage()
	if err := testImage.WriteImageToFileSystem(img, repo, "v1", ctrl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dgst := godigest.Digest(img.DigestStr())

	if _, err := common.GetImageManifestCached(store, repo, dgst, log); err != nil {
		t.Fatalf("first read (populates cache): %v", err)
	}

	// Untag, then let GC delete the blob itself.
	if err := store.DeleteImageManifest(context.Background(), repo, "v1", false); err != nil {
		t.Fatalf("untag: %v", err)
	}

	collector := gc.NewGarbageCollect(store, mocks.MetaDBMock{}, gc.Options{
		Delay: 0, ImageRetention: config.ImageRetention{Delay: 0},
	}, nil, log, metrics)

	if err := collector.CleanRepo(context.Background(), repo); err != nil {
		t.Fatalf("gc: %v", err)
	}

	// Give the local driver's delete a moment to be visible.
	time.Sleep(50 * time.Millisecond)

	_, err := common.GetImageManifestCached(store, repo, dgst, log)
	if err == nil {
		t.Fatal("deleted manifest was served from the parse cache")
	}

	if !errors.Is(err, zerr.ErrBlobNotFound) && !errors.Is(err, zerr.ErrManifestNotFound) {
		t.Logf("miss reported as: %v (acceptable: any not-found)", err)
	}
}
