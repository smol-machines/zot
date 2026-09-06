package imagestore_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	testImage "zotregistry.dev/zot/v2/pkg/test/image-utils"
)

// Overwriting a MULTI-ARCH tag prunes the old index's constituents, which needs
// the contents of every other image index in the repo — one storage read each.
// A CI repo that overwrites a multi-arch tag per build accumulates thousands of
// indexes, so that walk became 2,400 sequential GCS reads: 55-69s per push, held
// under the repo write lock. A goroutine dump taken mid-push named the path:
//
//	PutImageManifest -> UpdateIndexWithPrunedImageManifests
//	  -> PruneImageManifestsFromIndex -> GetImageIndex -> GetBlobContent (GCS)
//
// Indexes are content-addressed and immutable, so a second overwrite must not
// re-read what the first one already parsed — and the pruned result must be
// identical either way.
func TestOverwritingMultiarchTagDoesNotRereadEveryIndex(t *testing.T) {
	const existingIndexes = 60

	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	counter := &countingDriver{Driver: local.New(true)}
	store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
		counter, nil, nil, nil)
	ctrl := storage.StoreController{DefaultStore: store}

	const repo = "ci-repo"

	// A repo already holding many multi-arch builds, each under its own tag.
	for i := range existingIndexes {
		mi := testImage.CreateRandomMultiarch()
		if err := testImage.WriteMultiArchImageToFileSystem(mi, repo, fmt.Sprintf("build-%d", i), ctrl); err != nil {
			t.Fatalf("seeding multiarch %d: %v", i, err)
		}
	}

	// The tag CI overwrites every build, initially pointing at one index.
	first := testImage.CreateRandomMultiarch()
	if err := testImage.WriteMultiArchImageToFileSystem(first, repo, "latest", ctrl); err != nil {
		t.Fatalf("seeding latest: %v", err)
	}

	tagsBefore := tagList(t, store, repo)

	overwrite := func(label string) int64 {
		next := testImage.CreateRandomMultiarch()
		// Constituent manifests land first, as a real push does; then the index
		// itself replaces the tag, which is the call that walks other indexes.
		for _, img := range next.Images {
			if err := testImage.WriteImageToFileSystem(img, repo, img.DigestStr(), ctrl); err != nil {
				t.Fatalf("%s: writing constituent: %v", label, err)
			}
		}

		counter.reset()

		if _, _, err := store.PutImageManifest(context.Background(), repo, "latest",
			next.IndexDescriptor.MediaType, next.IndexDescriptor.Data, nil); err != nil {
			t.Fatalf("%s: overwrite: %v", label, err)
		}

		return counter.reads.Load()
	}

	coldReads := overwrite("cold")
	warmReads := overwrite("warm")

	t.Logf("indexes in repo=%d  reads on cold overwrite=%d  reads on warm overwrite=%d",
		existingIndexes, coldReads, warmReads)

	// Cold may legitimately read every other index once. Warm must not — the
	// contents are immutable and were just parsed. Allow a handful for the
	// repo's index.json and the new index itself.
	if warmReads > 8 {
		t.Fatalf("warm overwrite re-read %d blobs across %d indexes — the walk is not memoised",
			warmReads, existingIndexes)
	}

	// Correctness: every tagged build survives an overwrite of an unrelated tag.
	tagsAfter := tagList(t, store, repo)
	if len(tagsAfter) != len(tagsBefore) {
		t.Fatalf("tags changed across overwrite: before=%d after=%d", len(tagsBefore), len(tagsAfter))
	}

	for i := range tagsBefore {
		if tagsBefore[i] != tagsAfter[i] {
			t.Fatalf("tag set changed: %v vs %v", tagsBefore, tagsAfter)
		}
	}
}

func tagList(t *testing.T, store interface {
	GetImageTags(string) ([]string, error)
}, repo string,
) []string {
	t.Helper()

	tags, err := store.GetImageTags(repo)
	if err != nil {
		t.Fatalf("tags: %v", err)
	}

	sort.Strings(tags)

	return tags
}
