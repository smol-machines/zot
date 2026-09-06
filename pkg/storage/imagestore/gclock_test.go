package imagestore_test

import (
	"testing"
	"time"

	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	zlog "zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage/cache"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// held reports whether fn finished before the deadline. Used to ask "did this
// lock let me through?" without asserting on wall-clock timings.
func held(fn func()) bool {
	done := make(chan struct{})

	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// A GC pass over one repo took the STORE-WIDE write lock, so collecting any repo
// blocked reads and writes of every other repo for as long as the walk ran. On a
// bucket with thousands of objects that is minutes, and it is what left a
// 374-byte manifest PUT taking 55-123s while GC swept unrelated repositories.
func TestGCLockIsRepoScopedWithoutDedupeCache(t *testing.T) {
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	store := imagestore.NewImageStore(t.TempDir(), "", false, false, log, metrics, nil,
		local.New(true), nil, nil, nil)

	var gcLatency, otherLatency time.Time

	// GC is collecting repo-a.
	store.GCLock("repo-a", &gcLatency)
	defer store.GCUnlock("repo-a", &gcLatency)

	// Work in an unrelated repo must proceed while that pass runs.
	if !held(func() {
		store.RLockRepo("repo-b", &otherLatency)
		store.RUnlockRepo("repo-b", &otherLatency)
	}) {
		t.Fatal("a read of repo-b blocked behind GC of repo-a — GC is still store-wide")
	}

	// The repo under collection is still protected.
	if held(func() {
		var t2 time.Time

		store.LockRepo("repo-a", &t2)
		store.UnlockRepo("repo-a", &t2)
	}) {
		t.Fatal("a write to repo-a proceeded during its own GC pass — the repo is unprotected")
	}
}

// With a dedupe cache, identical blobs are SHARED across repositories, so a
// delete while collecting one repo can strip a blob another still references.
// That case must keep holding the whole store; narrowing it would trade a
// latency bug for a corruption bug.
func TestGCLockStaysStoreWideWithDedupeCache(t *testing.T) {
	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	boltCache, err := cache.NewBoltDBCache(cache.BoltDBDriverParameters{
		RootDir:     rootDir,
		Name:        "cache",
		UseRelPaths: true,
	}, log)
	if err != nil || boltCache == nil {
		t.Skipf("boltdb cache unavailable in this environment: %v", err)
	}

	var dedupeCache storageTypes.Cache = boltCache

	store := imagestore.NewImageStore(rootDir, "", true, false, log, metrics, nil,
		local.New(true), dedupeCache, nil, nil)

	var gcLatency time.Time

	store.GCLock("repo-a", &gcLatency)
	defer store.GCUnlock("repo-a", &gcLatency)

	if held(func() {
		var t2 time.Time

		store.RLockRepo("repo-b", &t2)
		store.RUnlockRepo("repo-b", &t2)
	}) {
		t.Fatal("repo-b proceeded during a dedupe-enabled GC pass — shared blobs are unprotected")
	}
}

// Production shape: remote storage with dedupe OFF. zot still creates a boltdb
// cache in that configuration, so keying the decision on "a cache exists" chose
// the store-wide lock here and left the fix inert exactly where it was needed.
func TestGCLockIsRepoScopedWithCacheButDedupeOff(t *testing.T) {
	rootDir := t.TempDir()
	log := zlog.NewTestLogger()
	metrics := monitoring.NewNopMetricServer()

	boltCache, err := cache.NewBoltDBCache(cache.BoltDBDriverParameters{
		RootDir: rootDir, Name: "cache", UseRelPaths: true,
	}, log)
	if err != nil || boltCache == nil {
		t.Skipf("boltdb cache unavailable: %v", err)
	}

	var c storageTypes.Cache = boltCache

	// dedupe=false, cache present — what CreateCacheDatabaseDriver yields for GCS.
	store := imagestore.NewImageStore(rootDir, "", false, false, log, metrics, nil,
		local.New(true), c, nil, nil)

	var gcLatency time.Time

	store.GCLock("repo-a", &gcLatency)
	defer store.GCUnlock("repo-a", &gcLatency)

	if !held(func() {
		var t2 time.Time

		store.RLockRepo("repo-b", &t2)
		store.RUnlockRepo("repo-b", &t2)
	}) {
		t.Fatal("repo-b blocked behind GC of repo-a with dedupe OFF — the cache alone must not widen the lock")
	}
}
