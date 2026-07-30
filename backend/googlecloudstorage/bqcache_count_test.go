package googlecloudstorage

// These tests guard the property that actually costs money: how many BigQuery
// jobs an access pattern fires. A past bug fired BigQuery millions of times (one
// job per directory during a tree walk), so every realistic pattern here asserts
// an exact BigQuery call count. Row-content correctness lives in bqcache_test.go;
// this file owns call count, exercised through the real listBQ/listBQCached
// orchestration via the injectable bqQuery seam. No real BigQuery, no buckets.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	storage "google.golang.org/api/storage/v1"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
)

// queryCall records what a fired BigQuery query targeted.
type queryCall struct {
	directory string
	recurse   bool
}

// queryRecorder is the fake bqQuery: it counts and records every BigQuery call,
// serves rows from a fixture tree by STARTS_WITH(directory), and can gate the one
// call so the concurrency test forces real contention on bqPopMu.
type queryRecorder struct {
	mu      sync.Mutex
	fixture []bqRow
	calls   []queryCall
	err     error // if set, query returns this instead of serving rows

	gate    bool
	started chan struct{}
	release chan struct{}
}

func (r *queryRecorder) query(ctx context.Context, bucketName, directory string, recurse bool, fn func(bqRow) error) error {
	r.mu.Lock()
	r.calls = append(r.calls, queryCall{directory: directory, recurse: recurse})
	gate := r.gate
	qerr := r.err
	r.mu.Unlock()
	if gate {
		r.started <- struct{}{}
		<-r.release
	}
	if qerr != nil {
		return qerr
	}
	// mimic the inventory query's STARTS_WITH(name, @dir)
	for _, row := range r.fixture {
		if strings.HasPrefix(row.name, directory) {
			if err := fn(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *queryRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *queryRecorder) snapshot() []queryCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queryCall(nil), r.calls...)
}

// newCountFs builds an Fs rooted at bucket "buck" with the fake bqQuery/bqNotFound
// wired in, and (when cacheOn) a fresh bbolt cache in a temp dir.
func newCountFs(t *testing.T, fixture []bqRow, cacheOn bool) (*Fs, *queryRecorder) {
	t.Helper()
	f := &Fs{}
	f.setRoot("buck") // rootBucket="buck", rootDirectory="" => remote root is ""
	f.opt.BigQueryCacheMaxAge = fs.Duration(48 * time.Hour)
	f.bqCtx, f.bqCancel = context.WithCancel(context.Background())
	rec := &queryRecorder{fixture: fixture}
	f.bqQuery = rec.query
	f.bqNotFound = func(ctx context.Context, bucketName, directory string) error {
		return fs.ErrorDirNotFound
	}
	if cacheOn {
		db, err := openBQCache(filepath.Join(t.TempDir(), "c.bolt"))
		if err != nil {
			t.Fatalf("openBQCache: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		f.bqDB = db
	}
	// registered last so it runs first (LIFO): drain any background populate
	// before the cleanups above close the bbolt handle under it
	t.Cleanup(func() {
		f.bqCancel()
		f.bqWG.Wait()
	})
	return f, rec
}

// walkDir simulates one FUSE readdir: a single directory listing at the backend.
func walkDir(f *Fs, dir string, recurse bool) error {
	return f.list(context.Background(), "buck", dir, "", false, recurse,
		func(string, *storage.Object, bool) error { return nil })
}

// fixtureDirs returns every directory in the tree (no trailing slash; "" is root)
// so tests can walk "all N directories" the way a recursive tree-walk would.
func fixtureDirs(rows []bqRow) []string {
	set := map[string]struct{}{"": {}}
	for _, r := range rows {
		for i := 0; i < len(r.name); i++ {
			if r.name[i] == '/' {
				set[r.name[:i]] = struct{}{}
			}
		}
	}
	dirs := make([]string, 0, len(set))
	for d := range set {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	return dirs
}

// ageCache backdates populated_at past any sane max_age, without sleeping.
func ageCache(t *testing.T, f *Fs, bucketName string) {
	t.Helper()
	old := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339)
	if err := f.bqDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bqMetaBucket)).Put(bqTSKey(bucketName), []byte(old))
	}); err != nil {
		t.Fatal(err)
	}
}

// countFixture: a file in root, two dirs with sub-dirs, and a deep chain -
// enough directories that "walk them all" is a meaningful storm test.
var countFixture = []bqRow{
	{name: "a.txt", size: "1"},
	{name: "d1/f1.txt", size: "1"},
	{name: "d1/f2.txt", size: "1"},
	{name: "d1/sub/g1.txt", size: "1"},
	{name: "d2/f3.txt", size: "1"},
	{name: "d2/sub/h1.txt", size: "1"},
	{name: "d3/deep/deeper/x.txt", size: "1"},
}

// assertAllRootRecursive is the scenario-8 invariant: with the cache on, every
// query that fired was a root-level recursive populate - never per-directory.
func assertAllRootRecursive(t *testing.T, f *Fs, rec *queryRecorder) {
	t.Helper()
	for i, c := range rec.snapshot() {
		if !c.recurse || c.directory != f.bqRemoteRoot() {
			t.Errorf("query %d was directory=%q recurse=%v; want root recursive (a per-directory query slipped through)", i, c.directory, c.recurse)
		}
	}
}

// 1. cold cache + recursive root list => exactly 1 query (the populate).
func TestBQCacheCountColdRecursiveRoot(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil {
		t.Fatal(err)
	}
	if n := rec.count(); n != 1 {
		t.Errorf("cold recursive root fired %d queries, want 1", n)
	}
	assertAllRootRecursive(t, f, rec)
}

// 2. cold cache + single-level subdir => 1 query, and it targets ROOT (bootstrap
// is a full-root populate, never a per-directory query).
func TestBQCacheCountColdSingleLevelBootstrapsRoot(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "d1", false); err != nil {
		t.Fatal(err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("cold single-level fired %d queries, want 1", len(calls))
	}
	if calls[0].directory != "" || !calls[0].recurse {
		t.Errorf("bootstrap query was directory=%q recurse=%v; want root recursive", calls[0].directory, calls[0].recurse)
	}
}

// 3. THE regression test: warm cache + walk every directory single-level => 0
// extra queries.
func TestBQCacheCountWarmWalkNoQueries(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1 query
		t.Fatal(err)
	}
	for _, d := range fixtureDirs(countFixture) {
		if err := walkDir(f, d, false); err != nil {
			t.Fatalf("walk %q: %v", d, err)
		}
	}
	if n := rec.count(); n != 1 {
		t.Errorf("warm walk fired %d queries total, want 1 (warm only, 0 during walk)", n)
	}
	assertAllRootRecursive(t, f, rec)
}

// 4. stale cache + walk every directory => exactly 1 repopulate, not N. The
// repopulate now runs in the background (single-flight), so drain it before
// asserting the count.
func TestBQCacheCountStaleWalkRepopulatesOnce(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck")
	for _, d := range fixtureDirs(countFixture) {
		if err := walkDir(f, d, false); err != nil {
			t.Fatalf("walk %q: %v", d, err)
		}
	}
	f.bqWG.Wait()
	if n := rec.count(); n != 2 {
		t.Errorf("stale walk fired %d queries total, want 2 (1 warm + 1 repopulate, not one per dir)", n)
	}
	if gen := readGen(t, f, "buck"); gen != 2 {
		t.Errorf("generation after background repopulate = %d; want 2", gen)
	}
	assertAllRootRecursive(t, f, rec)
}

// 5. concurrent cold reads => exactly 1 query (mutex + double-check). The gate
// holds the first populate in-flight so the others genuinely contend. -race.
func TestBQCacheCountConcurrentColdHerd(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	const M = 8
	rec.gate = true
	rec.started = make(chan struct{}, M)
	rec.release = make(chan struct{})

	var wg sync.WaitGroup
	var errs [M]error // per-index slots: disjoint writes, read after wg.Wait()
	for i := 0; i < M; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = walkDir(f, "d1", false)
		}(i)
	}
	<-rec.started      // one goroutine is inside the populate, holding bqPopMu
	close(rec.release) // release it; the rest re-check and find the cache fresh
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("herd reader %d: %v", i, err)
		}
	}
	if n := rec.count(); n != 1 {
		t.Errorf("concurrent cold herd of %d readers fired %d queries, want 1", M, n)
	}
}

// 6. empty/missing-dir lookups on a warm cache => 0 queries (a stat storm must
// not populate), and they return ErrorDirNotFound.
func TestBQCacheCountMissingDirNoPopulate(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := walkDir(f, "does/not/exist", false); err != fs.ErrorDirNotFound {
			t.Fatalf("missing dir: got %v, want ErrorDirNotFound", err)
		}
	}
	if n := rec.count(); n != 1 {
		t.Errorf("missing-dir lookups fired %d queries, want 1 (warm only)", n)
	}
}

// 7. The cache is now mandatory: NewFs refuses bigquery_table without
// bigquery_cache_db (so the storming uncached path can't even be configured), and
// the check must not fire for configs that don't use bigquery_table.
func TestNewFsRequiresCacheDB(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cfg          configmap.Simple
		wantCacheErr bool
	}{
		{name: "table without cache_db", cfg: configmap.Simple{"bigquery_table": "proj.ds.tbl"}, wantCacheErr: true},
		{name: "no bigquery_table", cfg: configmap.Simple{"anonymous": "true"}, wantCacheErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFs(context.Background(), "gcs", "bucket", tc.cfg)
			// robust either way: for the negative case we only require that the
			// error (if any, from later setup) is not the cache-db one.
			gotCacheErr := err != nil && strings.Contains(err.Error(), "bigquery_cache_db")
			if gotCacheErr != tc.wantCacheErr {
				t.Errorf("NewFs(%v): cache-db error = %v (err=%v); want %v", tc.cfg, gotCacheErr, err, tc.wantCacheErr)
			}
		})
	}
}

// 8 (mechanism): bqQueryRows - the only function that spends BigQuery - refuses
// any query that isn't a root recursive populate, before touching the BigQuery
// client. The guard is unconditional (bigquery_table always runs with the cache),
// so it fires whether or not the bbolt handle is present. (The allow path, root
// recursive, would proceed to the real client; the count tests exercise it via
// the injected fake.)
func TestBQQueryRowsGuardRejectsNonRoot(t *testing.T) {
	noop := func(bqRow) error { return nil }
	for _, cacheOn := range []bool{true, false} {
		f, _ := newCountFs(t, countFixture, cacheOn)
		if err := f.bqQueryRows(context.Background(), "buck", "d1/", false, noop); err == nil {
			t.Errorf("cacheOn=%v: bqQueryRows ran a per-directory query; want error", cacheOn)
		}
		if err := f.bqQueryRows(context.Background(), "buck", "d1/", true, noop); err == nil {
			t.Errorf("cacheOn=%v: bqQueryRows ran a recursive subtree query; want error", cacheOn)
		}
	}
}

// 9. warm cache + recursive subtree list => 0 queries.
func TestBQCacheCountWarmRecursiveSubtree(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1
		t.Fatal(err)
	}
	if err := walkDir(f, "d1", true); err != nil {
		t.Fatal(err)
	}
	if n := rec.count(); n != 1 {
		t.Errorf("warm recursive subtree fired %d queries total, want 1 (warm only)", n)
	}
}

// 10. max_age=0 => never re-queries, even long after the populate.
func TestBQCacheCountMaxAgeZeroNeverRequeries(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	f.opt.BigQueryCacheMaxAge = 0
	if err := walkDir(f, "", true); err != nil { // warm: 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck") // ancient timestamp - must be ignored when max_age=0
	for _, d := range fixtureDirs(countFixture) {
		if err := walkDir(f, d, false); err != nil {
			t.Fatalf("walk %q: %v", d, err)
		}
	}
	if n := rec.count(); n != 1 {
		t.Errorf("max_age=0 walk fired %d queries, want 1 (never stale)", n)
	}
}

// 11a. a recursive root list is TTL-gated like any other list: on a warm cache
// it serves from bbolt and fires NO query (this used to be an unconditional
// repopulate - "rclone sync --fast-list" was re-querying BigQuery every run).
func TestBQCacheCountWarmRootRecursiveNoQuery(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	for i := 0; i < 3; i++ {
		if err := walkDir(f, "", true); err != nil {
			t.Fatal(err)
		}
	}
	if n := rec.count(); n != 1 {
		t.Errorf("3 warm recursive root lists fired %d queries; want 1 (cold populate only)", n)
	}
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Errorf("generation = %d; want 1 (no re-populates)", gen)
	}
}

// 11b. the "refresh" backend command is the explicit force: 1 query each and the
// generation increments even on a fresh cache; failures and zero-row results are
// reported as errors while the old snapshot is kept.
func TestBQCommandRefreshAlwaysRepopulates(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := f.Command(ctx, "refresh", nil, nil); err != nil {
			t.Fatalf("refresh %d: %v", i, err)
		}
		if n := rec.count(); n != i {
			t.Errorf("after %d refreshes, %d queries fired; want %d", i, n, i)
		}
		if gen := readGen(t, f, "buck"); gen != uint64(i) {
			t.Errorf("after %d refreshes, generation = %d; want %d", i, gen, i)
		}
	}

	// a failing query surfaces as an error and keeps the old generation
	rec.err = errors.New("boom")
	if _, err := f.Command(ctx, "refresh", nil, nil); err == nil {
		t.Error("refresh with failing query returned nil error")
	}
	if gen := readGen(t, f, "buck"); gen != 3 {
		t.Errorf("failed refresh changed generation to %d; want 3 (kept)", gen)
	}

	// a zero-row query is also an error for the forced refresh (cron must see it)
	rec.err = nil
	rec.fixture = nil
	if _, err := f.Command(ctx, "refresh", nil, nil); err == nil {
		t.Error("refresh with empty inventory returned nil error")
	}
	if gen := readGen(t, f, "buck"); gen != 3 {
		t.Errorf("empty refresh changed generation to %d; want 3 (kept)", gen)
	}
}

// 12. randomized invariant: on a warm cache, any sequence of single-level and
// recursive-subtree reads fires 0 extra queries (root recursive excluded - that
// is the populate). Fixed seed for determinism.
func TestBQCacheCountRandomWarmInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	fixture := randomFixture(rng)
	f, rec := newCountFs(t, fixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1
		t.Fatal(err)
	}
	dirs := fixtureDirs(fixture)
	for i := 0; i < 300; i++ {
		d := dirs[rng.Intn(len(dirs))]
		recurse := rng.Intn(2) == 0
		if d == "" {
			recurse = false // a recursive root read is the populate, not a "0 extra" case
		}
		if err := walkDir(f, d, recurse); err != nil {
			t.Fatalf("warm read dir=%q recurse=%v: %v", d, recurse, err)
		}
		if n := rec.count(); n != 1 {
			t.Fatalf("warm cache fired %d queries after read %d (dir=%q recurse=%v); want 1", n, i, d, recurse)
		}
	}
}

func randomFixture(rng *rand.Rand) []bqRow {
	n := 20 + rng.Intn(30)
	rows := make([]bqRow, 0, n)
	for i := 0; i < n; i++ {
		depth := 1 + rng.Intn(4)
		parts := make([]string, depth)
		for d := 0; d < depth; d++ {
			parts[d] = fmt.Sprintf("d%d", rng.Intn(4))
		}
		name := strings.Join(parts, "/") + fmt.Sprintf("/f%d.txt", i)
		rows = append(rows, bqRow{name: name, size: "1"})
	}
	return rows
}

// 15. the exact stale-subdir scenario: warm (like --vfs-refresh) → bbolt goes
// stale → a single-level `ls` of a SUBDIR fires one repopulate whose query is at
// the ROOT (@dir=""), never the subdir.
func TestBQCacheCountStaleSubdirRepopulatesFromRoot(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm via recursive root: 1 query
		t.Fatal(err)
	}
	ageCache(t, f, "buck")                              // bbolt now older than max_age
	if err := walkDir(f, "d1/sub", false); err != nil { // single-level ls of a subdir
		t.Fatalf("ls d1/sub: %v", err)
	}
	f.bqWG.Wait() // drain the background repopulate the stale ls kicked
	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("fired %d queries, want 2 (1 warm + 1 stale repopulate)", len(calls))
	}
	if calls[1].directory != "" || !calls[1].recurse {
		t.Errorf("stale repopulate query was directory=%q recurse=%v; want root recursive (@dir=\"\")", calls[1].directory, calls[1].recurse)
	}
}

// 13. a failed BigQuery populate must not flip the generation or corrupt the
// cache: the error propagates, the cache stays cold, and a later populate
// recovers. (Guards against a transient BigQuery failure leaving a half-written
// or wrongly-current snapshot.)
func TestBQCachePopulateErrorDoesNotFlip(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	rec.err = errors.New("boom")
	if err := walkDir(f, "", true); err == nil {
		t.Fatal("populate error was not propagated")
	}
	if gen := readGen(t, f, "buck"); gen != 0 {
		t.Errorf("failed populate left generation %d; want 0 (no flip)", gen)
	}

	// recovery: query succeeds, cache populates and flips normally
	rec.err = nil
	if err := walkDir(f, "", true); err != nil {
		t.Fatalf("recovery populate: %v", err)
	}
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Errorf("generation after recovery = %d; want 1", gen)
	}
	if got := collect(t, f, "buck", "", true); len(got) == 0 {
		t.Error("recovered cache served nothing")
	}
}

// 14. generation-swap is atomic under concurrent reads: while a populate is
// mid-flight writing the next generation, a concurrent read sees the complete
// previous snapshot (never partial/empty), and the new one only after the swap.
// Driven through the forced "refresh" command (a warm recursive root list no
// longer populates). Run under -race.
func TestBQCacheGenerationSwapAtomicUnderConcurrentReads(t *testing.T) {
	f, rec := newCountFs(t, []bqRow{{name: "old.txt", size: "1"}}, true)
	if err := walkDir(f, "", true); err != nil { // populate gen 1 (old.txt)
		t.Fatal(err)
	}

	// the next populate produces gen 2 (new.txt); gate it so it blocks mid-query
	// with gen 1 still current. Set before launching the goroutine (happens-before).
	rec.fixture = []bqRow{{name: "new.txt", size: "1"}}
	rec.gate = true
	rec.started = make(chan struct{}, 1)
	rec.release = make(chan struct{})

	done := make(chan struct{})
	var popErr error
	go func() {
		_, popErr = f.Command(context.Background(), "refresh", nil, nil) // populate gen 2, blocks in the query
		close(done)
	}()
	<-rec.started // gen 2 is being written into its own bucket; gen 1 is still current

	if got := collect(t, f, "buck", "", false); !reflect.DeepEqual(got, []string{"old.txt"}) {
		t.Errorf("read during swap saw %v; want [old.txt] (complete previous snapshot)", got)
	}

	close(rec.release)
	<-done
	if popErr != nil {
		t.Fatalf("gen-2 populate: %v", popErr)
	}

	if got := collect(t, f, "buck", "", false); !reflect.DeepEqual(got, []string{"new.txt"}) {
		t.Errorf("read after swap saw %v; want [new.txt]", got)
	}
}

// 16 (Fix A): a stale cache whose repopulate FAILS serves the stale snapshot
// instead of failing the list, does not re-hammer BigQuery on every list (the
// cooldown), and repopulates once the query recovers. Contrast with test 13,
// where a COLD cache + failure still propagates the error.
func TestBQCacheCountStaleServesStaleOnPopulateFailure(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1 query, gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck") // now stale

	// repopulate fails, but a good (stale) snapshot is on disk: the list must
	// still succeed by serving stale, firing exactly one (failed) background
	// attempt. Drain it so the failure is recorded before the next list.
	rec.err = errors.New("boom")
	if err := walkDir(f, "d1", false); err != nil {
		t.Fatalf("stale list with failing repopulate: got %v, want nil (served stale)", err)
	}
	f.bqWG.Wait()
	if n := rec.count(); n != 2 {
		t.Fatalf("first failed repopulate fired %d queries total, want 2 (warm + 1 attempt)", n)
	}

	// a second stale list within the cooldown serves stale WITHOUT re-querying
	if err := walkDir(f, "d2", false); err != nil {
		t.Fatalf("second stale list: %v", err)
	}
	f.bqWG.Wait()
	if n := rec.count(); n != 2 {
		t.Errorf("cooldown let another attempt through: %d queries, want 2", n)
	}

	// recovery: clear the cooldown (white-box), query succeeds, cache repopulates
	// in the background while the list itself still serves the stale snapshot
	f.bqPopMu.Lock()
	f.bqLastPopFail = time.Time{}
	f.bqPopMu.Unlock()
	rec.err = nil
	if err := walkDir(f, "d1", false); err != nil {
		t.Fatalf("recovery list: %v", err)
	}
	f.bqWG.Wait()
	if n := rec.count(); n != 3 {
		t.Errorf("recovery fired %d queries total, want 3", n)
	}
	if gen := readGen(t, f, "buck"); gen != 2 {
		t.Errorf("generation after recovery = %d; want 2 (repopulated)", gen)
	}
}

// 18 (Bugbot follow-up): a stale cache whose repopulate returns ZERO rows keeps
// the stale snapshot but must NOT re-query on every later list — the empty result
// engages the same cooldown as a failure, or a transient empty inventory recreates
// the storm. (This fails on the naive "err == nil clears the cooldown" logic.)
func TestBQCacheCountStaleEmptyRepopulateDoesNotLoop(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1 query, gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck") // stale

	// inventory transiently empty: the repopulate succeeds with zero rows and
	// keeps gen 1 (a broken/regenerating inventory must not wipe a good cache).
	rec.fixture = nil
	if err := walkDir(f, "d1", false); err != nil {
		t.Fatalf("stale list with empty repopulate: %v", err)
	}
	f.bqWG.Wait() // drain so the zero-row outcome engages the cooldown
	if n := rec.count(); n != 2 {
		t.Fatalf("empty repopulate fired %d queries total, want 2 (warm + 1)", n)
	}
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Errorf("empty repopulate changed generation to %d; want 1 (kept)", gen)
	}

	// subsequent stale lists within the cooldown must NOT re-query
	for i := 0; i < 5; i++ {
		if err := walkDir(f, "d2", false); err != nil {
			t.Fatalf("stale list %d: %v", i, err)
		}
	}
	f.bqWG.Wait()
	if n := rec.count(); n != 2 {
		t.Errorf("empty-inventory cooldown breached: %d queries, want 2 (no re-query loop)", n)
	}
}

// 17 (Fix B): a stale recursive-root list whose repopulate returns zero rows
// keeps the existing cache AND still serves the caller its retained entries
// (not an empty/not-found tree), leaving the generation unchanged.
func TestBQCacheEmptyRefreshServesRetainedCache(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck") // stale, so the next list actually kicks a repopulate

	// the repopulate returns nothing (inventory mid-regeneration)
	rec.fixture = nil
	var names []string
	err := f.list(context.Background(), "buck", "", "", false, true,
		func(remote string, _ *storage.Object, _ bool) error {
			names = append(names, remote)
			return nil
		})
	if err != nil {
		t.Fatalf("empty refresh: got %v, want nil (served retained cache)", err)
	}
	if len(names) == 0 {
		t.Error("empty refresh served no entries; want the retained snapshot")
	}
	f.bqWG.Wait()
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Errorf("empty refresh changed generation to %d; want 1 (kept)", gen)
	}
}

// listNames drives the full list path (staleness check + refresh kick) and
// returns what it served, unlike collect which reads bqServe directly.
func listNames(t *testing.T, f *Fs, dir string, recurse bool) []string {
	t.Helper()
	var names []string
	err := f.list(context.Background(), "buck", dir, "", false, recurse,
		func(remote string, _ *storage.Object, _ bool) error {
			names = append(names, remote)
			return nil
		})
	if err != nil {
		t.Fatalf("list %q: %v", dir, err)
	}
	sort.Strings(names)
	return names
}

// 19. THE SIGN-726 regression test: a stale list must return the old snapshot
// while the repopulate is still in flight - the gate stays closed until after
// the list has answered, so a blocking refresh would deadlock this test.
func TestBQCacheStaleServesOldGenNonBlocking(t *testing.T) {
	f, rec := newCountFs(t, []bqRow{{name: "old.txt", size: "1"}}, true)
	if err := walkDir(f, "", true); err != nil { // warm: gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck")

	rec.fixture = []bqRow{{name: "new.txt", size: "1"}}
	rec.gate = true
	rec.started = make(chan struct{}, 1)
	rec.release = make(chan struct{})

	// this list kicks the background repopulate and must answer from gen 1
	// immediately, while the query is blocked on the gate
	if got := listNames(t, f, "", false); !reflect.DeepEqual(got, []string{"old.txt"}) {
		t.Errorf("stale list served %v; want [old.txt] (old snapshot)", got)
	}
	<-rec.started // the background populate is provably mid-flight

	// further lists keep serving the old snapshot without a second query
	if got := listNames(t, f, "", false); !reflect.DeepEqual(got, []string{"old.txt"}) {
		t.Errorf("list during refresh served %v; want [old.txt]", got)
	}
	if n := rec.count(); n != 2 {
		t.Errorf("lists during an in-flight refresh fired %d queries; want 2 (warm + the one in flight)", n)
	}

	close(rec.release)
	f.bqWG.Wait()
	if gen := readGen(t, f, "buck"); gen != 2 {
		t.Errorf("generation after background refresh = %d; want 2", gen)
	}
	if got := listNames(t, f, "", false); !reflect.DeepEqual(got, []string{"new.txt"}) {
		t.Errorf("list after refresh served %v; want [new.txt]", got)
	}
}

// 20. concurrent stale lists => one background repopulate total (single-flight),
// and none of them blocks on it. Run under -race.
func TestBQCacheBackgroundSingleFlight(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck")

	const M = 8
	rec.gate = true
	rec.started = make(chan struct{}, M)
	rec.release = make(chan struct{})

	var wg sync.WaitGroup
	var errs [M]error
	for i := 0; i < M; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = walkDir(f, "d1", false)
		}(i)
	}
	wg.Wait() // all lists answered while the one repopulate is still gated
	for i, err := range errs {
		if err != nil {
			t.Errorf("stale reader %d: %v", i, err)
		}
	}

	close(rec.release)
	f.bqWG.Wait()
	if n := rec.count(); n != 2 {
		t.Errorf("%d concurrent stale lists fired %d queries; want 2 (warm + 1 single-flight repopulate)", M, n)
	}
}

// 21. Shutdown drains an in-flight background populate before closing the bbolt
// handle (no write-after-close), and the populate still lands.
func TestBQCacheShutdownDrainsBackgroundPopulate(t *testing.T) {
	f, rec := newCountFs(t, countFixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: gen 1
		t.Fatal(err)
	}
	ageCache(t, f, "buck")

	rec.gate = true
	rec.started = make(chan struct{}, 1)
	rec.release = make(chan struct{})
	if err := walkDir(f, "d1", false); err != nil { // kicks the background populate
		t.Fatal(err)
	}
	<-rec.started // populate is mid-flight

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- f.Shutdown(context.Background()) }()
	close(rec.release) // let the populate finish; Shutdown must be waiting on it
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// 22. NewObject on a warm cache is a bbolt point lookup: correct metadata, zero
// BigQuery queries, zero Storage calls (f.svc is nil - a fallback would panic).
func TestBQCacheCountNewObjectFromCache(t *testing.T) {
	fixture := []bqRow{
		// md5 is base64 as the inventory SQL emits it (this is md5("") in base64)
		{name: "d1/f1.txt", size: "42", md5: "1B2M2Y8AsgTpgAmY7PhCfg==", updated: "2024-03-04T05:06:07Z"},
	}
	f, rec := newCountFs(t, fixture, true)
	if err := walkDir(f, "", true); err != nil { // warm: 1 query
		t.Fatal(err)
	}

	ctx := context.Background()
	o, err := f.NewObject(ctx, "d1/f1.txt")
	if err != nil {
		t.Fatalf("NewObject: %v", err)
	}
	if o.Size() != 42 {
		t.Errorf("Size = %d; want 42", o.Size())
	}
	if want := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC); !o.ModTime(ctx).Equal(want) {
		t.Errorf("ModTime = %v; want %v", o.ModTime(ctx), want)
	}
	if sum, err := o.Hash(ctx, hash.MD5); err != nil || sum != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Errorf("Hash = %q, %v; want the hex md5", sum, err)
	}
	if n := rec.count(); n != 1 {
		t.Errorf("NewObject fired %d queries; want 1 (warm only - stats must be free)", n)
	}

	// misses report ok=false so NewObject can fall back to a real Objects.Get
	// (an object newer than the inventory); they must not query anything
	if _, ok := f.bqGetObject("does/not/exist.txt"); ok {
		t.Error("bqGetObject hit for a missing key")
	}
	fCold, recCold := newCountFs(t, fixture, true)
	if _, ok := fCold.bqGetObject("d1/f1.txt"); ok {
		t.Error("bqGetObject hit on a cold cache")
	}
	if n := rec.count() + recCold.count(); n != 1 {
		t.Errorf("miss lookups fired %d total queries; want 1 (the warm populate)", n)
	}
}
