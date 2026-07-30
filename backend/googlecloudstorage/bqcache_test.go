package googlecloudstorage

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	storage "google.golang.org/api/storage/v1"

	"github.com/rclone/rclone/fs"
)

// newTestCacheFs returns an Fs backed by a fresh bbolt cache in a temp dir
// (identity path encoding, empty root => remote root is "").
func newTestCacheFs(t *testing.T) *Fs {
	t.Helper()
	f := &Fs{}
	f.opt.BigQueryCacheMaxAge = fs.Duration(48 * time.Hour)
	db, err := openBQCache(filepath.Join(t.TempDir(), "cache.bolt"))
	if err != nil {
		t.Fatalf("openBQCache: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	f.bqDB = db
	return f
}

func sliceSource(rows []bqRow) bqRowSource {
	return func(emit func(bqRow) error) error {
		for _, r := range rows {
			if err := emit(r); err != nil {
				return err
			}
		}
		return nil
	}
}

// collect serves (bucketName, directory, recurse) and returns the remotes,
// directories suffixed with "/", sorted. Only valid for non-empty listings:
// an empty result would call bqVerifyNotFound (which needs the Storage client).
func collect(t *testing.T, f *Fs, bucketName, directory string, recurse bool) []string {
	t.Helper()
	var got []string
	fn := func(remote string, _ *storage.Object, isDir bool) error {
		if isDir {
			remote += "/"
		}
		got = append(got, remote)
		return nil
	}
	if err := f.bqServe(context.Background(), bucketName, directory, "", false, recurse, fn); err != nil {
		t.Fatalf("bqServe(%q, recurse=%v): %v", directory, recurse, err)
	}
	sort.Strings(got)
	return got
}

func readGen(t *testing.T, f *Fs, bucketName string) uint64 {
	t.Helper()
	var gen uint64
	if err := f.bqDB.View(func(tx *bolt.Tx) error {
		gen = bqReadGen(tx, bucketName)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return gen
}

func TestBQCacheValueRoundtrip(t *testing.T) {
	for _, r := range []bqRow{
		{name: "x", size: "10", md5: "abc", updated: "2024-01-01T00:00:00Z"},
		{name: "y", size: "0"},          // empty md5/updated, as for a directory marker
		{name: "z"},                     // all empty
		{name: "w", updated: "2024-02"}, // gap in the middle
	} {
		if got := bqDecodeValue(r.name, bqEncodeValue(r)); got != r {
			t.Errorf("roundtrip %q = %+v, want %+v", r.name, got, r)
		}
	}
}

// fixtureRows is one object directly in root, a sub-tree two levels deep, and a
// second sibling directory - enough to exercise seek-skip dedup.
var fixtureRows = []bqRow{
	{name: "a.txt", size: "10", md5: "aaa", updated: "2024-01-01T00:00:00Z"},
	{name: "sub/b.txt", size: "20", md5: "bbb", updated: "2024-01-02T00:00:00Z"},
	{name: "sub/c/d.txt", size: "30"},
	{name: "sub2/e.txt", size: "40"},
}

func TestBQCacheServe(t *testing.T) {
	f := newTestCacheFs(t)
	if err := f.bqPopulate("buck", sliceSource(fixtureRows)); err != nil {
		t.Fatalf("populate: %v", err)
	}

	for _, tc := range []struct {
		name      string
		directory string
		recurse   bool
		want      []string
	}{
		// single-level synthesises one entry per immediate sub-dir (sub/ appears
		// once despite two descendants) and skips deeper objects
		{name: "single root", directory: "", recurse: false, want: []string{"a.txt", "sub/", "sub2/"}},
		{name: "single sub", directory: "sub/", recurse: false, want: []string{"sub/b.txt", "sub/c/"}},
		// recursive returns every object under the prefix, no synthesised dirs
		{name: "recurse root", directory: "", recurse: true, want: []string{"a.txt", "sub/b.txt", "sub/c/d.txt", "sub2/e.txt"}},
		{name: "recurse sub", directory: "sub/", recurse: true, want: []string{"sub/b.txt", "sub/c/d.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := collect(t, f, "buck", tc.directory, tc.recurse); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBQCacheObjectFields(t *testing.T) {
	f := newTestCacheFs(t)
	if err := f.bqPopulate("buck", sliceSource(fixtureRows)); err != nil {
		t.Fatalf("populate: %v", err)
	}
	var obj *storage.Object
	fn := func(remote string, o *storage.Object, _ bool) error {
		if remote == "a.txt" {
			obj = o
		}
		return nil
	}
	if err := f.bqServe(context.Background(), "buck", "", "", false, true, fn); err != nil {
		t.Fatal(err)
	}
	if obj == nil {
		t.Fatal("a.txt not served")
	}
	if obj.Size != 10 || obj.Md5Hash != "aaa" || obj.Updated != "2024-01-01T00:00:00Z" {
		t.Errorf("object = size %d md5 %q updated %q", obj.Size, obj.Md5Hash, obj.Updated)
	}
}

// A changed object appears once per snapshotTime it was captured at; the query
// orders by (name, snapshotTime), so the newest row arrives last and bbolt's
// last-Put-wins must leave exactly its metadata in the cache.
func TestBQCachePopulateDuplicateNameKeepsNewest(t *testing.T) {
	f := newTestCacheFs(t)
	if err := f.bqPopulate("buck", sliceSource([]bqRow{
		{name: "a.txt", size: "10", md5: "old", updated: "2024-01-01T00:00:00Z"},
		{name: "a.txt", size: "20", md5: "new", updated: "2024-06-01T00:00:00Z"},
	})); err != nil {
		t.Fatalf("populate: %v", err)
	}
	var obj *storage.Object
	fn := func(remote string, o *storage.Object, _ bool) error {
		if remote == "a.txt" {
			obj = o
		}
		return nil
	}
	if err := f.bqServe(context.Background(), "buck", "", "", false, true, fn); err != nil {
		t.Fatal(err)
	}
	if obj == nil {
		t.Fatal("a.txt not served")
	}
	if obj.Size != 20 || obj.Md5Hash != "new" || obj.Updated != "2024-06-01T00:00:00Z" {
		t.Errorf("duplicate name kept size %d md5 %q updated %q; want the newest row", obj.Size, obj.Md5Hash, obj.Updated)
	}
}

func TestBQCacheGenerationSwap(t *testing.T) {
	f := newTestCacheFs(t)
	if err := f.bqPopulate("buck", sliceSource([]bqRow{{name: "old.txt", size: "1"}})); err != nil {
		t.Fatal(err)
	}
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Fatalf("first populate generation = %d, want 1", gen)
	}
	if err := f.bqPopulate("buck", sliceSource([]bqRow{{name: "new.txt", size: "2"}})); err != nil {
		t.Fatal(err)
	}
	if gen := readGen(t, f, "buck"); gen != 2 {
		t.Fatalf("second populate generation = %d, want 2", gen)
	}
	// the old generation's data bucket is dropped, the new one present
	if err := f.bqDB.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bqDataBucket("buck", 1)) != nil {
			t.Error("old generation bucket still present")
		}
		if tx.Bucket(bqDataBucket("buck", 2)) == nil {
			t.Error("new generation bucket missing")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, f, "buck", "", true); !reflect.DeepEqual(got, []string{"new.txt"}) {
		t.Errorf("after swap got %v, want [new.txt]", got)
	}
}

// an empty source (broken/empty inventory) must not flip and wipe a good cache.
func TestBQCacheEmptySourceKeepsCache(t *testing.T) {
	f := newTestCacheFs(t)
	if err := f.bqPopulate("buck", sliceSource([]bqRow{{name: "keep.txt", size: "1"}})); err != nil {
		t.Fatal(err)
	}
	if err := f.bqPopulate("buck", sliceSource(nil)); err != nil {
		t.Fatal(err)
	}
	if gen := readGen(t, f, "buck"); gen != 1 {
		t.Errorf("generation after empty refresh = %d, want 1 (unchanged)", gen)
	}
	if got := collect(t, f, "buck", "", true); !reflect.DeepEqual(got, []string{"keep.txt"}) {
		t.Errorf("after empty refresh got %v, want [keep.txt]", got)
	}
}

func TestBQCacheStale(t *testing.T) {
	f := newTestCacheFs(t)

	// cold: no generation yet
	if stale, err := f.bqCacheStale("buck"); err != nil || !stale {
		t.Fatalf("cold: stale=%v err=%v, want stale", stale, err)
	}

	if err := f.bqPopulate("buck", sliceSource([]bqRow{{name: "a.txt", size: "1"}})); err != nil {
		t.Fatal(err)
	}
	if stale, err := f.bqCacheStale("buck"); err != nil || stale {
		t.Fatalf("fresh: stale=%v err=%v, want not stale", stale, err)
	}

	// age populated_at past max_age
	old := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339)
	if err := f.bqDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bqMetaBucket)).Put(bqTSKey("buck"), []byte(old))
	}); err != nil {
		t.Fatal(err)
	}
	if stale, err := f.bqCacheStale("buck"); err != nil || !stale {
		t.Fatalf("aged: stale=%v err=%v, want stale", stale, err)
	}

	// max_age 0 disables the age check (still fresh because a generation exists)
	f.opt.BigQueryCacheMaxAge = 0
	if stale, err := f.bqCacheStale("buck"); err != nil || stale {
		t.Fatalf("maxage=0: stale=%v err=%v, want not stale", stale, err)
	}
}
