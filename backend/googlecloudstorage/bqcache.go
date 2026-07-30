package googlecloudstorage

// bbolt listing cache for bigquery_table mode. It turns the per-directory
// BigQuery query storm (one job per FUSE readdir during a tree walk) into at
// most one job per refresh: every list is served from bbolt, and the cache is
// repopulated by a single recursive root query - in the background when a
// previous snapshot exists (lists keep serving the old snapshot meanwhile),
// synchronously only when the cache is cold, or on demand via the "refresh"
// backend command. The read path never issues a per-directory query.
//
// On-disk layout (one file per remote):
//
//	meta bucket:            gen/<gcsbucket> -> current generation (decimal)
//	                        ts/<gcsbucket>  -> populated_at (RFC3339)
//	data/<gcsbucket>/<gen>: key = full object name, value = size\x00md5\x00updated
//
// Writes use generation-swap: a populate streams the new snapshot into the next
// generation in batched transactions (bounded memory), then one tiny txn flips
// the generation pointer + populated_at and drops the old generation. Readers do
// the gen lookup and scan inside a single bbolt View txn, so they always see a
// complete snapshot, never a half-written one.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rclone/rclone/fs"
	bolt "go.etcd.io/bbolt"
	storage "google.golang.org/api/storage/v1"
)

const (
	bqMetaBucket = "meta"
	bqCacheBatch = 10000 // rows buffered per bbolt write txn during populate (bounds memory)

	// bqPopRetryCooldown bounds how often a failed populate is retried while the
	// cache is stale, so a sustained BigQuery outage doesn't fire one root query
	// per list. It does not touch on-disk staleness - the first list after the
	// cooldown still repopulates the moment BigQuery recovers.
	bqPopRetryCooldown = 60 * time.Second
)

// bqValueSep separates size/md5/updated in a stored value; 0x00 can't appear in
// a (UTF-8) object name or in the numeric/RFC3339 fields it joins.
var bqValueSep = []byte{0}

// bqRowSource streams inventory rows to emit. In production it wraps the
// recursive BigQuery query; tests inject an in-memory slice.
type bqRowSource func(emit func(bqRow) error) error

func bqGenKey(bucketName string) []byte { return []byte("gen/" + bucketName) }
func bqTSKey(bucketName string) []byte  { return []byte("ts/" + bucketName) }

func bqDataBucket(bucketName string, gen uint64) []byte {
	return []byte(fmt.Sprintf("data/%s/%d", bucketName, gen))
}

func bqEncodeValue(r bqRow) []byte {
	return bytes.Join([][]byte{[]byte(r.size), []byte(r.md5), []byte(r.updated)}, bqValueSep)
}

// bqDecodeValue rebuilds a row for name from its stored value.
func bqDecodeValue(name string, v []byte) bqRow {
	parts := bytes.SplitN(v, bqValueSep, 3)
	r := bqRow{name: name}
	if len(parts) > 0 {
		r.size = string(parts[0])
	}
	if len(parts) > 1 {
		r.md5 = string(parts[1])
	}
	if len(parts) > 2 {
		r.updated = string(parts[2])
	}
	return r
}

// openBQCache opens (creating the parent dir and the meta bucket) the on-disk
// bbolt cache. The Timeout makes a second rclone process fail fast with a clear
// error instead of hanging on the exclusive flock - the file must be a local
// path owned by one process (bbolt also memory-maps it, so never shared between
// multiple machines).
func openBQCache(path string) (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("bigquery_cache_db: %w", err)
	}
	// InitialMmapSize reserves virtual address space (not RAM), so populate-driven
	// file growth rarely forces a remap - a remap blocks every concurrent reader.
	// ponytail: 1GiB covers the current corpus with two generations coexisting;
	// raise if the DB file outgrows it.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second, InitialMmapSize: 1 << 30})
	if err != nil {
		return nil, fmt.Errorf("bigquery_cache_db %q (must be a local file owned by a single rclone process): %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bqMetaBucket))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// bqReadGen returns the current generation for bucketName, 0 if none populated.
func bqReadGen(tx *bolt.Tx, bucketName string) uint64 {
	v := tx.Bucket([]byte(bqMetaBucket)).Get(bqGenKey(bucketName))
	if v == nil {
		return 0
	}
	gen, _ := strconv.ParseUint(string(v), 10, 64)
	return gen
}

// bqRemoteRoot returns the remote's root directory as list() presents it (the
// trailing "/" already applied), i.e. the slice STARTS_WITH covers for a root
// populate and the value listBQCached compares directory against.
func (f *Fs) bqRemoteRoot() string {
	_, root := f.split("")
	if root != "" {
		root += "/"
	}
	return root
}

// bqCacheStale reports whether bucketName has no generation yet, or its
// populated_at is older than bigquery_cache_max_age (0 disables the age check).
func (f *Fs) bqCacheStale(bucketName string) (bool, error) {
	stale := false
	err := f.bqDB.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(bqMetaBucket))
		if meta.Get(bqGenKey(bucketName)) == nil {
			stale = true
			return nil
		}
		maxAge := time.Duration(f.opt.BigQueryCacheMaxAge)
		if maxAge <= 0 {
			return nil
		}
		ts := meta.Get(bqTSKey(bucketName))
		if ts == nil {
			stale = true
			return nil
		}
		populatedAt, err := time.Parse(time.RFC3339, string(ts))
		if err != nil {
			stale = true // unparseable timestamp - repopulate to be safe
			return nil
		}
		stale = time.Since(populatedAt) > maxAge
		return nil
	})
	return stale, err
}

// cacheRootSource is the only origin of cache-backed BigQuery queries: one
// recursive query over the remote root. bqQueryRows itself refuses any
// cache-backed query that is not exactly this (root, recursive), so a
// per-directory query from the cache-on path is impossible by construction.
func (f *Fs) cacheRootSource(ctx context.Context, bucketName string) bqRowSource {
	return func(emit func(bqRow) error) error {
		return f.bqQuery(ctx, bucketName, f.bqRemoteRoot(), true, emit)
	}
}

// bqGen returns the current generation for bucketName, 0 if none populated.
func (f *Fs) bqGen(bucketName string) uint64 {
	var gen uint64
	_ = f.bqDB.View(func(tx *bolt.Tx) error {
		gen = bqReadGen(tx, bucketName)
		return nil
	})
	return gen
}

// listBQ serves a list from the bbolt cache (bigquery_table always runs with the
// cache). Fresh cache: bbolt only, no BigQuery traffic. Stale with a previous
// snapshot: serve the old snapshot immediately and refresh in the background
// (single-flight). Cold (nothing populated yet): block on one synchronous
// populate - there is nothing to serve. Nothing here ever issues a
// per-directory query.
func (f *Fs) listBQ(ctx context.Context, bucketName, directory, prefix string, addBucket, recurse bool, fn listFn) error {
	stale, err := f.bqCacheStale(bucketName)
	if err != nil {
		return err
	}
	if stale {
		if f.bqGen(bucketName) == 0 {
			if err := f.bqRefresh(ctx, bucketName, false); err != nil {
				return err // cold cache, nothing to serve
			}
		} else {
			f.bqKickRefresh(bucketName)
		}
	}
	return f.bqServe(ctx, bucketName, directory, prefix, addBucket, recurse, fn)
}

// bqRefresh runs one synchronous populate under bqPopMu. It is the only
// blocking entry point: the cold-cache list path (force=false) and the
// "refresh" backend command (force=true, which bypasses the staleness re-check
// and additionally reports a zero-row query as an error, so a cron caller sees
// a failed refresh instead of a silently retained old snapshot).
func (f *Fs) bqRefresh(ctx context.Context, bucketName string, force bool) error {
	f.bqPopMu.Lock()
	defer f.bqPopMu.Unlock()
	if !force {
		// re-check under the lock: a racer may have populated while we waited
		stale, err := f.bqCacheStale(bucketName)
		if err != nil || !stale {
			return err
		}
	}
	advanced, err := f.bqPopulateLocked(ctx, bucketName)
	if err != nil {
		return err
	}
	if force && !advanced {
		return fmt.Errorf("refresh: BigQuery returned no rows, kept existing cache for %q (gen %d)", bucketName, f.bqGen(bucketName))
	}
	return nil
}

// bqKickRefresh starts a background populate for a stale-but-servable cache.
// ponytail: bqPopMu.TryLock IS the single-flight - losing the race means a
// populate is already running (or starting), so there is nothing to kick.
func (f *Fs) bqKickRefresh(bucketName string) {
	if !f.bqPopMu.TryLock() {
		return
	}
	// re-check under the lock, and honor the failure cooldown before spawning:
	// a stat storm during a BigQuery outage must start zero goroutines
	stale, err := f.bqCacheStale(bucketName)
	cooling := !f.bqLastPopFail.IsZero() && time.Since(f.bqLastPopFail) < bqPopRetryCooldown
	if err != nil || !stale || cooling {
		f.bqPopMu.Unlock()
		return
	}
	f.bqWG.Add(1)
	go func() {
		defer f.bqWG.Done()
		defer f.bqPopMu.Unlock()
		if _, err := f.bqPopulateLocked(f.bqCtx, bucketName); err != nil {
			fs.Errorf(f, "BigQuery listing cache: background refresh of %q failed, serving stale cache: %v", bucketName, err)
		}
	}()
}

// bqPopulateLocked runs one populate and owns the bqLastPopFail bookkeeping.
// advanced reports whether a new generation was flipped in (a zero-row query
// keeps the old one). Caller must hold bqPopMu.
func (f *Fs) bqPopulateLocked(ctx context.Context, bucketName string) (advanced bool, err error) {
	gen := f.bqGen(bucketName)
	err = f.bqPopulate(bucketName, f.cacheRootSource(ctx, bucketName))
	advanced = f.bqGen(bucketName) != gen
	switch {
	case err == nil && advanced:
		f.bqLastPopFail = time.Time{} // refreshed to a new generation
	case gen != 0:
		// failed, or returned zero rows: the old snapshot keeps being served, so
		// back off - don't re-fire a full-root query on every list during an
		// outage. The on-disk timestamp is untouched, so the first list past the
		// cooldown repopulates the moment a real snapshot returns.
		f.bqLastPopFail = time.Now()
	}
	return advanced, err
}

// bqPopulate rebuilds bucketName's cache from one recursive BigQuery query over
// the remote root using generation-swap. On any error the flip is skipped, so
// readers keep seeing the previous complete snapshot. Rows come from src
// (BigQuery in production). Caller must hold bqPopMu; callers serve the
// requested slice from bbolt afterwards, populate never emits entries itself.
func (f *Fs) bqPopulate(bucketName string, src bqRowSource) error {
	// pick the next generation and start it empty (clears any aborted attempt)
	var newGen uint64
	if err := f.bqDB.Update(func(tx *bolt.Tx) error {
		newGen = bqReadGen(tx, bucketName) + 1
		name := bqDataBucket(bucketName, newGen)
		if err := tx.DeleteBucket(name); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		_, err := tx.CreateBucket(name)
		return err
	}); err != nil {
		return err
	}

	pending := make([]bqRow, 0, bqCacheBatch)
	written := 0
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := f.bqDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bqDataBucket(bucketName, newGen))
			for _, r := range pending {
				if err := b.Put([]byte(r.name), bqEncodeValue(r)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		written += len(pending)
		pending = pending[:0]
		return nil
	}

	err := src(func(r bqRow) error {
		pending = append(pending, r)
		if len(pending) >= bqCacheBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}

	// don't flip to an empty generation: a broken or empty inventory must not wipe
	// a good cache. ponytail: a genuinely empty remote then never caches and
	// re-queries on each list - acceptable, that's not the workload this guards.
	if written == 0 {
		// drop the empty generation we provisioned; keep serving the current one
		_ = f.bqDB.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket(bqDataBucket(bucketName, newGen))
		})
		fs.Infof(f, "BigQuery listing cache: query returned no rows, kept existing cache for %q", bucketName)
		return nil
	}
	fs.Infof(f, "BigQuery listing cache: populated %q with %d objects (gen %d)", bucketName, written, newGen)

	// atomic flip + drop the old generation
	return f.bqDB.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(bqMetaBucket))
		oldGen := bqReadGen(tx, bucketName)
		if err := meta.Put(bqGenKey(bucketName), []byte(strconv.FormatUint(newGen, 10))); err != nil {
			return err
		}
		if err := meta.Put(bqTSKey(bucketName), []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
			return err
		}
		if oldGen != 0 && oldGen != newGen {
			if err := tx.DeleteBucket(bqDataBucket(bucketName, oldGen)); err != nil && err != bolt.ErrBucketNotFound {
				return err
			}
		}
		return nil
	})
}

// bqServe answers a list from the current generation inside a single read txn (a
// consistent snapshot, immune to a concurrent flip). Recursive lists scan the
// whole prefix; single-level lists use seek-skip - emit one synthesized entry
// per immediate sub-directory and jump past its sub-tree - to return immediate
// children in O(children) rather than O(descendants).
func (f *Fs) bqServe(ctx context.Context, bucketName, directory, prefix string, addBucket, recurse bool, fn listFn) error {
	found := 0
	err := f.bqDB.View(func(tx *bolt.Tx) error {
		gen := bqReadGen(tx, bucketName)
		if gen == 0 {
			return nil // nothing populated; handled as not-found below
		}
		data := tx.Bucket(bqDataBucket(bucketName, gen))
		if data == nil {
			return nil
		}
		c := data.Cursor()
		pfx := []byte(directory)

		if recurse {
			for k, v := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); k, v = c.Next() {
				emitted, err := f.emitBQRow(bqDecodeValue(string(k), v), directory, prefix, bucketName, addBucket, fn)
				if err != nil {
					return err
				}
				if emitted {
					found++
				}
			}
			return nil
		}

		k, v := c.Seek(pfx)
		for k != nil && bytes.HasPrefix(k, pfx) {
			rel := k[len(pfx):]
			if slash := bytes.IndexByte(rel, '/'); slash >= 0 {
				// immediate sub-directory: synthesize "<dir><sub>/" then skip its sub-tree
				child := make([]byte, len(pfx)+slash+1)
				copy(child, pfx)
				copy(child[len(pfx):], rel[:slash+1])
				emitted, err := f.emitBQRow(bqRow{name: string(child)}, directory, prefix, bucketName, addBucket, fn)
				if err != nil {
					return err
				}
				if emitted {
					found++
				}
				// 0xFF can't start a key under child (object names are UTF-8), so this
				// seeks straight to the first key past the sub-tree
				k, v = c.Seek(append(child, 0xFF))
				continue
			}
			emitted, err := f.emitBQRow(bqDecodeValue(string(k), v), directory, prefix, bucketName, addBucket, fn)
			if err != nil {
				return err
			}
			if emitted {
				found++
			}
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return err
	}
	fs.Infof(f, "BigQuery listing cache: served %d entries for %q from cache (recurse=%v)", found, directory, recurse)
	// match the Cloud Storage list path: an empty result may mean a missing dir
	if found == 0 {
		return f.bqNotFound(ctx, bucketName, directory)
	}
	return nil
}

// bqGetObject answers a single-object stat from the current cache generation.
// ok=false (cold cache, or key absent - e.g. an object newer than the
// inventory) means the caller falls back to a real Objects.Get. Never queries
// BigQuery and never triggers a populate: a stat storm must not spend money.
// No staleness check either - listings serve these same rows at the same age.
func (f *Fs) bqGetObject(remote string) (info *storage.Object, ok bool) {
	bucketName, bucketPath := f.split(remote) // bucketPath is Enc-encoded, matching the stored key
	if bucketName == "" || bucketPath == "" {
		return nil, false
	}
	_ = f.bqDB.View(func(tx *bolt.Tx) error {
		gen := bqReadGen(tx, bucketName)
		if gen == 0 {
			return nil
		}
		data := tx.Bucket(bqDataBucket(bucketName, gen))
		if data == nil {
			return nil
		}
		if v := data.Get([]byte(bucketPath)); v != nil {
			info, ok = bqRowObject(bqDecodeValue(bucketPath, v)), true
		}
		return nil
	})
	return info, ok
}
