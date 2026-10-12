package vfscache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local" // import the local backend
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/diskusage"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain drives the tests
func TestMain(m *testing.M) {
	fstest.TestMain(m)
}

// convert c.item to a string
func itemAsString(c *Cache) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for name, item := range c.item {
		out = append(out, fmt.Sprintf("name=%q opens=%d size=%d", filepath.ToSlash(name), item.opens, item.info.Size))
	}
	sort.Strings(out)
	return out
}

// convert c.item to a string
func itemSpaceAsString(c *Cache) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for name, item := range c.item {
		space := item.info.Rs.Size()
		out = append(out, fmt.Sprintf("name=%q opens=%d size=%d space=%d", filepath.ToSlash(name), item.opens, item.info.Size, space))
	}
	sort.Strings(out)
	return out
}

// open an item and write to it
func itemWrite(t *testing.T, item *Item, contents string) {
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte(contents), 0)
	require.NoError(t, err)
}

func assertPathNotExist(t *testing.T, path string) {
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func assertPathExist(t *testing.T, path string) os.FileInfo {
	fi, err := os.Stat(path)
	assert.NoError(t, err)
	return fi
}

type avInfo struct {
	Remote string
	Size   int64
	IsDir  bool
}

// avInfos is written by addVirtual, which the background reload goroutine can
// call, so it needs a lock - read it with getAvInfos, never directly.
var (
	avInfosMu sync.Mutex
	avInfos   []avInfo
)

func addVirtual(remote string, size int64, isDir bool) error {
	avInfosMu.Lock()
	defer avInfosMu.Unlock()
	avInfos = append(avInfos, avInfo{
		Remote: remote,
		Size:   size,
		IsDir:  isDir,
	})
	return nil
}

// getAvInfos returns the remotes passed to addVirtual so far, sorted
func getAvInfos() []string {
	avInfosMu.Lock()
	defer avInfosMu.Unlock()
	out := make([]string, 0, len(avInfos))
	for _, av := range avInfos {
		out = append(out, filepath.ToSlash(av.Remote))
	}
	return out
}

func resetAvInfos() {
	avInfosMu.Lock()
	defer avInfosMu.Unlock()
	avInfos = nil
}

func newTestCacheOpt(t *testing.T, opt vfscommon.Options) (r *fstest.Run, c *Cache) {
	r = fstest.NewRun(t)

	ctx, cancel := context.WithCancel(context.Background())

	resetAvInfos()
	c, err := New(ctx, r.Fremote, &opt, addVirtual)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := c.CleanUp()
		require.NoError(t, err)
		assertPathNotExist(t, c.root)
		cancel()
	})

	return r, c
}

func newTestCache(t *testing.T) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt

	// Disable the cache cleaner as it interferes with these tests
	opt.CachePollInterval = 0

	// Disable synchronous write
	opt.WriteBack = 0

	// Disable handle caching so existing tests get immediate close behavior
	opt.HandleCaching = 0

	return newTestCacheOpt(t, opt)
}

func TestCacheNew(t *testing.T) {
	r, c := newTestCache(t)

	assert.Contains(t, c.root, "vfs")
	assert.Contains(t, c.fcache.Root(), filepath.Base(r.Fremote.Root()))
	assert.Equal(t, []string(nil), itemAsString(c))

	// createItemDir
	p, err := c.createItemDir("potato")
	require.NoError(t, err)
	assert.Equal(t, "potato", filepath.Base(p))
	assert.Equal(t, []string(nil), itemAsString(c))

	fi := assertPathExist(t, filepath.Dir(p))
	assert.True(t, fi.IsDir())

	// get
	item, _ := c.get("potato")
	item2, _ := c.get("potato")
	assert.Equal(t, item, item2)
	assert.WithinDuration(t, time.Now(), item.info.ATime, time.Second)

	// open
	assert.Equal(t, []string{
		`name="potato" opens=0 size=0`,
	}, itemAsString(c))
	potato := c.Item("/potato")
	require.NoError(t, potato.Open(nil))
	assert.Equal(t, []string{
		`name="potato" opens=1 size=0`,
	}, itemAsString(c))
	assert.WithinDuration(t, time.Now(), potato.info.ATime, time.Second)
	assert.Equal(t, 1, potato.opens)

	// write the file
	require.NoError(t, potato.Truncate(5))
	atime := time.Now()
	potato.info.ATime = atime

	assert.Equal(t, []string{
		`name="potato" opens=1 size=5`,
	}, itemAsString(c))
	assert.True(t, atime.Equal(potato.info.ATime), fmt.Sprintf("%v != %v", atime, potato.info.ATime))

	// try purging with file open
	c.purgeOld(10 * time.Second)
	assertPathExist(t, p)

	// close
	assert.Equal(t, []string{
		`name="potato" opens=1 size=5`,
	}, itemAsString(c))
	require.NoError(t, potato.Truncate(6))
	assert.Equal(t, []string{
		`name="potato" opens=1 size=6`,
	}, itemAsString(c))
	require.NoError(t, potato.Close(nil))
	assert.Equal(t, []string{
		`name="potato" opens=0 size=6`,
	}, itemAsString(c))
	item, _ = c.get("potato")
	assert.WithinDuration(t, time.Now(), item.info.ATime, time.Second)
	assert.Equal(t, 0, item.opens)

	// try purging with file closed
	c.purgeOld(10 * time.Second)
	assertPathExist(t, p)

	//.. purge again with -ve age
	c.purgeOld(-10 * time.Second)
	assertPathNotExist(t, p)

	// clean - have tested the internals already
	c.clean(false)
}

func TestCacheOpens(t *testing.T) {
	_, c := newTestCache(t)

	assert.Equal(t, []string(nil), itemAsString(c))
	potato := c.Item("potato")
	require.NoError(t, potato.Open(nil))
	assert.Equal(t, []string{
		`name="potato" opens=1 size=0`,
	}, itemAsString(c))
	require.NoError(t, potato.Open(nil))
	assert.Equal(t, []string{
		`name="potato" opens=2 size=0`,
	}, itemAsString(c))
	require.NoError(t, potato.Close(nil))
	assert.Equal(t, []string{
		`name="potato" opens=1 size=0`,
	}, itemAsString(c))
	require.NoError(t, potato.Close(nil))
	assert.Equal(t, []string{
		`name="potato" opens=0 size=0`,
	}, itemAsString(c))

	require.NoError(t, potato.Open(nil))
	a1 := c.Item("a//b/c/d/one")
	a2 := c.Item("a/b/c/d/e/two")
	a3 := c.Item("a/b/c/d/e/f/three")
	require.NoError(t, a1.Open(nil))
	require.NoError(t, a2.Open(nil))
	require.NoError(t, a3.Open(nil))
	assert.Equal(t, []string{
		`name="a/b/c/d/e/f/three" opens=1 size=0`,
		`name="a/b/c/d/e/two" opens=1 size=0`,
		`name="a/b/c/d/one" opens=1 size=0`,
		`name="potato" opens=1 size=0`,
	}, itemAsString(c))
	require.NoError(t, potato.Close(nil))
	require.NoError(t, a1.Close(nil))
	require.NoError(t, a2.Close(nil))
	require.NoError(t, a3.Close(nil))
	assert.Equal(t, []string{
		`name="a/b/c/d/e/f/three" opens=0 size=0`,
		`name="a/b/c/d/e/two" opens=0 size=0`,
		`name="a/b/c/d/one" opens=0 size=0`,
		`name="potato" opens=0 size=0`,
	}, itemAsString(c))
}

// test the open, createItemDir, purge, close, purge sequence
func TestCacheOpenMkdir(t *testing.T) {
	_, c := newTestCache(t)

	// open
	potato := c.Item("sub/potato")
	require.NoError(t, potato.Open(nil))

	assert.Equal(t, []string{
		`name="sub/potato" opens=1 size=0`,
	}, itemAsString(c))

	// createItemDir
	p, err := c.createItemDir("sub/potato")
	require.NoError(t, err)
	assert.Equal(t, "potato", filepath.Base(p))
	assert.Equal(t, []string{
		`name="sub/potato" opens=1 size=0`,
	}, itemAsString(c))

	// test directory exists
	fi := assertPathExist(t, filepath.Dir(p))
	assert.True(t, fi.IsDir())

	// clean the cache
	c.purgeOld(-10 * time.Second)

	// test directory still exists
	fi = assertPathExist(t, filepath.Dir(p))
	assert.True(t, fi.IsDir())

	// close
	require.NoError(t, potato.Close(nil))

	assert.Equal(t, []string{
		`name="sub/potato" opens=0 size=0`,
	}, itemAsString(c))

	// clean the cache
	c.purgeOld(-10 * time.Second)
	c.purgeEmptyDirs("", true)

	assert.Equal(t, []string(nil), itemAsString(c))

	// test directory does not exist
	assertPathNotExist(t, filepath.Dir(p))
}

func TestCachePurgeOld(t *testing.T) {
	_, c := newTestCache(t)

	// Test funcs
	c.purgeOld(-10 * time.Second)

	potato2 := c.Item("sub/dir2/potato2")
	require.NoError(t, potato2.Open(nil))
	potato := c.Item("sub/dir/potato")
	require.NoError(t, potato.Open(nil))
	require.NoError(t, potato2.Close(nil))
	require.NoError(t, potato.Open(nil))

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=2 size=0`,
		`name="sub/dir2/potato2" opens=0 size=0`,
	}, itemAsString(c))

	c.purgeOld(-10 * time.Second)

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=2 size=0`,
	}, itemAsString(c))

	require.NoError(t, potato.Close(nil))

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=1 size=0`,
	}, itemAsString(c))

	c.purgeOld(-10 * time.Second)

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=1 size=0`,
	}, itemAsString(c))

	require.NoError(t, potato.Close(nil))

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=0 size=0`,
	}, itemAsString(c))

	c.purgeOld(10 * time.Second)

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=0 size=0`,
	}, itemAsString(c))

	c.purgeOld(-10 * time.Second)

	assert.Equal(t, []string(nil), itemAsString(c))
}

func TestCachePurgeOverQuota(t *testing.T) {
	_, c := newTestCache(t)

	// Test funcs

	// Make some test files
	potato := c.Item("sub/dir/potato")
	itemWrite(t, potato, "hello")

	potato2 := c.Item("sub/dir2/potato2")
	itemWrite(t, potato2, "hello2")

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=1 size=5`,
		`name="sub/dir2/potato2" opens=1 size=6`,
	}, itemAsString(c))

	// Check nothing removed
	c.opt.CacheMaxSize = 1
	c.purgeOverQuota()

	// Close the files
	require.NoError(t, potato.Close(nil))
	require.NoError(t, potato2.Close(nil))

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=0 size=5`,
		`name="sub/dir2/potato2" opens=0 size=6`,
	}, itemAsString(c))

	// Update the stats to read the total size
	c.updateUsed()

	// make potato2 definitely after potato
	t1 := time.Now().Add(10 * time.Second)
	potato2.info.ATime = t1

	// Check only potato removed to get below quota
	c.opt.CacheMaxSize = 10
	c.purgeOverQuota()
	assert.Equal(t, int64(6), c.used)

	assert.Equal(t, []string{
		`name="sub/dir2/potato2" opens=0 size=6`,
	}, itemAsString(c))

	// Put potato back
	potato = c.Item("sub/dir/potato")
	require.NoError(t, potato.Open(nil))
	require.NoError(t, potato.Truncate(5))
	require.NoError(t, potato.Close(nil))

	// Update the stats to read the total size
	c.updateUsed()

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=0 size=5`,
		`name="sub/dir2/potato2" opens=0 size=6`,
	}, itemAsString(c))

	// make potato definitely after potato2
	t2 := t1.Add(20 * time.Second)
	potato.info.ATime = t2

	// Check only potato2 removed to get below quota
	c.opt.CacheMaxSize = 10
	c.purgeOverQuota()
	assert.Equal(t, int64(5), c.used)
	c.purgeEmptyDirs("", true)

	assert.Equal(t, []string{
		`name="sub/dir/potato" opens=0 size=5`,
	}, itemAsString(c))

	// Now purge everything
	c.opt.CacheMaxSize = 1
	c.purgeOverQuota()
	assert.Equal(t, int64(0), c.used)
	c.purgeEmptyDirs("", true)

	assert.Equal(t, []string(nil), itemAsString(c))

	// Check nothing left behind
	c.clean(false)
	assert.Equal(t, int64(0), c.used)
	assert.Equal(t, []string(nil), itemAsString(c))
}

func TestCachePurgeMinFreeSpace(t *testing.T) {
	du, err := diskusage.New(config.GetCacheDir())
	if err == diskusage.ErrUnsupported {
		t.Skip(err)
	}
	// We've tested the quota mechanism already, so just test the
	// min free space quota is working.
	_, c := newTestCache(t)

	// First set free space quota very small and check it is OK
	c.opt.CacheMinFreeSpace = 1
	assert.True(t, c.minFreeSpaceQuotaOK())
	assert.True(t, c.quotasOK())

	// Now set it a bit larger than the current disk available and check it is BAD
	c.opt.CacheMinFreeSpace = fs.SizeSuffix(du.Available) + fs.Gibi
	assert.False(t, c.minFreeSpaceQuotaOK())
	assert.False(t, c.quotasOK())
}

// test reset clean files
func TestCachePurgeClean(t *testing.T) {
	r, c := newItemTestCache(t)
	contents, obj, potato1 := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, potato1.Open(obj))
	require.NoError(t, potato1.Open(obj))

	size, err := potato1.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Read something to instantiate the cache file
	buf := make([]byte, 10)
	_, err = potato1.ReadAt(buf, 10)
	require.NoError(t, err)

	// Test cache file present
	_, err = os.Stat(potato1.c.toOSPath(potato1.name))
	require.NoError(t, err)

	// Add some potatoes
	potato2 := c.Item("sub/dir/potato2")
	require.NoError(t, potato2.Open(nil))
	require.NoError(t, potato2.Truncate(5))

	potato3 := c.Item("sub/dir/potato3")
	require.NoError(t, potato3.Open(nil))
	require.NoError(t, potato3.Truncate(6))

	c.updateUsed()
	c.opt.CacheMaxSize = 1
	c.purgeClean()
	assert.Equal(t, []string{
		`name="existing" opens=2 size=100 space=0`,
		`name="sub/dir/potato2" opens=1 size=5 space=5`,
		`name="sub/dir/potato3" opens=1 size=6 space=6`,
	}, itemSpaceAsString(c))
	assert.Equal(t, int64(11), c.used)

	require.NoError(t, potato2.Close(nil))
	c.opt.CacheMaxSize = 1
	c.purgeClean()
	assert.Equal(t, []string{
		`name="existing" opens=2 size=100 space=0`,
		`name="sub/dir/potato3" opens=1 size=6 space=6`,
	}, itemSpaceAsString(c))
	assert.Equal(t, int64(6), c.used)

	require.NoError(t, potato1.Close(nil))
	require.NoError(t, potato1.Close(nil))
	require.NoError(t, potato3.Close(nil))

	// Remove all files now.  The are all not in use.
	// purgeClean does not remove empty cache files. purgeOverQuota does.
	// So we use purgeOverQuota here for the cleanup.
	c.opt.CacheMaxSize = 1
	c.purgeOverQuota()

	c.purgeEmptyDirs("", true)

	assert.Equal(t, []string(nil), itemAsString(c))
}

func TestCacheInUse(t *testing.T) {
	_, c := newTestCache(t)

	assert.False(t, c.InUse("potato"))

	potato := c.Item("potato")

	assert.False(t, c.InUse("potato"))

	require.NoError(t, potato.Open(nil))

	assert.True(t, c.InUse("potato"))

	require.NoError(t, potato.Close(nil))

	assert.False(t, c.InUse("potato"))
}

func TestCacheDirtyItem(t *testing.T) {
	_, c := newTestCache(t)

	assert.Nil(t, c.DirtyItem("potato"))

	potato := c.Item("potato")

	assert.Nil(t, c.DirtyItem("potato"))

	require.NoError(t, potato.Open(nil))
	require.NoError(t, potato.Truncate(5))

	assert.Equal(t, potato, c.DirtyItem("potato"))

	require.NoError(t, potato.Close(nil))

	assert.Nil(t, c.DirtyItem("potato"))
}

func TestCacheExistsAndRemove(t *testing.T) {
	_, c := newTestCache(t)

	assert.False(t, c.Exists("potato"))

	potato := c.Item("potato")

	assert.False(t, c.Exists("potato"))

	require.NoError(t, potato.Open(nil))

	assert.True(t, c.Exists("potato"))

	require.NoError(t, potato.Close(nil))

	assert.True(t, c.Exists("potato"))

	c.Remove("potato")

	assert.False(t, c.Exists("potato"))

}

func TestCacheRename(t *testing.T) {
	_, c := newTestCache(t)

	// setup

	assert.False(t, c.Exists("potato"))
	potato := c.Item("potato")
	require.NoError(t, potato.Open(nil))
	require.NoError(t, potato.Close(nil))
	assert.True(t, c.Exists("potato"))

	osPath := c.toOSPath("potato")
	osPathMeta := c.toOSPathMeta("potato")
	assertPathExist(t, osPath)
	assertPathExist(t, osPathMeta)

	// rename potato -> newPotato

	require.NoError(t, c.Rename("potato", "newPotato", nil))
	assertPathNotExist(t, osPath)
	assertPathNotExist(t, osPathMeta)
	assert.False(t, c.Exists("potato"))

	osPath = c.toOSPath("newPotato")
	osPathMeta = c.toOSPathMeta("newPotato")
	assertPathExist(t, osPath)
	assertPathExist(t, osPathMeta)
	assert.True(t, c.Exists("newPotato"))

	// rename newPotato -> sub/newPotato

	require.NoError(t, c.Rename("newPotato", "sub/newPotato", nil))
	assertPathNotExist(t, osPath)
	assertPathNotExist(t, osPathMeta)
	assert.False(t, c.Exists("potato"))

	osPath = c.toOSPath("sub/newPotato")
	osPathMeta = c.toOSPathMeta("sub/newPotato")
	assertPathExist(t, osPath)
	assertPathExist(t, osPathMeta)
	assert.True(t, c.Exists("sub/newPotato"))

	// remove

	c.Remove("sub/newPotato")
	assertPathNotExist(t, osPath)
	assertPathNotExist(t, osPathMeta)
	assert.False(t, c.Exists("sub/newPotato"))

	// nonexistent file - is ignored
	assert.NoError(t, c.Rename("nonexist", "nonexist2", nil))
}

func TestCacheCleaner(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = fs.Duration(10 * time.Millisecond)
	opt.CacheMaxAge = fs.Duration(20 * time.Millisecond)
	_, c := newTestCacheOpt(t, opt)

	time.Sleep(time.Duration(2 * opt.CachePollInterval))

	potato := c.Item("potato")
	potato2, found := c.get("potato")
	assert.Equal(t, fmt.Sprintf("%p", potato), fmt.Sprintf("%p", potato2))
	assert.True(t, found)

	for range 100 {
		time.Sleep(time.Duration(10 * opt.CachePollInterval))
		potato2, found = c.get("potato")
		if !found {
			break
		}
	}

	assert.NotEqual(t, fmt.Sprintf("%p", potato), fmt.Sprintf("%p", potato2))
	assert.False(t, found)
}

func TestCacheSetModTime(t *testing.T) {
	_, c := newTestCache(t)

	t1 := time.Date(2010, 1, 2, 3, 4, 5, 9, time.UTC)

	potato := c.Item("potato")
	require.NoError(t, potato.Open(nil))
	require.NoError(t, potato.Truncate(5))
	require.NoError(t, potato.Close(nil))

	c.SetModTime("potato", t1)
	osPath := potato.c.toOSPath("potato")
	fi, err := os.Stat(osPath)
	require.NoError(t, err)

	fstest.AssertTimeEqualWithPrecision(t, "potato", t1, fi.ModTime(), time.Second)
}

func TestCacheTotaInUse(t *testing.T) {
	_, c := newTestCache(t)

	assert.Equal(t, int(0), c.TotalInUse())

	potato := c.Item("potato")
	assert.Equal(t, int(0), c.TotalInUse())

	require.NoError(t, potato.Open(nil))
	assert.Equal(t, int(1), c.TotalInUse())

	require.NoError(t, potato.Truncate(5))
	assert.Equal(t, int(1), c.TotalInUse())

	potato2 := c.Item("potato2")
	assert.Equal(t, int(1), c.TotalInUse())

	require.NoError(t, potato2.Open(nil))
	assert.Equal(t, int(2), c.TotalInUse())

	require.NoError(t, potato2.Close(nil))
	assert.Equal(t, int(1), c.TotalInUse())

	require.NoError(t, potato.Close(nil))
	assert.Equal(t, int(0), c.TotalInUse())
}

func TestCacheDump(t *testing.T) {
	_, c := newTestCache(t)

	out := (*Cache)(nil).Dump()
	assert.Equal(t, "Cache: <nil>\n", out)

	out = c.Dump()
	assert.Equal(t, "Cache{\n}\n", out)

	c.Item("potato")

	out = c.Dump()
	want := "Cache{\n\t\"potato\": "
	assert.Equal(t, want, out[:len(want)])

	c.Remove("potato")

	out = c.Dump()
	assert.Equal(t, "Cache{\n}\n", out)
}

func TestCacheStats(t *testing.T) {
	_, c := newTestCache(t)

	out := c.Stats()
	assert.Equal(t, int64(0), out["bytesUsed"])
	assert.Equal(t, 0, out["erroredFiles"])
	assert.Equal(t, 0, out["files"])
	assert.Equal(t, 0, out["uploadsInProgress"])
	assert.Equal(t, 0, out["uploadsQueued"])
}

func TestCacheQueue(t *testing.T) {
	_, c := newTestCache(t)

	out := c.Queue()

	// We've checked the contents of queue in the writeback tests
	// Just check it is present here
	queue, found := out["queue"]
	require.True(t, found)
	_, ok := queue.([]writeback.QueueInfo)
	require.True(t, ok)
}

func TestCacheQueueSetExpiry(t *testing.T) {
	_, c := newTestCache(t)

	// Check this returns the correct error when called so we know
	// it is plumbed in correctly. The actual tests are done in
	// writeback.
	err := c.QueueSetExpiry(123123, time.Now(), 0)
	assert.Equal(t, writeback.ErrorIDNotFound, err)
}

// reloadCache makes a new Cache over the same remote - and so the same
// cache directory - as an existing one, which makes New load whatever
// is already on disk.
func reloadCache(t *testing.T, r *fstest.Run, opt vfscommon.Options) *Cache {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	resetAvInfos()
	c, err := New(ctx, r.Fremote, &opt, addVirtual)
	require.NoError(t, err)
	return c
}

// fill the cache with n items which each have a data and a metadata file
func fillCache(t *testing.T, c *Cache, n int) {
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("dir%02d/file%03d", i%7, i)
		item := c.Item(name)
		itemWrite(t, item, fmt.Sprintf("contents of %s", name))
		require.NoError(t, item.Close(nil))
	}
}

// reload used to create every item under c.mu, one at a time. Each one
// stats its data file and reads and decodes its metadata file, so on a
// cache directory with any latency - NFS, say - starting a large cache
// took minutes, with the mount unavailable throughout. The items are
// created concurrently and off c.mu now, so check that the parallel
// and the serial paths agree on what they load.
func TestCacheReloadConcurrency(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 100
	fillCache(t, c, items)
	want := itemAsString(c)
	require.Len(t, want, items)

	// Loading the same cache directory again must find exactly the
	// same items however many workers do it.
	for _, concurrency := range []int{0, 1, 2, 64} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			opt := opt
			opt.CacheReloadConcurrency = concurrency
			got := itemAsString(reloadCache(t, r, opt))
			assert.Equal(t, want, got)
		})
	}
}

// markDirty flips the Dirty flag in an item's metadata file on disk,
// so a later reload sees it as needing writing back.
func markDirty(t *testing.T, c *Cache, name string) {
	osPathMeta := c.toOSPathMeta(name)
	buf, err := os.ReadFile(osPathMeta)
	require.NoError(t, err)
	var info Info
	require.NoError(t, json.Unmarshal(buf, &info))
	info.Dirty = true
	buf, err = json.Marshal(&info)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(osPathMeta, buf, 0600))
}

// reload creates the items concurrently but then hands them to
// item.reload serially, in walk order, so that writing back the dirty
// ones is sequenced exactly as it was before. item.reload calls
// AddVirtual for each dirty item, so check those arrive in order.
func TestCacheReloadDirtyOrder(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 50
	fillCache(t, c, items)

	// Dirty a scattered handful so walk order and creation order differ
	var dirty []string
	for i := 0; i < items; i += 6 {
		name := fmt.Sprintf("dir%02d/file%03d", i%7, i)
		markDirty(t, c, name)
		dirty = append(dirty, name)
	}
	sort.Strings(dirty)
	require.Greater(t, len(dirty), 1)

	opt.CacheReloadConcurrency = 64
	reloadCache(t, r, opt)

	assert.Equal(t, dirty, getAvInfos())
}

// With --vfs-cache-reload-async the cache scan runs in the background, so New
// returns before it has finished. An item the scan has not reached yet must
// still be usable, because Cache.Item builds it from what is on disk.
func TestCacheReloadAsync(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 50
	fillCache(t, c, items)
	want := itemAsString(c)
	require.Len(t, want, items)

	opt.CacheReloadAsync = true
	opt.ReadOnly = true // async loading is only allowed read-only
	async := reloadCache(t, r, opt)

	// Usable before the scan has necessarily reached it. Check the contents,
	// not that Item returned something - Item never returns nil. Open with
	// the real object: Open(nil) means "no remote object" and truncates.
	const name = "dir00/file000"
	obj, err := r.Fremote.NewObject(context.Background(), name)
	require.NoError(t, err)
	item := async.Item(name)
	require.NoError(t, item.Open(obj))
	buf := make([]byte, 64)
	n, err := item.ReadAt(buf, 0)
	require.NoError(t, item.Close(nil))
	if err != nil && err != io.EOF { // short read at EOF is expected
		require.NoError(t, err)
	}
	assert.Equal(t, "contents of "+name, string(buf[:n]))

	// and the scan converges on the same set the synchronous path finds
	for range 100 {
		if len(itemAsString(async)) == items {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equal(t, want, itemAsString(async))
}

// A cache left dirty by an earlier read-write session is not written back by
// the async scan - item.reload would race the live mount's handles - but it
// must not be dropped silently either, and the item must still be present.
func TestCacheReloadAsyncDirtyNotSilentlyDropped(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 20
	fillCache(t, c, items)
	name := "dir00/file000"
	markDirty(t, c, name)

	opt.CacheReloadAsync = true
	opt.ReadOnly = true
	async := reloadCache(t, r, opt)

	for range 100 {
		if len(itemAsString(async)) == items {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// the scan found it ...
	assert.Len(t, itemAsString(async), items)
	// ... and deliberately did not drive writeback from a live mount
	assert.Empty(t, getAvInfos(), "async reload must not write back dirty items")
	assert.Equal(t, int64(1), async.DirtyNotWrittenBack(), "the dirty item must be reported")
	_ = name
}

// The dangerous shape of the above: the live mount probes the path before the
// scan reaches it. Cache.Exists inserts into c.item, so the scan then finds
// the name already present - and must still notice it is dirty rather than
// passing over it in silence, which is the whole point of the warning.
func TestCacheReloadAsyncDirtyProbedFirst(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 20
	fillCache(t, c, items)
	const name = "dir00/file000"
	markDirty(t, c, name)

	// Build the cache by hand so we can probe before reload runs, exactly as
	// a live mount would.
	opt.CacheReloadAsync = true
	opt.ReadOnly = true
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	resetAvInfos()

	probed, err := New(ctx, r.Fremote, &opt, addVirtual)
	require.NoError(t, err)
	// race the scan: whether we win or lose, the item must not be passed over
	// silently - if we win, reload sees it already in c.item
	probed.Exists(name)

	for range 100 {
		if len(itemAsString(probed)) == items {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	item := probed.Item(name)
	assert.True(t, item.IsDirty(), "the item must still be known to be dirty")
	assert.Empty(t, getAvInfos(), "async reload must not write back dirty items")
	// and crucially it was reported rather than passed over in silence
	assert.Equal(t, int64(1), probed.DirtyNotWrittenBack(),
		"a dirty item the VFS probed first must still be reported")
}

// The cleaner unlinks cache files while an async scan is walking, so an entry
// that disappears between Readdirnames and Lstat must not fail the scan.
func TestCacheReloadWalkToleratesVanishedEntry(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	_, c := newTestCacheOpt(t, opt)

	dir := filepath.Join(c.root, "flat")
	require.NoError(t, os.MkdirAll(dir, 0700))
	const items = 200
	for i := range items {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", i)), nil, 0600))
	}

	// Delete entries behind the walk, the way the cleaner would.
	var seen atomic.Int64
	err := c.walk(context.Background(), c.root, 1, func(osPath string, fi os.FileInfo, name string) error {
		if fi.IsDir() {
			return nil
		}
		if seen.Add(1) == 1 {
			// remove a batch the walk has listed but not yet stat'ed
			for i := items - 50; i < items; i++ {
				_ = os.Remove(filepath.Join(dir, fmt.Sprintf("f%05d", i)))
			}
		}
		return nil
	})
	require.NoError(t, err, "a vanished entry must not fail the walk")
	assert.Greater(t, seen.Load(), int64(100), "the walk should have carried on")
}

// The background scan outlives New, so cancelling the cache context has to
// stop it part-way through - otherwise it keeps walking a cache that has been
// shut down.
//
// The entries are deliberately in one flat directory, because that is the
// shape that matters: the cache this was written for holds >1M entries in a
// single directory, and there the per-entry check is the only thing that stops
// the walk promptly. The per-directory check never fires again, and the
// per-batch check only every Readdirnames batch.
func TestCacheReloadCancelMidWalk(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	_, c := newTestCacheOpt(t, opt)

	const items = 4000 // several Readdirnames batches worth
	dir := filepath.Join(c.root, "flat")
	require.NoError(t, os.MkdirAll(dir, 0700))
	for i := range items {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", i)), nil, 0600))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seen atomic.Int64
	err := c.walk(ctx, c.root, 4, func(osPath string, fi os.FileInfo, name string) error {
		if seen.Add(1) == 10 {
			cancel() // cancel once we are genuinely under way
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)

	n := seen.Load()
	assert.Greater(t, n, int64(0), "walk should have started")
	// Without a per-entry check the whole first batch is dispatched before
	// anything notices, so this would be >= 1024.
	assert.Less(t, n, int64(512), "walk should have stopped promptly, visited %d of %d", n, items)
}

// A failing walk must not cost us the writeback of the items it did find.
// reload used to return as soon as walk did, and because walk carries on past
// an error it could populate the whole of c.item and then reload none of it -
// worse than the inline filepath.Walk it replaced.
func TestCacheReloadWalkErrorStillReloads(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	r, c := newTestCacheOpt(t, opt)

	const items = 20
	fillCache(t, c, items)
	var dirty []string
	for i := 0; i < items; i += 4 {
		name := fmt.Sprintf("dir%02d/file%03d", i%7, i)
		markDirty(t, c, name)
		dirty = append(dirty, name)
	}
	sort.Strings(dirty)
	require.NotEmpty(t, dirty)

	// An unreadable directory makes the walk fail partway through.
	bad := filepath.Join(c.root, "unreadable")
	require.NoError(t, os.MkdirAll(bad, 0700))
	require.NoError(t, os.Chmod(bad, 0000))
	t.Cleanup(func() { _ = os.Chmod(bad, 0700) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	resetAvInfos()
	opt.CacheReloadConcurrency = 8
	failed, err := New(ctx, r.Fremote, &opt, addVirtual)
	if err == nil {
		t.Skip("walk did not fail - running as a user that can read 0000 dirs")
	}
	assert.Nil(t, failed)

	// Every dirty item the walk reached must still have been written back.
	got := getAvInfos()
	sort.Strings(got)
	assert.Equal(t, dirty, got)
}

// walk returns immediately when handed a context that is already cancelled.
// This only covers the guard before the root is read - cancellation part-way
// through a walk is TestCacheReloadCancelMidWalk.
func TestCacheReloadCancelAtStart(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	_, c := newTestCacheOpt(t, opt)
	fillCache(t, c, 20)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the walk starts

	err := c.walk(ctx, c.root, 4, func(osPath string, fi os.FileInfo, name string) error {
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

// newItem deletes what looks inconsistent: a data file with no metadata is
// treated as an orphan. On a live mount that shape is also what Item.open
// leaves behind between writing the data file and writing its metadata, so an
// async scan must not tidy it away - it would destroy a cache file in use.
// A synchronous load runs before anything is mounted, so it still cleans up.
func TestCacheReloadAsyncDoesNotTidyLiveItems(t *testing.T) {
	for _, test := range []struct {
		name        string
		async       bool
		wantRemoved bool
	}{
		{name: "sync tidies orphans", async: false, wantRemoved: true},
		{name: "async leaves them alone", async: true, wantRemoved: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			opt := vfscommon.Opt
			opt.CachePollInterval = 0
			opt.WriteBack = 0
			opt.HandleCaching = 0

			r, c := newTestCacheOpt(t, opt)
			fillCache(t, c, 5)

			// a data file whose metadata has not been written yet
			const name = "dir00/file000"
			require.NoError(t, os.Remove(c.toOSPathMeta(name)))
			dataPath := c.toOSPath(name)
			_, err := os.Stat(dataPath)
			require.NoError(t, err, "data file should still be there")

			opt.CacheReloadAsync = test.async
			opt.ReadOnly = test.async // async is only allowed read-only
			reloaded := reloadCache(t, r, opt)
			for range 100 {
				if len(itemAsString(reloaded)) >= 4 {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}

			_, err = os.Stat(dataPath)
			if test.wantRemoved {
				assert.True(t, os.IsNotExist(err), "sync load should have removed the orphan, got %v", err)
			} else {
				assert.NoError(t, err, "async load must not delete a data file that may be mid-create")
			}
		})
	}
}

// countOpenFDs returns how many file descriptors this process has open.
func countOpenFDs(t *testing.T) int {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd on this platform")
	}
	return len(ents)
}

// walk must not keep a directory open while descending into it. When the
// worker pool is saturated the walk runs inline, so holding the fd would nest
// one per level of the tree, and a deep tree would exhaust the fd table -
// which on a synchronous load fails New and leaves the mount with no cache.
func TestCacheReloadWalkDoesNotNestDirFDs(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = 0
	opt.HandleCaching = 0

	_, c := newTestCacheOpt(t, opt)

	// one deep chain, so every level is entered while the previous is open
	const depth = 300
	dir := c.root
	for i := range depth {
		dir = filepath.Join(dir, fmt.Sprintf("d%03d", i))
	}
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "leaf"), nil, 0600))

	base := countOpenFDs(t)
	var peak atomic.Int64
	err := c.walk(context.Background(), c.root, 1, func(osPath string, fi os.FileInfo, name string) error {
		if n := int64(countOpenFDs(t) - base); n > peak.Load() {
			peak.Store(n)
		}
		return nil
	})
	require.NoError(t, err)
	// Holding the fd open across the descent gives roughly one per level.
	assert.Less(t, peak.Load(), int64(depth/4),
		"walk held %d extra fds descending %d levels", peak.Load(), depth)
}
