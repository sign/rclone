package vfscommon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Loading the cache in the background is only safe when nothing can dirty an
// item, so Init refuses the flag on a writable mount. It has to be refused
// here and not by failing vfscache.New: SetCacheMode turns that error into
// "disable the cache and carry on", which would silently leave a mount with
// no cache at all.
func TestOptionsInitCacheReloadAsyncNeedsReadOnly(t *testing.T) {
	ctx := context.Background()

	opt := Opt
	opt.CacheReloadAsync = true
	opt.ReadOnly = false
	opt.Init(ctx)
	assert.False(t, opt.CacheReloadAsync, "async loading must be refused on a writable mount")

	opt = Opt
	opt.CacheReloadAsync = true
	opt.ReadOnly = true
	opt.Init(ctx)
	assert.True(t, opt.CacheReloadAsync, "async loading is allowed read-only")
}

func TestOptionsInitCacheReloadConcurrency(t *testing.T) {
	ctx := context.Background()

	for _, test := range []struct {
		in, want int
	}{
		{in: -1, want: 1},
		{in: 0, want: 1},
		{in: 16, want: 16},
		{in: maxCacheReloadConcurrency, want: maxCacheReloadConcurrency},
		{in: maxCacheReloadConcurrency * 10, want: maxCacheReloadConcurrency},
	} {
		opt := Opt
		opt.CacheReloadConcurrency = test.in
		opt.Init(ctx)
		assert.Equal(t, test.want, opt.CacheReloadConcurrency, "in=%d", test.in)
	}
}
