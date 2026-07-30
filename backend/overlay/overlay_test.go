package overlay

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
)

// TestConcurrentOpens guards readObject's double-checked locking: the read
// object is resolved outside o.mu, so many concurrent first opens of the same
// object must race safely and all read correct data. Run under -race.
func TestConcurrentOpens(t *testing.T) {
	ctx := context.Background()

	// the memory backend shares one in-process store, so writing via a direct
	// handle makes the object visible to both overlay upstreams
	mem, err := fs.NewFs(ctx, ":memory:overlay-test-bucket")
	if err != nil {
		t.Fatalf("memory NewFs: %v", err)
	}
	content := []byte("hello through the overlay")
	src := object.NewStaticObjectInfo("dir/one.bin", time.Now(), int64(len(content)), true, nil, nil)
	if _, err := mem.Put(ctx, bytes.NewReader(content), src); err != nil {
		t.Fatalf("put: %v", err)
	}

	f, err := NewFs(ctx, "ovl", "", configmap.Simple{
		"list_remote": ":memory:overlay-test-bucket",
		"read_remote": ":memory:overlay-test-bucket",
	})
	if err != nil {
		t.Fatalf("overlay NewFs: %v", err)
	}

	o, err := f.NewObject(ctx, "dir/one.bin")
	if err != nil {
		t.Fatalf("NewObject: %v", err)
	}

	const N = 16
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rc, err := o.Open(ctx)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = rc.Close() }()
			got, err := io.ReadAll(rc)
			if err != nil {
				errs[i] = err
				return
			}
			if !bytes.Equal(got, content) {
				errs[i] = io.ErrUnexpectedEOF
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("reader %d: %v", i, err)
		}
	}
}
