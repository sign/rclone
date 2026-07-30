// Package overlay provides a read-only wrapping Fs that enumerates one remote
// (list_remote) but reads object data from another (read_remote).
//
// It exists to make `rclone mount` of a Cloudflare R2 bucket with Sippy enabled
// show the full keyspace. R2's ListObjects only returns objects already migrated
// into R2, and Sippy only copies on GetObject (never on List or Head). So we list
// from the GCS origin (authoritative keyspace) and route every read through R2,
// which triggers Sippy's copy-on-read for not-yet-migrated objects.
package overlay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "overlay",
		Description: "Read-only overlay: list from one remote, read object data from another (e.g. list a GCS origin, read R2+Sippy)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "list_remote",
			Required: true,
			Help: `Remote used for directory listings and object metadata, e.g. "gcs:bucket/path".

This is the authoritative keyspace - normally the Sippy origin bucket. Both
List/ListR and NewObject (stat) are served from here, so objects that have not
yet been migrated into the read remote are still visible.`,
		}, {
			Name:     "read_remote",
			Required: true,
			Help: `Remote used to read object data, e.g. "r2:bucket/path".

Opening a file issues a GetObject against this remote. With Cloudflare R2 + Sippy
that triggers a copy-on-read migration from the origin. Must address the same
keys as list_remote (same bucket layout).`,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	ListRemote string `config:"list_remote"` // authoritative keyspace + metadata (e.g. GCS origin)
	ReadRemote string `config:"read_remote"` // object reads → GetObject (e.g. R2 with Sippy)
}

// Fs represents a read-only overlay of two upstream remotes
type Fs struct {
	name     string
	root     string
	opt      Options
	listFs   fs.Fs // enumerated for List/ListR/NewObject
	readFs   fs.Fs // read through for object data (Open)
	features *fs.Features
}

// resolveUpstream opens an upstream remote rooted at the same sub-path as the
// overlay, so a key listed from list_remote addresses the identical key in
// read_remote with no path translation.
func resolveUpstream(ctx context.Context, remote, root string) (fs.Fs, error) {
	target := remote
	if root != "" {
		target = fspath.JoinRootPath(remote, root)
	}
	return cache.Get(ctx, target)
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.ListRemote == "" || opt.ReadRemote == "" {
		return nil, errors.New("overlay: both list_remote and read_remote must be set")
	}
	// reject pointing either upstream back at this overlay, which would recurse forever
	for _, remote := range []string{opt.ListRemote, opt.ReadRemote} {
		if strings.HasPrefix(remote, name+":") {
			return nil, errors.New("overlay: list_remote/read_remote can't point at the overlay itself")
		}
	}

	// open the authoritative list upstream first; fs.ErrorIsFile is not fatal (root is a file)
	listFs, errList := resolveUpstream(ctx, opt.ListRemote, root)
	if errList != nil && errList != fs.ErrorIsFile {
		return nil, fmt.Errorf("overlay: failed to open list_remote %q: %w", opt.ListRemote, errList)
	}

	// when the list root is a file the backend rooted listFs at the parent dir and returned
	// ErrorIsFile (mirrors crypt). correct our root and open read_remote at the SAME parent so
	// keys line up - opening read_remote at the original file path is wrong unless it also
	// returns ErrorIsFile (a plain read remote with the key missing roots one level too deep,
	// so readFs.NewObject would look up <key>/<key>).
	if errList == fs.ErrorIsFile {
		root = path.Dir(root)
		if root == "." || root == "/" {
			root = ""
		}
	}

	readFs, errRead := resolveUpstream(ctx, opt.ReadRemote, root)
	if errRead != nil && errRead != fs.ErrorIsFile {
		return nil, fmt.Errorf("overlay: failed to open read_remote %q: %w", opt.ReadRemote, errRead)
	}

	f := &Fs{
		name:   name,
		root:   root,
		opt:    *opt,
		listFs: listFs,
		readFs: readFs,
	}
	// keep both upstreams alive in the fs cache for as long as the overlay is referenced.
	// cache.PinUntilFinalized sets a single finalizer per object, so we can't call it twice
	// for the same overlay - pin both and release them together in one finalizer.
	cache.Pin(f.listFs)
	cache.Pin(f.readFs)
	runtime.SetFinalizer(f, func(f *Fs) {
		cache.Unpin(f.listFs)
		cache.Unpin(f.readFs)
	})

	f.features = (&fs.Features{
		ReadMimeType: true,
		BucketBased:  true,
	}).Fill(ctx, f)

	// only advertise recursive listing when the list upstream can do it efficiently
	if f.listFs.Features().ListR == nil {
		f.features.ListR = nil
	}

	return f, errList
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("overlay (list %q, read %q)", f.opt.ListRemote, f.opt.ReadRemote)
}

// Precision of the ModTimes in this Fs, taken from the authoritative list remote
func (f *Fs) Precision() time.Duration { return f.listFs.Precision() }

// Hashes returns the supported hash sets, taken from the authoritative list remote
func (f *Fs) Hashes() hash.Set { return f.listFs.Hashes() }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// wrapEntries replaces objects with overlay objects so their reads route to the
// read remote. Directories use identity name mapping, so they pass through.
func (f *Fs) wrapEntries(entries fs.DirEntries) fs.DirEntries {
	for i, entry := range entries {
		if o, ok := entry.(fs.Object); ok {
			entries[i] = f.wrapObject(o)
		}
	}
	return entries
}

// List the objects and directories in dir into entries, served from the list remote.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.listFs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	return f.wrapEntries(entries), nil
}

// ListR lists dir recursively, served from the list remote (when it supports ListR).
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	listR := f.listFs.Features().ListR
	if listR == nil {
		return fs.ErrorNotImplemented
	}
	return listR(ctx, dir, func(entries fs.DirEntries) error {
		return callback(f.wrapEntries(entries))
	})
}

// NewObject finds the Object at remote, with metadata from the list remote so
// that not-yet-migrated keys still stat successfully.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.listFs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return f.wrapObject(o), nil
}

// Put is unsupported - this backend is read-only.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}

// Mkdir is unsupported - this backend is read-only.
func (f *Fs) Mkdir(ctx context.Context, dir string) error { return fs.ErrorPermissionDenied }

// Rmdir is unsupported - this backend is read-only.
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return fs.ErrorPermissionDenied }

// Object wraps a list-remote object for metadata while reading data from the
// read remote.
type Object struct {
	fs.Object            // list-remote object: authoritative Remote/Size/ModTime/Hash/Storable
	f         *Fs        //
	mu        sync.Mutex // guards readObj
	readObj   fs.Object  // read-remote object, resolved lazily on first Open and cached
}

func (f *Fs) wrapObject(o fs.Object) *Object {
	return &Object{Object: o, f: f}
}

// Fs returns the overlay Fs this object belongs to (not the list remote's Fs).
func (o *Object) Fs() fs.Info { return o.f }

// String returns the remote path
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Object.Remote()
}

// readObject resolves and caches the read-remote object for this key.
//
// The read-remote NewObject is a HeadObject. With R2 + Sippy enabled this proxies
// origin metadata and returns 200 even for keys not yet migrated, so an error here
// means the key is genuinely absent.
//
// The HEAD runs outside o.mu: holding a mutex across a network round-trip
// serialises concurrent first opens of the same object. A rare duplicate HEAD
// on a racing first open is cheaper than that.
func (o *Object) readObject(ctx context.Context) (fs.Object, error) {
	o.mu.Lock()
	ro := o.readObj
	o.mu.Unlock()
	if ro != nil {
		return ro, nil
	}
	ro, err := o.f.readFs.NewObject(ctx, o.Object.Remote())
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.readObj == nil {
		o.readObj = ro
	} else {
		ro = o.readObj // keep the racer's winner so all handles share one object
	}
	o.mu.Unlock()
	return ro, nil
}

// Open opens the file for read against the read remote. Range/Seek options are
// forwarded verbatim, so a ranged GetObject still drives Sippy's copy-on-read
// (large objects migrate over multiple requests).
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	ro, err := o.readObject(ctx)
	if err != nil {
		return nil, err
	}
	return ro.Open(ctx, options...)
}

// MimeType forwards the content type from the list remote when available.
func (o *Object) MimeType(ctx context.Context) string {
	if do, ok := o.Object.(fs.MimeTyper); ok {
		return do.MimeType(ctx)
	}
	return ""
}

// SetModTime is unsupported - this backend is read-only.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error { return fs.ErrorPermissionDenied }

// Update is unsupported - this backend is read-only.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return fs.ErrorPermissionDenied
}

// Remove is unsupported - this backend is read-only.
func (o *Object) Remove(ctx context.Context) error { return fs.ErrorPermissionDenied }

// Check the interfaces are satisfied
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.ListRer   = (*Fs)(nil)
	_ fs.Object    = (*Object)(nil)
	_ fs.MimeTyper = (*Object)(nil)
)
