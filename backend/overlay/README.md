# overlay backend

A read-only wrapping backend: it **lists/stats from one remote** (`list_remote`) and
**reads object data from another** (`read_remote`).

## Why

When you `rclone mount` a Cloudflare R2 bucket that has **Sippy** enabled (lazy
migration from a GCS/S3 origin), `ls` only shows objects already migrated into R2.
R2's `ListObjects` returns only what is physically in R2, and Sippy copies **only on
`GetObject`** — never on `List` or `Head`. So objects still living only in the origin
are invisible and therefore never get read, so they never migrate.

`overlay` fixes this by enumerating from the **origin** (the authoritative full
keyspace) while routing every read through **R2** (`GetObject` → triggers Sippy's
copy-on-read). Net effect when mounted: `ls` shows every object; opening one migrates
it; subsequent reads come straight from R2.

```
            ┌─────────────── overlay (read-only) ───────────────┐
 ls / stat ─┤ List / ListR / NewObject ──► list_remote  (origin: full keyspace)
 open/read ─┤ Object.Open ──────────────► read_remote  (R2 + Sippy: GetObject → migrate)
            └────────────────────────────────────────────────────┘
```

It is not specific to R2/Sippy — any "list from A, read from B" use case works (the read
path simply has to address the same keys as the list path).

## Build

Requires Go ≥ the version in `go.mod`.

```sh
go build -o rclone .
rclone help backends | grep overlay   # confirm it's registered
```

## Configure

`overlay` wraps two existing remotes. For the R2 + Sippy case the list remote is the
GCS/S3 origin (reachable with the same credentials Sippy uses) and the read remote is
the R2 bucket:

```ini
[gcs]
type = google cloud storage
service_account_file = /path/to/origin-sa.json

[r2]
type = s3
provider = Cloudflare
access_key_id = <R2_ACCESS_KEY_ID>
secret_access_key = <R2_SECRET_ACCESS_KEY>
endpoint = https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com
acl = private
no_check_bucket = true

[overlay]
type = overlay
list_remote = gcs:<origin-bucket>     ; authoritative keyspace (Sippy origin)
read_remote = r2:<r2-bucket>          ; reads → GetObject → Sippy migration
```

Both upstreams must address the **same keys** (same bucket layout); `overlay` does no
path translation. Any sub-path on the overlay (e.g. `overlay:bucket/prefix`) is appended
to both upstreams, so you can also leave the buckets off the config and supply them via
the mount path when the origin and read buckets share a name:

```ini
[overlay]
type = overlay
list_remote = gcs:
read_remote = r2:
```
…then mount `overlay:my-bucket` to get `gcs:my-bucket` (list) + `r2:my-bucket` (read).

## Mount

```sh
rclone mount overlay: /mnt/point \
  --read-only --dir-cache-time 24h --no-modtime \
  --vfs-cache-mode full
```

- `--read-only` — the backend refuses writes anyway (`Put`/`Mkdir`/`Update`/`Remove`
  return permission denied), but set this so the VFS never tries.
- `--dir-cache-time 24h` — the origin keyspace is stable; a key migrating into R2 does
  not change the (origin-backed) listing, which is the desired behaviour.
- `--no-modtime` — modtime comes from the list remote anyway.

## Performance

- **Cold open = one `HeadObject` + one `GetObject`** against the read remote (the HEAD
  resolves the read object; with Sippy it proxies origin metadata so it succeeds for
  not-yet-migrated keys). Re-opens of the same in-memory object skip the HEAD. The HEAD
  runs outside any lock, so concurrent opens of different (or the same) files scale.
- **Do NOT set `no_head_object = true` on the read remote** to skip that HEAD. The s3
  backend then leaves the object size at 0, and its `Open` runs
  `fs.FixRangeOption(options, 0)` which **silently strips all Range options** — every
  ranged read (which is how the VFS reads) fetches from offset 0 and returns wrong data.
- **Stat (`NewObject`) is served by the list remote.** With a `gcs` list remote in
  `bigquery_table` mode, stats are answered from the local bbolt cache with no network
  call; refresh the cache after inventory updates with
  `rclone rc backend/command command=refresh fs=gcs:<bucket>` against the running mount
  (needs `--rc`), or `rclone backend refresh gcs:<bucket>` when nothing holds the cache
  file lock.

## Behaviour / limitations

- **Read-only.** All mutations return `permission denied`.
- **Listing & metadata** (size, modtime, hash, mime type) come from `list_remote`.
- **Data** comes from `read_remote`. With R2 + Sippy, `HeadObject` (used to resolve the
  read object) proxies origin metadata and returns 200 even for not-yet-migrated keys,
  so opens succeed and trigger migration. Against a plain remote with no such proxy, a
  key present only in `list_remote` will fail to open with "object not found".
- **ETags/hashes** may differ between origin and Sippy-migrated R2 objects; this is
  cosmetic for a mount (the VFS invalidates on size + modtime).
- Newly written objects are not supported, so there's no write-visibility asymmetry.

## Verify

```sh
rclone lsf -R overlay: | head            # full origin keyspace, incl. non-migrated keys
rclone cat overlay:<not-yet-migrated-key> >/dev/null   # triggers Sippy
rclone lsjson r2:<bucket>/<that-key> --stat            # now present in R2
```
