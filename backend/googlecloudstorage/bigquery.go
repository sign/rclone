package googlecloudstorage

// BigQuery-backed listing: when bigquery_table is set, List/ListR are served by
// querying a GCS Storage Insights inventory report table instead of the Cloud
// Storage list API. Both modes feed the same listFn callback as the API path, so
// itemToDirEntry/newObjectWithInfo/setMetaData are reused unchanged.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/rclone/rclone/fs"
	"google.golang.org/api/iterator"
	storage "google.golang.org/api/storage/v1"
)

// bqAccelMinRows gates the Storage Read API check in bqQueryRows. When the read
// session can't be created (API not enabled, missing
// bigquery.readsessions.create) the client silently falls back to serial REST
// paging at ~17-18k rows/s - a 9.26M-row populate would take ~47min, so a big
// download hard-fails with the missing grant instead. Results this small are
// still seconds over REST, so tiny corpuses (and test tables) work unaccelerated.
const bqAccelMinRows = 200000

// bqProgressEvery is how often (in rows) the download loop logs progress.
const bqProgressEvery = 500000

// bqTableRe guards the table reference before it is interpolated into SQL (BigQuery
// can't bind table names as query parameters). project ids allow dashes; dataset
// and table names are alphanumeric + underscore; the parts are dot-separated.
var bqTableRe = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+){1,2}$`)

// resolveBQProject picks the project that runs (and is billed for) the inventory
// query: the explicit option, else the project component of a fully-qualified
// table, else the service account credentials' project_id.
func resolveBQProject(opt *Options) (string, error) {
	if !bqTableRe.MatchString(opt.BigQueryTable) {
		return "", fmt.Errorf("invalid bigquery_table %q: want project.dataset.table or dataset.table", opt.BigQueryTable)
	}
	if opt.BigQueryBillingProject != "" {
		return opt.BigQueryBillingProject, nil
	}
	if parts := strings.Split(opt.BigQueryTable, "."); len(parts) == 3 {
		return parts[0], nil
	}
	// fall back to the service account's own project
	if opt.ServiceAccountCredentials != "" {
		var sa struct {
			ProjectID string `json:"project_id"`
		}
		if err := json.Unmarshal([]byte(opt.ServiceAccountCredentials), &sa); err == nil && sa.ProjectID != "" {
			return sa.ProjectID, nil
		}
	}
	return "", errors.New("can't determine BigQuery billing project: set bigquery_billing_project or use a fully-qualified project.dataset.table")
}

// bqListQuery builds the GoogleSQL listing query and its named parameters.
//
// updated is formatted to RFC3339Nano so it drops straight into
// storage.Object.Updated; md5Hash is converted from the inventory's hex to base64
// so both are parsed by setMetaData unchanged (it base64-decodes the hash and
// RFC3339-parses the time).
//
// @dir is always read through IFNULL(@dir, ”): an empty named STRING parameter
// has historically been easy to bind as NULL (REST omitempty dropped the value),
// and NULL would make STARTS_WITH match nothing at the root - IFNULL makes the
// query immune to how the client library serializes "".
//
// Recursive lists every object under @dir. Single-level returns objects directly in
// @dir plus a synthesized row per immediate sub-directory (name ending in "/", which
// objectRemote then recognises as a directory) - the SQL equivalent of the list
// API's "/" delimiter common-prefixes.
func (f *Fs) bqListQuery(bucketName, directory string, recurse bool) (string, []bq.QueryParameter) {
	tbl := f.opt.BigQueryTable
	params := []bq.QueryParameter{
		{Name: "bucket", Value: bucketName},
		{Name: "dir", Value: directory},
	}
	const ts = "FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated)"
	const md5 = "TO_BASE64(FROM_HEX(md5Hash))" // Storage Insights stores md5Hash as hex; setMetaData wants base64
	const dir = "IFNULL(@dir, '')"

	if recurse {
		// The populate is a bare full-table read, which leans on two documented
		// requirements of bigquery_table (see its option help): the table holds
		// exactly one bucket, and exactly one row per object. That is why there is
		// no "bucket = @bucket" filter - cache keys are the object name with no
		// bucket component, so a multi-bucket table would list one bucket's
		// objects under another - and no newest-per-name QUALIFY window.
		//
		// There is also no STARTS_WITH(name, @dir) filter: a prefixed remote root
		// therefore caches the whole bucket. That stays correct (bqServe seeks to
		// the directory and walks only that prefix) and just wastes cache space.
		//
		// ORDER BY name is load-bearing and paid for on purpose. It costs job time
		// (a global sort without LIMIT funnels through a single BigQuery worker)
		// and read parallelism (the client sniffs a top-level ORDER BY and drops
		// the read session to one stream). Both are worth it because neither was
		// ever the bottleneck: the populate is bound by bbolt insertion, and bbolt
		// is a copy-on-write B+tree, so random-order keys rewrite ~one leaf page
		// per row and get slower as the tree grows - measured decaying from 74k
		// rows/s to 13k rows/s across a 9.26M-row load, and 43x slower than sorted
		// input at 1M rows in BenchmarkBQPopulateKeyOrder. Sorted keys land
		// consecutive rows in the same leaf.
		sql := fmt.Sprintf(
			"SELECT name, size, %s AS md5Hash, %s AS updated FROM `%s` ORDER BY name",
			md5, ts, tbl)
		// no parameters: the query references neither @bucket nor @dir
		return sql, nil
	}

	sql := fmt.Sprintf(
		"WITH objs AS ("+
			"SELECT name, size, %s AS md5Hash, %s AS updated, SUBSTR(name, LENGTH(%s)+1) AS rel "+
			"FROM `%s` WHERE bucket = @bucket AND STARTS_WITH(name, %s)) "+
			"SELECT name, size, md5Hash, updated FROM objs WHERE STRPOS(rel, '/') = 0 "+
			"UNION DISTINCT "+
			"SELECT CONCAT(%s, REGEXP_EXTRACT(rel, r'^[^/]+/')), CAST(0 AS INT64), CAST(NULL AS STRING), CAST(NULL AS STRING) "+
			"FROM objs WHERE STRPOS(rel, '/') > 0",
		md5, ts, dir, tbl, dir, dir)
	return sql, params
}

// valueString reads a BigQuery result value as a string (size arrives as int64;
// FORMAT_TIMESTAMP/TO_BASE64 columns as string); NULL values yield "".
func valueString(v bq.Value) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(v)
	}
}

// bqRow is one inventory row as returned by BigQuery: raw cell strings, before
// any path mapping. size/md5/updated are unused (and typically empty) for
// directory-marker rows.
type bqRow struct {
	name    string
	size    string
	md5     string
	updated string
}

// bqQueryRows runs the listing query for (bucketName, directory) and invokes fn
// for each row. It is the BigQuery half of listing, driven by the cache populate
// in bqcache.go. Results are downloaded through the BigQuery Storage Read API
// (parallel streams, Arrow decode inside the client library) - getQueryResults
// paging capped per-job throughput at ~17-18k rows/s, which made big populates
// take minutes even fetched 8 pages at a time.
func (f *Fs) bqQueryRows(ctx context.Context, bucketName, directory string, recurse bool, fn func(bqRow) error) error {
	// Money guard: this is the only function that spends BigQuery, and the sole
	// legitimate query is a root-level recursive populate (bigquery_table always
	// runs with the cache). A per-directory query here would reopen the storm the
	// cache exists to prevent, so refuse it rather than execute it.
	if !recurse || directory != f.bqRemoteRoot() {
		return fmt.Errorf("googlecloudstorage: internal error: a BigQuery listing query must be a root recursive populate, got directory=%q recurse=%v", directory, recurse)
	}
	// bound the whole populate (job + result download) so a slow query can't
	// hold bqPopMu forever; the old snapshot keeps being served on timeout
	if t := time.Duration(f.opt.BigQueryTimeout); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	fs.Infof(f, "BigQuery listing cache: running BigQuery query to populate %q (dir=%q, recurse=%v)", bucketName, directory, recurse)
	sql, params := f.bqListQuery(bucketName, directory, recurse)
	q := f.bqClient.Query(sql)
	q.Parameters = params

	start := time.Now()
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting BigQuery listing query: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for BigQuery listing query: %w", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("BigQuery listing query failed: %w", err)
	}
	// marks where query execution ends and result download begins
	fs.Infof(f, "BigQuery listing cache: query finished in %v", time.Since(start).Round(time.Millisecond))

	dlStart := time.Now()
	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("reading BigQuery listing results: %w", err)
	}
	var rows uint64
	for {
		var row []bq.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("downloading BigQuery listing results: %w", err)
		}
		if rows == 0 {
			// TotalRows and IsAccelerated are only settled once iteration has
			// started, so the acceleration check lives after the first Next
			if it.IsAccelerated() {
				fs.Infof(f, "BigQuery listing cache: downloading %d rows via the Storage Read API", it.TotalRows)
			} else if it.TotalRows > bqAccelMinRows {
				return fmt.Errorf("BigQuery Storage Read API unavailable for a %d-row listing download (the REST fallback pages serially at ~17k rows/s): enable the BigQuery Storage Read API on project %q and grant roles/bigquery.readSessionUser to the credentials", it.TotalRows, f.bqProject)
			} else {
				fs.Logf(f, "BigQuery listing cache: Storage Read API unavailable, downloading %d rows via REST (grant roles/bigquery.readSessionUser to accelerate)", it.TotalRows)
			}
		}
		rows++
		if len(row) < 4 {
			continue
		}
		name := valueString(row[0])
		if name == "" {
			continue
		}
		r := bqRow{name: name, size: valueString(row[1]), md5: valueString(row[2]), updated: valueString(row[3])}
		if err := fn(r); err != nil {
			return err
		}
		if rows%bqProgressEvery == 0 {
			fs.Infof(f, "BigQuery listing cache: fetched %d/%d rows in %v", rows, it.TotalRows, time.Since(dlStart).Round(time.Millisecond))
		}
	}
	fs.Infof(f, "BigQuery listing cache: downloaded %d rows in %v", rows, time.Since(dlStart).Round(time.Millisecond))
	return nil
}

// bqRowObject builds the storage.Object a cached inventory row stands for, in
// the exact shape setMetaData expects. Shared by listing emission and the
// NewObject point lookup.
func bqRowObject(r bqRow) *storage.Object {
	object := &storage.Object{Name: r.name}
	object.Size, _ = strconv.ParseUint(r.size, 10, 64)
	object.Md5Hash = r.md5 // base64 (converted from the inventory's hex in SQL), as setMetaData expects
	object.Updated = r.updated
	return object
}

// emitBQRow maps one raw inventory row through objectRemote/setMetaData exactly
// as the Cloud Storage list path does, and feeds the listFn callback. It reports
// whether a row was emitted (skipped rows don't count towards the
// directory-not-found check). Shared by the direct and cache-served paths.
func (f *Fs) emitBQRow(r bqRow, directory, prefix, bucketName string, addBucket bool, fn listFn) (emitted bool, err error) {
	remote, isDirectory, skip := f.objectRemote(r.name, directory, prefix, bucketName, addBucket)
	if skip {
		return false, nil
	}
	object := &storage.Object{Name: r.name}
	if !isDirectory {
		object = bqRowObject(r)
	}
	if err := fn(remote, object, isDirectory); err != nil {
		return false, err
	}
	return true, nil
}
