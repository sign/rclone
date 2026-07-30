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
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sync/errgroup"
	bigquery "google.golang.org/api/bigquery/v2"
	storage "google.golang.org/api/storage/v1"
)

// bqPageSize is the row count requested per BigQuery results page. Deliberately
// large: getQueryResults caps each response at ~10MB and returns a pageToken for
// the rest, so a high value just minimises round-trips (a flat 100k-object
// directory then needs a handful of pages instead of ~100 at 1000/page).
const bqPageSize = 100000

// bqFetchConcurrency is how many getQueryResults pages are fetched in parallel
// once the job is complete (random access via StartIndex). The download is the
// populate's bottleneck: pages are ~10MB and took ~26s each when chained
// serially through pageTokens in production. ponytail: fixed at 8 (~150MB of
// transient row data); make it an option if a host ever needs tuning.
const bqFetchConcurrency = 8

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

// bqParam builds a named STRING query parameter. ForceSendFields keeps an empty
// Value in the request - otherwise omitempty drops it and BigQuery binds the
// parameter as NULL, which makes STARTS_WITH/SUBSTR(@dir) match nothing at the root.
func bqParam(name, value string) *bigquery.QueryParameter {
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: &bigquery.QueryParameterType{Type: "STRING"},
		ParameterValue: &bigquery.QueryParameterValue{
			Value:           value,
			ForceSendFields: []string{"Value"},
		},
	}
}

// bqListQuery builds the GoogleSQL listing query and its named parameters.
//
// updated is formatted to RFC3339Nano so it drops straight into
// storage.Object.Updated; md5Hash is converted from the inventory's hex to base64
// so both are parsed by setMetaData unchanged (it base64-decodes the hash and
// RFC3339-parses the time).
//
// Recursive lists every object under @dir. Single-level returns objects directly in
// @dir plus a synthesized row per immediate sub-directory (name ending in "/", which
// objectRemote then recognises as a directory) - the SQL equivalent of the list
// API's "/" delimiter common-prefixes.
func (f *Fs) bqListQuery(bucketName, directory string, recurse bool) (string, []*bigquery.QueryParameter) {
	tbl := f.opt.BigQueryTable
	params := []*bigquery.QueryParameter{bqParam("bucket", bucketName), bqParam("dir", directory)}
	const ts = "FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated)"
	const md5 = "TO_BASE64(FROM_HEX(md5Hash))" // Storage Insights stores md5Hash as hex; setMetaData wants base64

	if recurse {
		// QUALIFY keeps exactly one row per name - the newest capture - so a
		// changed object can't cache stale metadata and older duplicates are
		// never transferred. Deliberately a partitioned window and not a global
		// ORDER BY: ORDER BY without LIMIT runs on a single BigQuery worker and
		// multiplied the job time ~8x in production. And not a MAX(snapshotTime)
		// pin: snapshotTime is per-object (an unchanged file keeps its old one),
		// so pinning would drop every file unchanged since the last report.
		sql := fmt.Sprintf(
			"SELECT name, size, %s AS md5Hash, %s AS updated "+
				"FROM `%s` WHERE bucket = @bucket AND STARTS_WITH(name, @dir) "+
				"QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY snapshotTime DESC) = 1",
			md5, ts, tbl)
		return sql, params
	}

	sql := fmt.Sprintf(
		"WITH objs AS ("+
			"SELECT name, size, %s AS md5Hash, %s AS updated, SUBSTR(name, LENGTH(@dir)+1) AS rel "+
			"FROM `%s` WHERE bucket = @bucket AND STARTS_WITH(name, @dir)) "+
			"SELECT name, size, md5Hash, updated FROM objs WHERE STRPOS(rel, '/') = 0 "+
			"UNION DISTINCT "+
			"SELECT CONCAT(@dir, REGEXP_EXTRACT(rel, r'^[^/]+/')), CAST(0 AS INT64), CAST(NULL AS STRING), CAST(NULL AS STRING) "+
			"FROM objs WHERE STRPOS(rel, '/') > 0",
		md5, ts, tbl)
	return sql, params
}

// cellString reads a BigQuery result cell as a string (REST returns INTEGER and
// TIMESTAMP as strings); NULL cells yield "".
func cellString(cell *bigquery.TableCell) string {
	if cell == nil || cell.V == nil {
		return ""
	}
	if s, ok := cell.V.(string); ok {
		return s
	}
	return fmt.Sprint(cell.V)
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
// in bqcache.go.
func (f *Fs) bqQueryRows(ctx context.Context, bucketName, directory string, recurse bool, fn func(bqRow) error) error {
	// Money guard: this is the only function that spends BigQuery, and the sole
	// legitimate query is a root-level recursive populate (bigquery_table always
	// runs with the cache). A per-directory query here would reopen the storm the
	// cache exists to prevent, so refuse it rather than execute it.
	if !recurse || directory != f.bqRemoteRoot() {
		return fmt.Errorf("googlecloudstorage: internal error: a BigQuery listing query must be a root recursive populate, got directory=%q recurse=%v", directory, recurse)
	}
	// bound the whole populate (job + result pagination) so a slow query can't
	// hold bqPopMu forever; the old snapshot keeps being served on timeout
	if t := time.Duration(f.opt.BigQueryTimeout); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	fs.Infof(f, "BigQuery listing cache: running BigQuery query to populate %q (dir=%q, recurse=%v)", bucketName, directory, recurse)
	sql, params := f.bqListQuery(bucketName, directory, recurse)
	useLegacy := false
	req := &bigquery.QueryRequest{
		Query:           sql,
		UseLegacySql:    &useLegacy,
		ParameterMode:   "NAMED",
		QueryParameters: params,
		MaxResults:      bqPageSize,
	}

	// run the query and poll to completion; the completion response carries the
	// first page of results inline plus the total row count
	var (
		jobRef    *bigquery.JobReference
		complete  bool
		firstRows []*bigquery.TableRow
		totalRows uint64
	)
	start := time.Now()
	{
		var resp *bigquery.QueryResponse
		if err := f.pacer.Call(func() (bool, error) {
			var err error
			resp, err = f.bqSvc.Jobs.Query(f.bqProject, req).Context(ctx).Do()
			return shouldRetry(ctx, err)
		}); err != nil {
			return err
		}
		jobRef, complete, firstRows, totalRows = resp.JobReference, resp.JobComplete, resp.Rows, resp.TotalRows
	}
	for !complete {
		// job still running; the pacer only sleeps after retryable errors, so
		// back off here instead of hammering getQueryResults in a tight loop
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
		var resp *bigquery.GetQueryResultsResponse
		if err := f.pacer.Call(func() (bool, error) {
			call := f.bqSvc.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId).MaxResults(bqPageSize).Context(ctx)
			if jobRef.Location != "" {
				call = call.Location(jobRef.Location) // required for non-US/EU datasets
			}
			var err error
			resp, err = call.Do()
			return shouldRetry(ctx, err)
		}); err != nil {
			return err
		}
		complete, firstRows, totalRows = resp.JobComplete, resp.Rows, resp.TotalRows
	}
	// marks where query execution ends and result download begins - the
	// download of ~10MB getQueryResults pages usually dominates
	fs.Infof(f, "BigQuery listing cache: query finished in %v, downloading %d rows", time.Since(start).Round(time.Millisecond), totalRows)

	dlStart := time.Now()
	for _, r := range parseBQRows(firstRows) {
		if err := fn(r); err != nil {
			return err
		}
	}
	// row offsets below count raw result rows (including any skipped by
	// parseBQRows), so chunking uses len(firstRows), not the parsed count
	off := uint64(len(firstRows))
	if off > 0 {
		fs.Infof(f, "BigQuery listing cache: fetched %d rows (%d/%d) inline", len(firstRows), off, totalRows)
	}
	if off >= totalRows {
		return nil
	}

	// fan out: getQueryResults supports random access by row index (StartIndex),
	// so the remaining pages are fetched and parsed by bqFetchConcurrency
	// workers instead of being chained serially through pageTokens. This
	// goroutine stays the sole caller of fn - the bqRowSource emit contract is
	// single-threaded - by draining the workers' pages channel.
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(fctx)
	pages := make(chan []bqRow, bqFetchConcurrency)

	const chunkSize = uint64(bqPageSize)
	numChunks := (totalRows - off + chunkSize - 1) / chunkSize
	var nextChunk, fetched atomic.Uint64
	for w := 0; w < bqFetchConcurrency; w++ {
		g.Go(func() error {
			for {
				ci := nextChunk.Add(1) - 1
				if ci >= numChunks {
					return nil
				}
				cur := off + ci*chunkSize
				end := min(cur+chunkSize, totalRows)
				for cur < end {
					pageStart := time.Now()
					var resp *bigquery.GetQueryResultsResponse
					if err := f.pacer.Call(func() (bool, error) {
						call := f.bqSvc.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId).
							StartIndex(cur).MaxResults(int64(end - cur)).Context(gctx)
						if jobRef.Location != "" {
							call = call.Location(jobRef.Location)
						}
						var err error
						resp, err = call.Do()
						return shouldRetry(gctx, err)
					}); err != nil {
						return err
					}
					if len(resp.Rows) == 0 {
						// guards against an infinite loop if the server ever returns
						// an empty page short of the advertised total
						return fmt.Errorf("googlecloudstorage: getQueryResults returned no rows at index %d of %d", cur, totalRows)
					}
					select {
					case pages <- parseBQRows(resp.Rows):
					case <-gctx.Done():
						return gctx.Err()
					}
					cur += uint64(len(resp.Rows))
					fs.Infof(f, "BigQuery listing cache: fetched %d rows (%d/%d) in %v",
						len(resp.Rows), off+fetched.Add(uint64(len(resp.Rows))), totalRows, time.Since(pageStart).Round(time.Millisecond))
				}
			}
		})
	}
	go func() {
		_ = g.Wait() // the g.Wait below surfaces the error; this one just orders the close
		close(pages)
	}()

	var emitErr error
	for page := range pages {
		if emitErr != nil {
			continue // keep draining so no worker blocks on a send after an emit failure
		}
		for _, r := range page {
			if emitErr = fn(r); emitErr != nil {
				cancel() // stop the fetch fan-out
				break
			}
		}
	}
	if err := g.Wait(); err != nil && emitErr == nil {
		return err
	}
	if emitErr != nil {
		return emitErr
	}
	fs.Infof(f, "BigQuery listing cache: downloaded %d rows in %v", totalRows, time.Since(dlStart).Round(time.Millisecond))
	return nil
}

// parseBQRows converts raw REST result rows to bqRows, skipping malformed or
// nameless rows the same way the serial emit loop always has.
func parseBQRows(rows []*bigquery.TableRow) []bqRow {
	out := make([]bqRow, 0, len(rows))
	for _, row := range rows {
		if len(row.F) < 4 {
			continue
		}
		name := cellString(row.F[0])
		if name == "" {
			continue
		}
		out = append(out, bqRow{
			name:    name,
			size:    cellString(row.F[1]),
			md5:     cellString(row.F[2]),
			updated: cellString(row.F[3]),
		})
	}
	return out
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
