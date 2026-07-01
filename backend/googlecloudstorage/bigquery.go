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

	bigquery "google.golang.org/api/bigquery/v2"
	storage "google.golang.org/api/storage/v1"
)

// bqPageSize is the row count requested per BigQuery results page. Deliberately
// large: getQueryResults caps each response at ~10MB and returns a pageToken for
// the rest, so a high value just minimises round-trips (a flat 100k-object
// directory then needs a handful of pages instead of ~100 at 1000/page).
const bqPageSize = 100000

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
		sql := fmt.Sprintf(
			"SELECT name, size, %s AS md5Hash, %s AS updated "+
				"FROM `%s` WHERE bucket = @bucket AND STARTS_WITH(name, @dir)",
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
// for each row. It is the BigQuery half of listing, shared by the direct
// (cache-disabled) path and by the cache populate in bqcache.go.
func (f *Fs) bqQueryRows(ctx context.Context, bucketName, directory string, recurse bool, fn func(bqRow) error) error {
	// Money guard: this is the only function that spends BigQuery. With the cache
	// enabled the sole legitimate query is a root-level recursive populate; a
	// per-directory query here would reopen the storm the cache exists to prevent,
	// so refuse it rather than execute it. Without the cache, per-directory
	// single-level queries are the intended behaviour, so the guard doesn't apply.
	if f.bqDB != nil && (!recurse || directory != f.bqRemoteRoot()) {
		return fmt.Errorf("googlecloudstorage: internal error: a cache-backed BigQuery query must be a root recursive populate, got directory=%q recurse=%v", directory, recurse)
	}
	sql, params := f.bqListQuery(bucketName, directory, recurse)
	useLegacy := false
	req := &bigquery.QueryRequest{
		Query:           sql,
		UseLegacySql:    &useLegacy,
		ParameterMode:   "NAMED",
		QueryParameters: params,
		MaxResults:      bqPageSize,
	}

	var (
		jobRef    *bigquery.JobReference
		complete  bool
		rows      []*bigquery.TableRow
		pageToken string
		started   bool
	)
	for {
		if !started {
			// run the query (synchronous; first page comes back inline)
			var resp *bigquery.QueryResponse
			if err := f.pacer.Call(func() (bool, error) {
				var err error
				resp, err = f.bqSvc.Jobs.Query(f.bqProject, req).Context(ctx).Do()
				return shouldRetry(ctx, err)
			}); err != nil {
				return err
			}
			jobRef, complete, rows, pageToken = resp.JobReference, resp.JobComplete, resp.Rows, resp.PageToken
			started = true
		} else {
			// poll a still-running job, or fetch the next page of a complete one
			var resp *bigquery.GetQueryResultsResponse
			if err := f.pacer.Call(func() (bool, error) {
				call := f.bqSvc.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId).MaxResults(bqPageSize).Context(ctx)
				if jobRef.Location != "" {
					call = call.Location(jobRef.Location) // required for non-US/EU datasets
				}
				if pageToken != "" {
					call = call.PageToken(pageToken)
				}
				var err error
				resp, err = call.Do()
				return shouldRetry(ctx, err)
			}); err != nil {
				return err
			}
			complete, rows, pageToken = resp.JobComplete, resp.Rows, resp.PageToken
		}

		if !complete {
			continue // job still running; poll again (the pacer paces this)
		}

		for _, row := range rows {
			if len(row.F) < 4 {
				continue
			}
			name := cellString(row.F[0])
			if name == "" {
				continue
			}
			if err := fn(bqRow{
				name:    name,
				size:    cellString(row.F[1]),
				md5:     cellString(row.F[2]),
				updated: cellString(row.F[3]),
			}); err != nil {
				return err
			}
		}

		if pageToken == "" {
			break
		}
	}
	return nil
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
		object.Size, _ = strconv.ParseUint(r.size, 10, 64)
		object.Md5Hash = r.md5 // base64 (converted from the inventory's hex in SQL), as setMetaData expects
		object.Updated = r.updated
	}
	if err := fn(remote, object, isDirectory); err != nil {
		return false, err
	}
	return true, nil
}

// listBQ serves list() from the BigQuery inventory table. Without a cache
// (bqDB nil) it queries BigQuery on every call; with one it serves from bbolt,
// querying BigQuery only to (re)populate - see bqcache.go.
func (f *Fs) listBQ(ctx context.Context, bucketName, directory, prefix string, addBucket, recurse bool, fn listFn) error {
	if f.bqDB == nil {
		return f.listBQDirect(ctx, bucketName, directory, prefix, addBucket, recurse, fn)
	}
	return f.listBQCached(ctx, bucketName, directory, prefix, addBucket, recurse, fn)
}

// listBQDirect queries BigQuery and emits the rows - the original uncached
// behaviour, used when bigquery_cache_db is not set.
func (f *Fs) listBQDirect(ctx context.Context, bucketName, directory, prefix string, addBucket, recurse bool, fn listFn) error {
	found := 0
	err := f.bqQuery(ctx, bucketName, directory, recurse, func(r bqRow) error {
		emitted, err := f.emitBQRow(r, directory, prefix, bucketName, addBucket, fn)
		if emitted {
			found++
		}
		return err
	})
	if err != nil {
		return err
	}
	// BigQuery returns zero rows for a missing bucket/directory the same as for an
	// empty one, so reproduce the Cloud Storage list path's fs.ErrorDirNotFound.
	if found == 0 {
		return f.bqNotFound(ctx, bucketName, directory)
	}
	return nil
}
