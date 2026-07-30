package googlecloudstorage

import (
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
)

func TestObjectRemote(t *testing.T) {
	f := &Fs{} // zero MultiEncoder => identity path mapping
	for _, tc := range []struct {
		name, directory, prefix, bucket string
		addBucket                       bool
		wantRemote                      string
		wantDir, wantSkip               bool
	}{
		{name: "a/b.txt", wantRemote: "a/b.txt"},
		{name: "a/", wantRemote: "a", wantDir: true},                                              // synthesized sub-dir / marker
		{name: "photos/x.jpg", directory: "photos/", prefix: "photos/", wantRemote: "x.jpg"},      // listing under a prefix
		{name: "photos/", directory: "photos/", prefix: "photos/", wantDir: true, wantSkip: true}, // the listed dir itself
		{name: "other/x", prefix: "photos/", wantSkip: true},                                      // odd name (wrong prefix)
		{name: "a/b.txt", bucket: "mybucket", addBucket: true, wantRemote: "mybucket/a/b.txt"},
	} {
		remote, isDir, skip := f.objectRemote(tc.name, tc.directory, tc.prefix, tc.bucket, tc.addBucket)
		if remote != tc.wantRemote || isDir != tc.wantDir || skip != tc.wantSkip {
			t.Errorf("objectRemote(%q,%q,%q,%q,%v) = (%q,%v,%v); want (%q,%v,%v)",
				tc.name, tc.directory, tc.prefix, tc.bucket, tc.addBucket,
				remote, isDir, skip, tc.wantRemote, tc.wantDir, tc.wantSkip)
		}
	}
}

func TestBQListQuery(t *testing.T) {
	f := &Fs{}
	f.opt.BigQueryTable = "proj.ds.tbl"

	rec, params := f.bqListQuery("buck", "photos/", true)
	for _, want := range []string{"FROM `proj.ds.tbl`", "TO_BASE64(FROM_HEX(md5Hash))"} {
		if !strings.Contains(rec, want) {
			t.Errorf("recursive query missing %q:\n%s", want, rec)
		}
	}
	// the populate is a bare full-table read: bigquery_table requires a
	// single-bucket, one-row-per-object table, so there is nothing to filter or
	// dedupe. Anything reintroduced here is scanned over the whole corpus.
	for _, unwanted := range []string{"WHERE", "QUALIFY", "@bucket", "@dir"} {
		if strings.Contains(rec, unwanted) {
			t.Errorf("recursive query should not contain %q:\n%s", unwanted, rec)
		}
	}
	// no parameters may be sent for a query that references none of them
	if params != nil {
		t.Errorf("recursive query should send no parameters, got %+v", params)
	}
	// The populate writes rows straight into bbolt, whose insert cost collapses if
	// keys arrive out of order (copy-on-write B+tree: random keys rewrite a leaf
	// page per row). Sorted rows are what keep it fast, so the trailing ORDER BY
	// is load-bearing and must stay last - the client library only drops the read
	// session to the single ordered stream that preserves this order when it sees
	// a top-level ORDER BY, and bqPopulate's FillPercent=1.0 assumes in-order
	// appends.
	if !strings.HasSuffix(rec, " ORDER BY name") {
		t.Errorf("recursive query must end with a top-level ORDER BY name:\n%s", rec)
	}

	single, _ := f.bqListQuery("buck", "", false)
	for _, want := range []string{"UNION DISTINCT", "REGEXP_EXTRACT(rel, r'^[^/]+/')", "STRPOS(rel, '/') = 0"} {
		if !strings.Contains(single, want) {
			t.Errorf("single-level query missing %q:\n%s", want, single)
		}
	}
}

func TestResolveBQProject(t *testing.T) {
	for _, tc := range []struct {
		opt     Options
		want    string
		wantErr bool
	}{
		{opt: Options{BigQueryTable: "proj.ds.tbl"}, want: "proj"},
		{opt: Options{BigQueryTable: "ds.tbl", BigQueryBillingProject: "bp"}, want: "bp"},
		{opt: Options{BigQueryTable: "ds.tbl", ServiceAccountCredentials: `{"project_id":"sap"}`}, want: "sap"},
		{opt: Options{BigQueryTable: "ds.tbl"}, wantErr: true},
		{opt: Options{BigQueryTable: "bad table!"}, wantErr: true},
	} {
		got, err := resolveBQProject(&tc.opt)
		if (err != nil) != tc.wantErr || (err == nil && got != tc.want) {
			t.Errorf("resolveBQProject(%+v) = (%q,%v); want (%q, err=%v)", tc.opt, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestValueString(t *testing.T) {
	for _, tc := range []struct {
		v    bq.Value
		want string
	}{
		{v: nil, want: ""},
		{v: "x", want: "x"},
		{v: int64(9223372036854), want: "9223372036854"},
		{v: 5, want: "5"},
	} {
		if got := valueString(tc.v); got != tc.want {
			t.Errorf("valueString(%+v) = %q; want %q", tc.v, got, tc.want)
		}
	}
}
