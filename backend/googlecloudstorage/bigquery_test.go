package googlecloudstorage

import (
	"strings"
	"testing"

	bigquery "google.golang.org/api/bigquery/v2"
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
	for _, want := range []string{"FROM `proj.ds.tbl`", "STARTS_WITH(name, @dir)", "TO_BASE64(FROM_HEX(md5Hash))", "ORDER BY name, snapshotTime"} {
		if !strings.Contains(rec, want) {
			t.Errorf("recursive query missing %q:\n%s", want, rec)
		}
	}
	if len(params) != 2 || params[0].ParameterValue.Value != "buck" || params[1].ParameterValue.Value != "photos/" {
		t.Errorf("unexpected params: %+v", params)
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

func TestCellString(t *testing.T) {
	for _, tc := range []struct {
		cell *bigquery.TableCell
		want string
	}{
		{cell: nil, want: ""},
		{cell: &bigquery.TableCell{V: nil}, want: ""},
		{cell: &bigquery.TableCell{V: "x"}, want: "x"},
		{cell: &bigquery.TableCell{V: 5}, want: "5"},
	} {
		if got := cellString(tc.cell); got != tc.want {
			t.Errorf("cellString(%+v) = %q; want %q", tc.cell, got, tc.want)
		}
	}
}
