package googlecloudstorage

import (
	"fmt"
	"sort"
	"testing"
)

// benchRows builds n inventory rows with realistic ~90-character object names.
// Names are derived from a deterministic LCG so a run is reproducible and the
// key distribution is spread across the keyspace like a real corpus, not
// clustered by a counter.
func benchRows(n int, sorted bool) []bqRow {
	rows := make([]bqRow, 0, n)
	state := uint64(0x2545F4914F6CDD1D)
	for i := 0; i < n; i++ {
		state = state*6364136223846793005 + 1442695040888963407
		hi, lo := state>>32, state&0xFFFFFFFF
		rows = append(rows, bqRow{
			name:    fmt.Sprintf("videos/%04x/%04x/clip-%08x%08x-transcoded-h264-1080p.mp4", hi&0xFFFF, lo&0xFFFF, hi, lo),
			size:    "1048576",
			md5:     "1B2M2Y8AsgTpgAmY7PhCfg==",
			updated: "2026-07-30T19:36:32Z",
		})
	}
	if sorted {
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	}
	return rows
}

// BenchmarkBQPopulateKeyOrder measures the populate's bbolt write path with keys
// arriving sorted vs unsorted. bbolt is a copy-on-write B+tree, so out-of-order
// keys rewrite roughly a leaf page per row and degrade as the tree grows - this
// is what made a 9.26M-row production populate decay from 74k rows/s to 13k
// rows/s and take 8m31s regardless of how fast the rows were downloaded. The
// query's ORDER BY name exists to keep this benchmark's "sorted" case the one
// that runs in production.
func BenchmarkBQPopulateKeyOrder(b *testing.B) {
	for _, n := range []int{200000, 1000000} {
		for _, sorted := range []bool{true, false} {
			order := "unsorted"
			if sorted {
				order = "sorted"
			}
			b.Run(fmt.Sprintf("rows=%d/%s", n, order), func(b *testing.B) {
				rows := benchRows(n, sorted)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					f := newTestCacheFs(b) // opening the db is setup, not the thing measured
					b.StartTimer()
					if err := f.bqPopulate("bucket", sliceSource(rows)); err != nil {
						b.Fatalf("bqPopulate: %v", err)
					}
				}
				b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})
		}
	}
}
