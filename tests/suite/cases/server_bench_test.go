package cases

import (
	"bytes"
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	"io"
	"net/url"
	"testing"
)

var strResult string

func benchFprintf(b *testing.B, writer io.Writer, format string, a ...any) {
	_, err := fmt.Fprintf(writer, format, a...)
	suite.BenchErr(b, err)
}

func BenchmarkServer_Query_Count_1(b *testing.B)    { benchmarkServerQueryCount(b, 1) }
func BenchmarkServer_Query_Count_1K(b *testing.B)   { benchmarkServerQueryCount(b, 1000) }
func BenchmarkServer_Query_Count_100K(b *testing.B) { benchmarkServerQueryCount(b, 100000) }
func BenchmarkServer_Query_Count_1M(b *testing.B)   { benchmarkServerQueryCount(b, 1000000) }

func benchmarkServerQueryCount(b *testing.B, pointN int) {
	if _, err := server.Query(`DROP MEASUREMENT air`); err != nil {
		b.Fatal(err)
	}

	// Write data into server.
	var buf bytes.Buffer
	for i := 0; i < pointN; i++ {
		benchFprintf(b, &buf, `air temperature=100 %d`, i+1)
		if i != pointN-1 {
			benchFprintf(b, &buf, "\n")
		}
	}
	server.MustWrite("db0", "rp0", buf.String(), nil)

	// Query simple count from server.
	b.ResetTimer()
	b.ReportAllocs()
	var err error
	for i := 0; i < b.N; i++ {
		if strResult, err = server.Query(`SELECT count(temperature) FROM db0.rp0.air`); err != nil {
			b.Fatal(err)
		} else if strResult != fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",%d]]}]}]}`, pointN) {
			b.Fatalf("unexpected result: %s", strResult)
		}
	}
}

func BenchmarkServer_Query_Count_Where_500(b *testing.B) {
	benchmarkServerQueryCountWhere(b, false, 500)
}
func BenchmarkServer_Query_Count_Where_1K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, false, 1000)
}
func BenchmarkServer_Query_Count_Where_10K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, false, 10000)
}
func BenchmarkServer_Query_Count_Where_100K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, false, 100000)
}

func BenchmarkServer_Query_Count_Where_Regex_500(b *testing.B) {
	benchmarkServerQueryCountWhere(b, true, 500)
}
func BenchmarkServer_Query_Count_Where_Regex_1K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, true, 1000)
}
func BenchmarkServer_Query_Count_Where_Regex_10K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, true, 10000)
}
func BenchmarkServer_Query_Count_Where_Regex_100K(b *testing.B) {
	benchmarkServerQueryCountWhere(b, true, 100000)
}

func benchmarkServerQueryCountWhere(b *testing.B, useRegex bool, pointN int) {
	if _, err := server.Query(`DROP MEASUREMENT air`); err != nil {
		b.Fatal(err)
	}

	// Write data into server.
	var buf bytes.Buffer
	for i := 0; i < pointN; i++ {
		benchFprintf(b, &buf, `air,station=XiaoMaiDao-%d temperature=100 %d`, i, i)
		if i != pointN-1 {
			benchFprintf(b, &buf, "\n")
		}
	}
	server.MustWrite("db0", "rp0", buf.String(), nil)

	// Query count from server with WHERE
	var (
		err       error
		condition = `station = 'XiaoMaiDao-666'`
	)

	if useRegex {
		condition = `station =~ /^XiaoMaiDao-666$/`
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if strResult, err = server.Query(fmt.Sprintf(`SELECT count(temperature) FROM db0.rp0.air WHERE %s`, condition)); err != nil {
			b.Fatal(err)
		} else if strResult == `{"results":[{}]}` {
			b.Fatal("no results")
		}
	}
}

func BenchmarkServer_ShowSeries_1(b *testing.B)    { benchmarkServerShowSeries(b, 1) }
func BenchmarkServer_ShowSeries_1K(b *testing.B)   { benchmarkServerShowSeries(b, 1000) }
func BenchmarkServer_ShowSeries_100K(b *testing.B) { benchmarkServerShowSeries(b, 100000) }
func BenchmarkServer_ShowSeries_1M(b *testing.B)   { benchmarkServerShowSeries(b, 1000000) }

func benchmarkServerShowSeries(b *testing.B, pointN int) {
	if _, err := server.Query(`DROP MEASUREMENT air`); err != nil {
		b.Fatal(err)
	}

	// Write data into server.
	var buf bytes.Buffer
	for i := 0; i < pointN; i++ {
		benchFprintf(b, &buf, `air,station=XiaoMaiDao%d temperature=100 %d`, i, i+1)
		if i != pointN-1 {
			benchFprintf(b, &buf, "\n")
		}
	}
	server.MustWrite("db0", "rp0", buf.String(), nil)

	// Query simple count from server.
	b.ResetTimer()
	b.ReportAllocs()
	var err error
	for i := 0; i < b.N; i++ {
		if strResult, err = server.QueryWithParams(`SHOW SERIES`, url.Values{"db": {"db0"}}); err != nil {
			b.Fatal(err)
		}
	}
}
