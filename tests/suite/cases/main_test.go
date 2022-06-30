package cases

import (
	"flag"
	"fmt"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/tests"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"os"
	"testing"
	"time"
)

var server tests.Server

func TestMain(m *testing.M) {
	flag.BoolVar(&tests.VerboseServerLogs, "vv", false, "Turn on very verbose server logging.")
	flag.BoolVar(&tests.CleanupData, "clean", true, "Clean up test data on disk.")
	flag.Int64Var(&tests.Seed, "seed", 0, "Set specific seed controlling randomness.")
	flag.Parse()

	// Set random seed if not explicitly set.
	if tests.Seed == 0 {
		tests.Seed = time.Now().UnixNano()
	}
	rand.Seed(tests.Seed)

	var r int
	for _, tests.IndexType = range tsdb.RegisteredIndexes() {
		//setup server
		c := tests.NewConfig()
		c.RetentionPolicy.Enabled = false
		c.Monitor.StoreEnabled = false
		c.Subscriber.Enabled = false
		c.ContinuousQuery.Enabled = true
		c.Data.MaxValuesPerTag = 1000000 // 1M
		c.Data.MaxSeriesPerDatabase = 10000000
		c.Log = logger.NewDefaultLogConfig()
		c.Log.Level = zapcore.ErrorLevel
		c.HTTPD.MaxBodySize = 0
		if err := logger.InitZapLogger(c.Log); err != nil {
			fmt.Printf("parse log config: %s\n", err)
		}
		server = tests.OpenDefaultServer(c)

		if testing.Verbose() {
			fmt.Printf("================ Running all tests #{%v} index ================\n", tests.IndexType)
		}

		if curr := m.Run(); r == 0 {
			r = curr
		}

		server.Close()
		if testing.Verbose() {
			fmt.Println()
		}
	}
	os.Exit(r)
}
