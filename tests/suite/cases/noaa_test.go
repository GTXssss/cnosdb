package cases

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	"github.com/cnosdb/cnosdb/tests/suite/noaa"
	"testing"
)

func TestNoaa(t *testing.T) {
	n := noaa.NOAA{S: server, T: t}
	n.Load()
	n.Test()
}

const (
	db_noaa = "NOAA_water_database"
	rp_noaa = "rp0"
)

func TestGenCode(t *testing.T) {

	n := noaa.NOAA{S: server, T: t}
	n.Load()
	s := suite.Step{
		Name:  "h2o_feet_h2o_pH",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet","%s"."%s"."h2o_pH" LIMIT 10 OFFSET 1000`, db_noaa, rp_noaa, db_noaa, rp_noaa),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	}
	s.ResCode(server)
}
