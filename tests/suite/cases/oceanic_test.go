package cases

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	"github.com/cnosdb/cnosdb/tests/suite/oceanic"
	"testing"
)

func TestOceanic(t *testing.T) {
	n := oceanic.OCEANIC{S: server, T: t}
	n.Load()
	n.Test()
}

const (
	db = "oceanic_station"
	rp = "rp0"
)

func TestGenCode_1(t *testing.T) {

	n := oceanic.OCEANIC{S: server, T: t}
	n.Load()
	s := suite.Step{
		Name:  "select_air_all",
		Query: fmt.Sprintf(`select * from "%s"."%s".air limit 30 offset 700`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	}
	s.ResCode(server)
}
func TestGenCode_n(t *testing.T) {
	n := oceanic.OCEANIC{S: server, T: t}
	fmt.Printf("loading data...")
	n.Load()
	var steps = [...]suite.Step{
		//Addition
		{
			Name:  "select_air_all",
			Query: fmt.Sprintf(`select * from "%s"."%s".air limit 30`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "select_air_all_vis_70",
			Query: fmt.Sprintf(`select * from "%s"."%s".air where visibility > 70 limit 30 offset 500`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
	}
	var i int
	for i = 0; i < len(steps); i++ {
		fmt.Printf("\n")
		fmt.Printf(steps[i].Name)
		fmt.Printf("\n")
		steps[i].ResCode(server)
	}
}
