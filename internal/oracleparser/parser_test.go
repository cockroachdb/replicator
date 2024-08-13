package oracleparser

import (
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	const testdataPath = `./testdata`
	datadriven.Walk(t, testdataPath, func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "parse":
				var expectError bool
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "error":
						expectError = true
					default:
						t.Errorf("unknown cmd arg %s", arg.Key)
					}
				}
				strRes, err := Parse(td.Input)
				if expectError {
					require.Error(t, err)
				}
				return strRes
			default:
				t.Fatalf("uknown cmd")
			}
			return ""
		})
	})
}
