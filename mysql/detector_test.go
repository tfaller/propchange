package mysql

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tfaller/propchange"
	"github.com/tfaller/propchange/internal/tests"
)

var detector propchange.Detector

func TestMain(m *testing.M) {
	// for each test we have to open the db first

	mysqlDs := os.Getenv("MYSQL_DS")
	if mysqlDs == "" {
		panic("No MYSQL_DS Env provided")
	}

	var err error
	detector, err = NewDetector(mysqlDs)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestSuite(t *testing.T) {
	tests.TestSuite(detector, t)
}

func BenchmarkNextChange(b *testing.B) {
	ctx := context.TODO()

	// insert test data
	for i := 0; i < b.N; i++ {
		var docName = fmt.Sprintf("b-%v", i)

		doc, err := detector.OpenDocument(ctx, docName)
		bExpectNoError(err, b)
		bExpectNoError(doc.SetProperty("prop", 1), b)
		bExpectNoError(doc.Commit(), b)

		bExpectNoError(detector.AddListener(ctx, docName, []propchange.ChangeFilter{
			{
				Document:   docName,
				Properties: map[string]uint64{"prop": 0},
			},
		}), b)
	}

	// run actual tests
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		change, err := detector.NextChange(ctx)
		bExpectNoError(err, b)
		bExpectNoError(change.Commit(), b)
	}
}

func expectNoError(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}
}

func bExpectNoError(err error, b *testing.B) {
	if err != nil {
		b.Fatalf("Expected no error but got %v", err)
	}
}
