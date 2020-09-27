package mysql

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tfaller/propchange"
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

func TestBasicChange(t *testing.T) {
	ctx := context.TODO()

	doc, err := detector.OpenDocument(ctx, "obj")
	expectNoError(err, t)

	for i := 0; i < 10; i++ {
		err = doc.SetProperty(fmt.Sprintf("prop%v", i), 4)
		expectNoError(err, t)
	}

	expectNoError(doc.Commit(), t)

	err = detector.AddListener(ctx, "1", []propchange.ChangeFilter{
		{Document: "obj", Properties: map[string]uint64{"prop1": 4}},
	})

	expectNoError(err, t)

	// no changes should be found
	change, err := detector.NextChange(ctx)
	if err != propchange.ErrNoMoreChanges {
		t.Errorf("Expected no changes but got %v", change)
	}

	// modify doc to now see changes
	doc, err = detector.OpenDocument(ctx, "obj")
	expectNoError(err, t)

	err = doc.SetProperty("prop1", 5)
	expectNoError(err, t)
	expectNoError(doc.Commit(), t)

	change, err = detector.NextChange(ctx)
	expectNoError(err, t)

	if change.Listener() != "1" {
		t.Errorf("Wrong listener wanted %q but got %q", "1", change.Listener())
	}

	if change.Documents()[0] != "obj" {
		t.Error("Expected doc as document")
	}

	err = change.Commit()
	expectNoError(err, t)

	// we commit last change ... we should now found none
	change, err = detector.NextChange(ctx)
	if err != propchange.ErrNoMoreChanges {
		t.Errorf("Expected no changes but got %v", change)
	}
}

func expectNoError(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}
}
