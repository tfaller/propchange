package mysql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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

func TestUpdateListener(t *testing.T) {
	ctx := context.TODO()

	// prepare test case

	doc, err := detector.OpenDocument(ctx, "a")
	expectNoError(err, t)

	doc.SetProperty("a", 1)
	expectNoError(doc.Commit(), t)

	doc, err = detector.OpenDocument(ctx, "b")
	expectNoError(err, t)

	doc.SetProperty("a", 1)
	expectNoError(doc.Commit(), t)

	testCases := []struct {
		VersionA, VersionB, ChangeVersion uint64
		DocA, DocB, ChangeDoc             string
		Error                             error
	}{
		// simple test case, register first listener at docA, add listener for docB
		// trigger change on docA.
		{VersionA: 1, VersionB: 1, DocA: "a", DocB: "b", ChangeDoc: "a", ChangeVersion: 2},
		// simple test case, register first listener at docA, add listener for docB
		// trigger change on docB.
		{VersionA: 2, VersionB: 1, DocA: "a", DocB: "b", ChangeDoc: "b", ChangeVersion: 2},
		// overwrite listener property filter with a higher property number -> the lower number
		// should still be triggered
		{VersionA: 2, VersionB: 3, DocA: "a", DocB: "a", ChangeDoc: "a", ChangeVersion: 3},
		// overwrite a higher property filter with a lower -> the lower should trigger
		{VersionA: 4, VersionB: 3, DocA: "a", DocB: "a", ChangeDoc: "a", ChangeVersion: 4},
	}

	for testIdx, test := range testCases {
		// register first listener
		expectNoError(detector.AddListener(ctx, "updatefilter", []propchange.ChangeFilter{
			{Document: test.DocA, Properties: map[string]uint64{"a": test.VersionA}},
		}), t)

		// update the listener
		expectNoError(detector.AddListener(ctx, "updatefilter", []propchange.ChangeFilter{
			{Document: test.DocB, Properties: map[string]uint64{"a": test.VersionB}},
		}), t)

		doc, err = detector.OpenDocument(ctx, test.ChangeDoc)
		expectNoError(err, t)

		doc.SetProperty("a", test.ChangeVersion)
		expectNoError(doc.Commit(), t)

		change, err := detector.NextChange(ctx)
		if !errors.Is(err, test.Error) {
			t.Fatalf("%v: Expected error %v but got %v", testIdx, test.Error, err)
		}
		expectNoError(change.Commit(), t)
	}
}

func TestNewDocument(t *testing.T) {
	ctx := context.TODO()
	docName := fmt.Sprintf("%v", time.Now().UnixNano())

	// listen for document that does not exist
	expectNoError(detector.AddListener(ctx, "listener-newdoc", []propchange.ChangeFilter{
		{Document: docName, NewDocument: true},
	}), t)

	if _, err := detector.NextChange(ctx); err != propchange.ErrNoMoreChanges {
		t.Fatal("expected to find no changes")
	}

	// add the document
	doc, err := detector.OpenDocument(ctx, docName)
	expectNoError(err, t)
	if !doc.IsNew() {
		t.Fatal("document should be new")
	}
	expectNoError(doc.Commit(), t)

	for i := 0; i < 2; i++ {
		change, err := detector.NextChange(ctx)
		expectNoError(err, t)

		if change.Listener() != "listener-newdoc" {
			t.Fatal("Got wrong listener")
		}
		if len(change.Documents()) != 1 || change.Documents()[0] != docName {
			t.Fatal("Got wrong document")
		}

		expectNoError(change.Commit(), t)

		if _, err := detector.NextChange(ctx); err != propchange.ErrNoMoreChanges {
			t.Fatal("expected to find no changes")
		}

		if i == 0 {
			// listen again, but now the document exists
			expectNoError(detector.AddListener(ctx, "listener-newdoc", []propchange.ChangeFilter{
				{Document: docName, NewDocument: true},
			}), t)
		}
	}
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
