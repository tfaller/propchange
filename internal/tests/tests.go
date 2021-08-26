package tests

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tfaller/propchange"
)

func TestSuite(detector propchange.Detector, t *testing.T) {
	t.Run("TestBasicChange", func(t *testing.T) { TestBasicChange(detector, t) })
	t.Run("TestUsedCloseDoc", func(t *testing.T) { TestUsedCloseDoc(detector, t) })
	t.Run("TestDelDocument", func(t *testing.T) { TestDelDocument(detector, t) })
	t.Run("TestDelProperty", func(t *testing.T) { TestDelProperty(detector, t) })
	t.Run("TestUpdateListener", func(t *testing.T) { TestUpdateListener(detector, t) })
	t.Run("TestDelListener", func(t *testing.T) { TestDelListener(detector, t) })
	t.Run("TestInvalidListener", func(t *testing.T) { TestInvalidListener(detector, t) })
	t.Run("TestListenerMulti", func(t *testing.T) { TestListenerMulti(detector, t) })
	t.Run("TestListenerMultiDelete", func(t *testing.T) { TestListenerMultiDelete(detector, t) })
	t.Run("TestListenerDocNotExisting", func(t *testing.T) { TestListenerDocNotExisting(detector, t) })
	t.Run("TestListenerPropNotExisting", func(t *testing.T) { TestListenerPropNotExisting(detector, t) })
	t.Run("TestListenerNameReuse", func(t *testing.T) { TestListenerNameReuse(detector, t) })
	t.Run("TestNewDocument", func(t *testing.T) { TestNewDocument(detector, t) })
	t.Run("TestAbortNewDocument", func(t *testing.T) { TestAbortNewDocument(detector, t) })
	t.Run("TestAbortChange", func(t *testing.T) { TestAbortChange(detector, t) })
	t.Run("TestSingleChange", func(t *testing.T) { TestSingleChange(detector, t) })
}

func TestBasicChange(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	doc, err := detector.OpenDocument(ctx, "obj")
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	for i := 0; i < 10; i++ {
		assert.NoError(t, doc.SetProperty(fmt.Sprintf("prop%v", i), 4))
	}

	// invalid property name
	err = propchange.ErrInvalidPropertyName("")
	assert.ErrorAs(t, doc.SetProperty("", 0), &err)

	assert.NoError(t, doc.Commit())

	assert.NoError(t, detector.AddListener(ctx, "1", []propchange.ChangeFilter{
		{Document: "obj", Properties: map[string]uint64{"prop1": 4}},
	}))

	// no changes should be found
	assertNoChange(t, ctx, detector)

	// modify doc to now see changes
	doc, err = detector.OpenDocument(ctx, "obj")
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	assert.NoError(t, doc.SetProperty("prop1", 5))
	assert.NoError(t, doc.Commit())

	for i := 0; i < 2; i++ {
		change, err := detector.NextChange(ctx)
		assert.NoError(t, err)
		assertChange(t, change, "1", []string{"obj"})
		assert.NoError(t, change.Commit())

		assertNoChange(t, ctx, detector)

		if i == 0 {
			// add an already triggering listener
			assert.NoError(t, detector.AddListener(ctx, "1", []propchange.ChangeFilter{{Document: "obj", Properties: map[string]uint64{"prop1": 4}}}))
		}
	}

	assertNoChange(t, ctx, detector)
}

func TestUsedCloseDoc(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	docCloseMethods := []func() propchange.DocumentOps{
		// committed doc
		func() propchange.DocumentOps {
			doc := assertOpenNewDoc(t, ctx, detector, "used")
			assert.NoError(t, doc.Commit())
			return doc
		},
		// closed doc
		func() propchange.DocumentOps {
			doc, err := detector.OpenDocument(ctx, "used")
			assert.NoError(t, err)
			assert.NotNil(t, doc)
			assert.NoError(t, doc.Close())
			return doc
		},
		// deleted doc
		func() propchange.DocumentOps {
			doc, err := detector.OpenDocument(ctx, "used")
			assert.NoError(t, err)
			assert.NotNil(t, doc)
			assert.NoError(t, doc.Delete())
			return doc
		},
	}

	for _, closeMethod := range docCloseMethods {
		doc := closeMethod()

		assert.ErrorIs(t, propchange.ErrDocAlreadyClosedError, doc.SetProperty("a", 0))
		assert.ErrorIs(t, propchange.ErrDocAlreadyClosedError, doc.DelProperty("a"))
		assert.ErrorIs(t, propchange.ErrDocAlreadyClosedError, doc.Close())
		assert.ErrorIs(t, propchange.ErrDocAlreadyClosedError, doc.Commit())
		assert.ErrorIs(t, propchange.ErrDocAlreadyClosedError, doc.Delete())
	}
}

func TestDelDocument(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// first test when doc listener exists for existing doc

	doc := assertOpenNewDoc(t, ctx, detector, "doc")
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.Equal(t, map[string]uint64{"a": 1}, doc.GetProperties())
	assert.NoError(t, doc.Commit())

	assert.NoError(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: "doc", Properties: map[string]uint64{"a": 1}}}))
	assertNoChange(t, ctx, detector)

	// delete document ...
	doc, err := detector.OpenDocument(ctx, "doc")
	assert.NoError(t, err)
	assert.NotNil(t, doc)
	assert.False(t, doc.IsNew())
	assert.NoError(t, doc.Delete())

	// doc should not exists anymore
	assertDocNotExists(t, ctx, detector, "doc")

	// change should trigger
	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener", []string{"doc"})
	assert.NoError(t, change.Commit())

	// no more changes
	assertNoChange(t, ctx, detector)

	// special case: listener listens to "NewDocument". If
	// a non existing document gets deleted ... nothing should happen

	assert.NoError(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: "doc", NewDocument: true}}))
	assertNoChange(t, ctx, detector)

	// delete non existing document
	doc = assertOpenNewDoc(t, ctx, detector, "doc")
	assert.NoError(t, doc.Delete())

	// listener is not allowed to trigger
	assertNoChange(t, ctx, detector)

	// now create the document
	doc = assertOpenNewDoc(t, ctx, detector, "doc")
	assert.NoError(t, doc.Commit())

	// change should finally trigger
	change, err = detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener", []string{"doc"})
	assert.NoError(t, change.Commit())

	// no more changes
	assertNoChange(t, ctx, detector)
}

func TestDelProperty(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := "doc-del-prop"

	// doc with a some props
	doc := assertOpenNewDoc(t, ctx, detector, docName)
	assert.NoError(t, doc.SetProperty("a", 0))
	assert.NoError(t, doc.SetProperty("b", 1))
	assert.NoError(t, doc.SetProperty("c", 2))
	assert.NoError(t, doc.Commit())

	assert.NoError(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: docName, Properties: map[string]uint64{"a": 0}}}))
	assertNoChange(t, ctx, detector)

	// delete a prop ... this should not trigger a change
	doc, err := detector.OpenDocument(ctx, docName)
	assert.NoError(t, err)
	assert.Equal(t, map[string]uint64{"a": 0, "b": 1, "c": 2}, doc.GetProperties())
	assert.NoError(t, doc.DelProperty("c"))
	assert.Equal(t, map[string]uint64{"a": 0, "b": 1}, doc.GetProperties())
	assert.NoError(t, doc.Commit())
	assertNoChange(t, ctx, detector)

	// delete prop that should trigger a change
	doc, err = detector.OpenDocument(ctx, docName)
	assert.NoError(t, err)
	assert.Equal(t, map[string]uint64{"a": 0, "b": 1}, doc.GetProperties())
	assert.NoError(t, doc.DelProperty("c")) // delete non existing prop should be silently ignored
	assert.NoError(t, doc.DelProperty("a"))
	assert.NoError(t, doc.DelProperty("b"))
	assert.NoError(t, doc.SetProperty("b", 0))
	assert.Equal(t, map[string]uint64{"b": 1}, doc.GetProperties())
	assert.NoError(t, doc.Commit())

	// the change ...
	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener", []string{docName})
	assert.NoError(t, change.Commit())

	// no more changes
	assertNoChange(t, ctx, detector)
}

func TestUpdateListener(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// prepare test case

	doc := assertOpenNewDoc(t, ctx, detector, "a")
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.Commit())

	doc = assertOpenNewDoc(t, ctx, detector, "b")
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.Commit())

	testCases := []struct {
		VersionA, VersionB, ChangeVersion uint64
		DocA, DocB, ChangeDoc             string
		ListenerDocs                      []string
	}{
		// simple test case, register first listener at docA, add listener for docB
		// trigger change on docA.
		{VersionA: 1, VersionB: 1, DocA: "a", DocB: "b", ChangeDoc: "a", ChangeVersion: 2, ListenerDocs: []string{"a", "b"}},
		// simple test case, register first listener at docA, add listener for docB
		// trigger change on docB.
		{VersionA: 2, VersionB: 1, DocA: "a", DocB: "b", ChangeDoc: "b", ChangeVersion: 2, ListenerDocs: []string{"a", "b"}},
		// overwrite listener property filter with a higher property number -> the lower number
		// should still be triggered
		{VersionA: 2, VersionB: 3, DocA: "a", DocB: "a", ChangeDoc: "a", ChangeVersion: 3, ListenerDocs: []string{"a"}},
		// overwrite a higher property filter with a lower -> the lower should trigger
		{VersionA: 4, VersionB: 3, DocA: "a", DocB: "a", ChangeDoc: "a", ChangeVersion: 4, ListenerDocs: []string{"a"}},
	}

	for _, test := range testCases {
		// register first listener
		assert.NoError(t, detector.AddListener(ctx, "updatefilter", []propchange.ChangeFilter{
			{Document: test.DocA, Properties: map[string]uint64{"a": test.VersionA}},
		}), t)

		// update the listener
		assert.NoError(t, detector.AddListener(ctx, "updatefilter", []propchange.ChangeFilter{
			{Document: test.DocB, Properties: map[string]uint64{"a": test.VersionB}},
		}), t)

		doc, err := detector.OpenDocument(ctx, test.ChangeDoc)
		assert.NoError(t, err)
		assert.NoError(t, doc.SetProperty("a", test.ChangeVersion))
		assert.NoError(t, doc.Commit())

		change, err := detector.NextChange(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "updatefilter", change.Listener())
		assert.ElementsMatch(t, test.ListenerDocs, change.Documents())
		assert.NoError(t, change.Commit())
	}
}

func TestDelListener(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// test new doc listener
	assert.NoError(t, detector.AddListener(ctx, "new-doc", []propchange.ChangeFilter{{Document: "listener-del", NewDocument: true}}))
	assertNoChange(t, ctx, detector)

	// deleting non existing listener should cause no error
	assert.NoError(t, detector.DelListener(ctx, "foobar"))
	assertNoChange(t, ctx, detector)

	// delete actual listener
	assert.NoError(t, detector.DelListener(ctx, "new-doc"))
	assertNoChange(t, ctx, detector)

	// now create the doc
	doc := assertOpenNewDoc(t, ctx, detector, "listener-del")
	assert.NoError(t, doc.SetProperty("a", 0))
	assert.NoError(t, doc.Commit())

	// we deleted the listener, no new change listener is allowed
	assertNoChange(t, ctx, detector)

	// test property listener
	assert.NoError(t, detector.AddListener(ctx, "new-doc", []propchange.ChangeFilter{{Document: "listener-del", Properties: map[string]uint64{"a": 0}}}))
	assertNoChange(t, ctx, detector)

	// instant delete it again
	assert.NoError(t, detector.DelListener(ctx, "new-doc"))
	assertNoChange(t, ctx, detector)

	// cause a property change
	doc, err := detector.OpenDocument(ctx, "listener-del")
	assert.NoError(t, err)
	assert.NotNil(t, doc)
	assert.False(t, doc.IsNew())
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.Equal(t, map[string]uint64{"a": 1}, doc.GetProperties())
	assert.NoError(t, doc.Commit())

	// we deleted the listener, no new change listener is allowed
	assertNoChange(t, ctx, detector)
}

func TestInvalidListener(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// empty listener name
	assert.Error(t, detector.AddListener(ctx, "", []propchange.ChangeFilter{{Document: "doc", NewDocument: true}}))

	// empty filter
	assert.Error(t, detector.AddListener(ctx, "listener", nil))

	// empty document filter
	assert.Error(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: "doc"}}))
}

func TestListenerMulti(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	doc := assertOpenNewDoc(t, ctx, detector, "multi")
	assert.NoError(t, doc.SetProperty("a", 0))
	assert.NoError(t, doc.Commit())

	// register some listeners
	assert.NoError(t, detector.AddListener(ctx, "a", []propchange.ChangeFilter{{Document: "multi", Properties: map[string]uint64{"a": 1}}}))
	assert.NoError(t, detector.AddListener(ctx, "b", []propchange.ChangeFilter{{Document: "multi", Properties: map[string]uint64{"a": 0}}}))
	assert.NoError(t, detector.AddListener(ctx, "c", []propchange.ChangeFilter{{Document: "multi", Properties: map[string]uint64{"a": 1}}}))

	assertNoChange(t, ctx, detector)

	// trigger first
	doc, err := detector.OpenDocument(ctx, "multi")
	assert.NoError(t, err)
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.Commit())

	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "b", []string{"multi"})
	assert.NoError(t, change.Commit())

	assertNoChange(t, ctx, detector)

	// trigger next changes
	doc, err = detector.OpenDocument(ctx, "multi")
	assert.NoError(t, err)
	assert.NoError(t, doc.SetProperty("a", 2))
	assert.NoError(t, doc.Commit())

	change1, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assert.Contains(t, []string{"a", "c"}, change1.Listener())

	change2, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assert.Contains(t, []string{"a", "c"}, change1.Listener())

	// should be different listener
	assert.NotEqual(t, change1.Listener(), change2.Listener())

	assert.NoError(t, change1.Commit())
	assert.NoError(t, change2.Commit())

	assertNoChange(t, ctx, detector)

	// do some more tests with different insert -> trigger order
	doc, err = detector.OpenDocument(ctx, "multi")
	assert.NoError(t, err)
	assert.NoError(t, doc.SetProperty("c", 1))
	assert.NoError(t, doc.SetProperty("b", 1))
	assert.NoError(t, doc.SetProperty("d", 1))
	assert.NoError(t, doc.SetProperty("e", 1))
	assert.NoError(t, doc.Commit())

	addListeners := []struct {
		Listener string
		Property string
		Revion   uint64
	}{
		{"b1", "b", 1}, {"b2", "b", 2}, {"b3", "b", 3},
		{"c1", "c", 1}, {"c3", "c", 3}, {"c2", "c", 2},
		{"d2", "d", 2}, {"d3", "d", 3}, {"d1", "d", 1},
		{"e3", "e", 3}, {"e2", "e", 2}, {"e1", "e", 1},
	}
	for _, i := range addListeners {
		assert.NoError(t, detector.AddListener(ctx, i.Listener, []propchange.ChangeFilter{
			{Document: "multi", Properties: map[string]uint64{i.Property: i.Revion}},
		}))
	}

	for _, prop := range []string{"b", "c", "d", "e"} {
		for _, rev := range []uint64{2, 3, 4} {
			listener := fmt.Sprintf("%v%v", prop, rev-1)

			assertNoChange(t, ctx, detector)

			doc, err = detector.OpenDocument(ctx, "multi")
			assert.NoError(t, err)
			assert.NoError(t, doc.SetProperty(prop, rev))
			assert.NoError(t, doc.Commit())

			change, err := detector.NextChange(ctx)
			assert.NoError(t, err, listener)
			assertChange(t, change, listener, []string{"multi"})
			assert.NoError(t, change.Commit())
		}
	}

	assertNoChange(t, ctx, detector)
}

func TestListenerMultiDelete(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := "multi-del"

	doc := assertOpenNewDoc(t, ctx, detector, docName)
	assert.NoError(t, doc.SetProperty("a", 0))
	assert.NoError(t, doc.Commit())

	testCases := []struct {
		setnewRev         uint64
		listenRev         []uint64
		delete            []string
		triggeredListener []string
	}{
		{setnewRev: 1, listenRev: []uint64{0, 1, 2}, delete: []string{"2", "1"}, triggeredListener: []string{"0"}},
		{setnewRev: 2, listenRev: []uint64{1, 2, 3}, delete: []string{"1", "2"}, triggeredListener: []string{"0"}},
		{setnewRev: 3, listenRev: []uint64{2, 3, 4}, delete: []string{"0", "1", "2"}, triggeredListener: []string{}},
		{setnewRev: 5, listenRev: []uint64{3, 5, 4}, delete: []string{"1"}, triggeredListener: []string{"0", "2"}},
		{setnewRev: 7, listenRev: []uint64{5, 6, 7}, delete: []string{"0", "2"}, triggeredListener: []string{"1"}},
	}

	for _, test := range testCases {
		assertNoChange(t, ctx, detector)

		// register listeners
		for i, rev := range test.listenRev {
			assert.NoError(t, detector.AddListener(ctx, fmt.Sprint(i), []propchange.ChangeFilter{{Document: docName, Properties: map[string]uint64{"a": rev}}}))
		}

		// delete listeners early
		for _, l := range test.delete {
			assert.NoError(t, detector.DelListener(ctx, l))
		}

		// perform change
		doc, err := detector.OpenDocument(ctx, docName)
		assert.NoError(t, err)
		assert.NoError(t, doc.SetProperty("a", test.setnewRev))
		assert.NoError(t, doc.Commit())

		// check for expected changes

		done := map[string]struct{}{}
		for i := len(test.triggeredListener) - 1; i >= 0; i-- {
			change, err := detector.NextChange(ctx)
			assert.NoError(t, err)
			assert.Equal(t, []string{docName}, change.Documents())
			assert.NotContains(t, done, change.Listener())
			assert.Contains(t, test.triggeredListener, change.Listener())
			assert.NoError(t, change.Commit())
			done[change.Listener()] = struct{}{}
		}
	}

	assertNoChange(t, ctx, detector)
}

func TestListenerDocNotExisting(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// make sure everthing is clean
	assertNoChange(t, ctx, detector)

	// listen to a doc that does not exists
	assert.Nil(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: "not-exists", Properties: map[string]uint64{"name": 1}}}))

	// a non existing doc should instantly trigger a change
	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener", []string{"not-exists"})
	assert.Nil(t, change.Commit())

	// no more change
	assertNoChange(t, ctx, detector)

	// retest ... but now a "new-doc" listener exists. The same thing should happen
	assert.Nil(t, detector.AddListener(ctx, "listener-newdoc", []propchange.ChangeFilter{{Document: "not-exists", NewDocument: true}}))
	assertNoChange(t, ctx, detector)

	// new listener which should trigger instantly
	assert.Nil(t, detector.AddListener(ctx, "listener", []propchange.ChangeFilter{{Document: "not-exists", Properties: map[string]uint64{"name": 1}}}))

	change, err = detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener", []string{"not-exists"})
	assert.Nil(t, change.Commit())

	assertNoChange(t, ctx, detector)
}

func TestListenerPropNotExisting(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := fmt.Sprintf("propdoc-%v", time.Now().UnixNano())

	assertNoChange(t, ctx, detector)

	// create test doc
	doc := assertOpenNewDoc(t, ctx, detector, docName)
	assert.NoError(t, doc.Commit())

	assertDocExists(t, ctx, detector, docName)

	// listen to prop that does not exist
	assert.NoError(t, detector.AddListener(ctx, "listener-prop", []propchange.ChangeFilter{{Document: docName, Properties: map[string]uint64{"prop": 0}}}))

	// listener should instantly trigger
	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "listener-prop", []string{docName})
	assert.NoError(t, change.Commit())

	assertNoChange(t, ctx, detector)
}

func TestListenerNameReuse(detector propchange.Detector, t *testing.T) {
	// Disable gc for this test.
	// We basically want to test whether a race condition could
	// break the listener reuse. For that we want to make the test and
	// environment more deterministic ... however it is still possible
	// that we hit not each time the wanted race condition :(
	gcPercent := debug.SetGCPercent(-1)
	defer func() { debug.SetGCPercent(gcPercent) }()

	ctx := context.TODO()
	startTime := time.Now()
	listenerName := fmt.Sprintf("l-reuse-%v", time.Now().UnixNano())
	docNameA, docNameB := "la", "lb"

	// basic test doc
	doc := assertOpenNewDoc(t, ctx, detector, docNameA)
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.Commit())

	doc = assertOpenNewDoc(t, ctx, detector, docNameB)
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.Commit())

	assertNoChange(t, ctx, detector)

	// reuse the same listener name directly after each other
	for i := 0; i < 2; i++ {
		// add a listener which triggers
		assert.NoError(t, detector.AddListener(ctx, listenerName, []propchange.ChangeFilter{{Document: docNameA, Properties: map[string]uint64{"a": 0}}}))

		change, err := detector.NextChange(ctx)
		assert.NoError(t, err)
		assertChange(t, change, listenerName, []string{docNameA})
		assert.NoError(t, change.Commit())

		assertNoChange(t, ctx, detector)
	}

	// now reuse a listener which is currently "open"
	assert.NoError(t, detector.AddListener(ctx, listenerName, []propchange.ChangeFilter{{Document: docNameA, Properties: map[string]uint64{"a": 0}}}))

	change, err := detector.NextChange(ctx)
	assertChange(t, change, listenerName, []string{docNameA})
	assert.NoError(t, err)

	stopTime := time.Now()

	// Update / reuse a open listener.
	// Do this in a goroutine because it might block.
	// This is where the non determinism is ... the following "AddListener"
	// might be blocked because the listener is already open ... or it is stuck
	// somewere else -> which breaks the test. We can't know.
	// If we wait a bit, the possibility is high that it is block at the right spot.
	addListenerStarted := sync.WaitGroup{}
	addListenerStarted.Add(1)

	addListenerFinished := sync.WaitGroup{}
	addListenerFinished.Add(1)
	go func() {
		// don't allow another goroutine to run in this process
		// this increases the chance that the listener will block a the right spot
		runtime.LockOSThread()
		addListenerStarted.Done()

		assert.NoError(t, detector.AddListener(ctx, listenerName, []propchange.ChangeFilter{{Document: docNameB, Properties: map[string]uint64{"a": 0}}}))

		addListenerFinished.Done()
	}()

	addListenerStarted.Wait()

	// wait some sensible amount of time
	time.Sleep(stopTime.Sub(startTime) * 10)

	// commit change ... second reused listener should not
	// be cancalled by that.
	assert.NoError(t, change.Commit())

	addListenerFinished.Wait()

	change, err = detector.NextChange(ctx)
	assert.NoError(t, err)
	assert.Equal(t, listenerName, change.Listener())
	assert.Contains(t, change.Documents(), docNameB)
	// technically the listener should only contain "docNameB"
	// but because we reused / updated a listener "docNameA" is also allowed
	assert.Subset(t, []string{docNameA, docNameB}, change.Documents())
	assert.NoError(t, change.Commit())

	// cleanup
	assertNoChange(t, ctx, detector)
}

func TestNewDocument(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := fmt.Sprintf("%v", time.Now().UnixNano())
	listenerName := fmt.Sprintf("newdoc-%v", time.Now().UnixNano())

	assertNoChange(t, ctx, detector)

	// listen for document that does not exist
	assert.Nil(t, detector.AddListener(ctx, listenerName, []propchange.ChangeFilter{{Document: docName, NewDocument: true}}), t)

	assertNoChange(t, ctx, detector)

	// add the document
	doc := assertOpenNewDoc(t, ctx, detector, docName)
	assert.Empty(t, doc.GetProperties())
	assert.Nil(t, doc.Commit())

	for i := 0; i < 2; i++ {
		change, err := detector.NextChange(ctx)
		assert.NoError(t, err)
		assertChange(t, change, listenerName, []string{docName})
		assert.NoError(t, change.Commit())

		assertNoChange(t, ctx, detector)

		if i == 0 {
			// listen again, but now the document exists .. this should instantly trigger a change
			assert.NoError(t, detector.AddListener(ctx, listenerName, []propchange.ChangeFilter{
				{Document: docName, NewDocument: true},
			}), t)
		}
	}

	assertNoChange(t, ctx, detector)
}

func TestAbortNewDocument(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := fmt.Sprintf("abort-new-%v", time.Now().UnixNano())

	assert.NoError(t, detector.AddListener(ctx, docName, []propchange.ChangeFilter{{Document: docName, NewDocument: true}}))
	assertNoChange(t, ctx, detector)

	doc := assertOpenNewDoc(t, ctx, detector, docName)
	// if we commit ... the doc would exists ... abort
	assert.NoError(t, doc.Close())

	// doc must not exist
	assertDocNotExists(t, ctx, detector, docName)

	// new doc listern did not trigger
	assertNoChange(t, ctx, detector)

	// cleanup
	assert.NoError(t, detector.DelListener(ctx, docName))
}

func TestAbortChange(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()
	docName := fmt.Sprintf("abort-change-%v", time.Now().UnixNano())

	doc := assertOpenNewDoc(t, ctx, detector, docName)
	assert.NoError(t, doc.Commit())

	assertNoChange(t, ctx, detector)

	assert.NoError(t, detector.AddListener(ctx, docName, []propchange.ChangeFilter{{Document: docName, NewDocument: true}}))

	for i := 0; i < 2; i++ {
		change, err := detector.NextChange(ctx)
		assert.NoError(t, err)
		assertChange(t, change, docName, []string{docName})

		// the some change should not trigger -> is open
		assertNoChange(t, ctx, detector)

		if i == 0 {
			// abort change
			assert.NoError(t, change.Close())
		} else {
			// finally commit change
			assert.NoError(t, change.Commit())
		}
	}

	assertNoChange(t, ctx, detector)
}

func TestSingleChange(detector propchange.Detector, t *testing.T) {
	ctx := context.TODO()

	// prepare a doc with two props
	doc := assertOpenNewDoc(t, ctx, detector, "single")
	assert.NoError(t, doc.SetProperty("a", 1))
	assert.NoError(t, doc.SetProperty("b", 1))
	assert.NoError(t, doc.Commit())

	// listen for both props
	assert.NoError(t, detector.AddListener(ctx, "s", []propchange.ChangeFilter{{Document: "single", Properties: map[string]uint64{"a": 1, "b": 1}}}))

	// change both props
	doc, err := detector.OpenDocument(ctx, "single")
	assert.NoError(t, err)
	assert.NoError(t, doc.SetProperty("a", 2))
	assert.NoError(t, doc.SetProperty("b", 2))
	assert.NoError(t, doc.Commit())

	// This should trigger only one change.
	change, err := detector.NextChange(ctx)
	assert.NoError(t, err)
	assertChange(t, change, "s", []string{"single"})
	// no other open change
	assertNoChange(t, ctx, detector)
	// commit change
	assert.NoError(t, change.Commit())

	// no more changes ...
	assertNoChange(t, ctx, detector)
}

func assertNoChange(t *testing.T, ctx context.Context, detector propchange.Detector) {
	change, err := detector.NextChange(ctx)
	assert.Nil(t, change, "no change expected")
	assert.ErrorIs(t, propchange.ErrNoMoreChanges, err)
	if change != nil {
		require.NoError(t, change.Close())
	}
}

func assertDocExists(t *testing.T, ctx context.Context, detector propchange.Detector, name string) {
	doc, err := detector.OpenDocument(ctx, name)
	assert.NotNil(t, doc)
	assert.NoError(t, err)
	assert.False(t, doc.IsNew())
	assert.NoError(t, doc.Close())
}

func assertDocNotExists(t *testing.T, ctx context.Context, detector propchange.Detector, name string) {
	doc, err := detector.OpenDocument(ctx, name)
	assert.NotNil(t, doc)
	assert.NoError(t, err)
	assert.True(t, doc.IsNew())
	assert.NoError(t, doc.Close())
}

func assertOpenNewDoc(t *testing.T, ctx context.Context, detector propchange.Detector, name string) propchange.DocumentOps {
	doc, err := detector.OpenDocument(ctx, name)
	require.NoError(t, err)
	require.NotNil(t, doc)
	assert.True(t, doc.IsNew())
	return doc
}

func assertChange(t *testing.T, change propchange.OnChange, listener string, docs []string) {
	require.NotNil(t, change)
	assert.Equal(t, listener, change.Listener())
	assert.ElementsMatch(t, docs, change.Documents())
}
