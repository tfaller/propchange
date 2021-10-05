package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/tfaller/go-sqlprepare"
	"github.com/tfaller/propchange"
)

const (
	// MaxNameBytesLen is the maximum length of bytes
	// which can be used for document, property and listener names.
	MaxNameBytesLen = 1024

	newDocProperty = ":new"

	// is a complete new document
	docStateNew = 0
	// document is not new in the database,
	// but the user did not know about this document
	docStateExistingNew = 1
	// the document is a existing document.
	docStateExisting = 2
)

// Detector implements the Detector with
// a MySQL > 8.0.4 backend
type Detector struct {
	db *sql.DB

	// needed prepared statements
	stmtOpenDoc          *sql.Stmt
	stmtDelDoc           *sql.Stmt
	stmtDelDocConvert    *sql.Stmt
	stmtInsertDoc        *sql.Stmt
	stmtCreatedDoc       *sql.Stmt
	stmtDelProp          *sql.Stmt
	stmtDelPropConvert   *sql.Stmt
	stmtGetProperties    *sql.Stmt
	stmtInsertProperty   *sql.Stmt
	stmtUpdateProperty   *sql.Stmt
	stmtNewListener      *sql.Stmt
	stmtAddListenerProps *sql.Stmt
	stmtGetListener      *sql.Stmt
	stmtDelListener      *sql.Stmt
	stmtNextChange       *sql.Stmt
}

// mysqlOpenDoc is an open document
type mysqlOpenDoc struct {
	m        sync.Mutex
	tx       *sql.Tx
	detector *Detector
	closed   bool
	state    int
	docID    uint64
	props    map[string]*mysqlDocProp
}

// mysqlDocProp is used in an open doc
// to handle property changes
type mysqlDocProp struct {
	id       uint64
	name     string
	revision uint64
	changed  bool
	deleted  bool
}

// mysqlChange is a found change, based
// on a previous added listener
type mysqlChange struct {
	tx         *sql.Tx
	detector   *Detector
	listener   string
	listenerID uint64
	documents  []string
}

// listenerFilterImport is used to add a listener
type listenerFilterImport struct {
	Listener   uint64                     `json:"listener"`
	Document   string                     `json:"doc"`
	Properties []listenerFilterImportProp `json:"props"`
}

// listenerFilterImportProp is used to define what
// an newly added listener should track
type listenerFilterImportProp struct {
	Property string `json:"prop"`
	Revision uint64 `json:"rev"`
}

// NewDetector crates a new MySQL detector
func NewDetector(ds string) (*Detector, error) {
	m, err := &Detector{}, error(nil)
	m.db, err = sql.Open("mysql", ds)
	if err != nil {
		return nil, err
	}

	err = m.prepare()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (d *Detector) prepare() (err error) {
	return sqlprepare.Prepare(d.db, []sqlprepare.ToPrepare{
		{Name: "open-doc", Target: &d.stmtOpenDoc,
			Query: "SELECT id FROM document WHERE name = ? FOR UPDATE"},

		{Name: "insert-doc", Target: &d.stmtInsertDoc,
			Query: "INSERT INTO document (name, created) VALUES (?, 0)"},

		{Name: "del-doc", Target: &d.stmtDelDoc,
			Query: "DELETE FROM document WHERE id = ?"},

		{Name: "del-doc-convert", Target: &d.stmtDelDocConvert,
			Query: "UPDATE listener_property SET property = -1, changed = 1 WHERE property IN (SELECT id FROM property WHERE document = ?)"},

		{Name: "del-prop", Target: &d.stmtDelProp,
			Query: "DELETE FROM property WHERE id = ?"},

		{Name: "del-prop-convert", Target: &d.stmtDelPropConvert,
			Query: "UPDATE listener_property SET property = -1, changed = 1 WHERE property = ?"},

		{Name: "insert-prop", Target: &d.stmtInsertProperty,
			Query: "INSERT INTO property (name, document, revision) VALUES (?,?,?)"},

		{Name: "get-props", Target: &d.stmtGetProperties,
			Query: "SELECT id, name, revision FROM property WHERE document = ? FOR UPDATE"},

		{Name: "update-prop", Target: &d.stmtUpdateProperty,
			Query: `
			UPDATE property p LEFT JOIN listener_property lp ON lp.property = p.id
			SET p.revision = ?, lp.changed = lp.expected < ? WHERE p.id = ?`},

		{Name: "insert-listener", Target: &d.stmtNewListener,
			Query: "INSERT INTO listener (name, docs) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), docs = JSON_MERGE_PATCH(docs, VALUES(docs))"},

		// adding listening props is complicated ... we have to lookup the document & property Ids
		// and compare if the listener already should trigger. To do this, without complex dynamic generated
		// SQL, we can use a JSON document -> we have only one network round trip.
		// Note: Because we check if the listener is updated already, we HAVE TO use "SELECT ... FOR SHARE"
		// Otherwise we could have an race condition.
		{Name: "add-listener-props", Target: &d.stmtAddListenerProps,
			Query: `
			INSERT INTO listener_property (listener, property, expected, changed)
			(SELECT j.listener, IFNULL(prop.id, -1), IFNULL(j.rev, 0), IFNULL(j.rev < prop.revision, 1) FROM (
				SELECT * FROM JSON_TABLE(?, '$[*]' COLUMNS(
				doc VARBINARY(1024) PATH '$.doc',
				listener BIGINT PATH '$.listener',
				NESTED PATH '$.props[*]' COLUMNS (
					prop VARBINARY(1024) PATH '$.prop',
					rev BIGINT PATH '$.rev'
				))) AS x) AS j
			LEFT JOIN document doc ON doc.name = j.doc
			LEFT JOIN property prop ON prop.document = doc.id AND prop.name = j.prop FOR SHARE)
			ON DUPLICATE KEY UPDATE expected = LEAST(VALUES(expected), expected), changed = (VALUES(changed) OR changed)
			`},

		{Name: "get-listener", Target: &d.stmtGetListener,
			Query: `SELECT d.name, p.name, lp.expected FROM listener l
					JOIN listener_property lp ON lp.listener = l.id
					JOIN property p ON p.id = lp.property
					JOIN document d ON d.id = p.document
					WHERE l.name = ?
					ORDER BY d.id`},

		{Name: "del-listener", Target: &d.stmtDelListener,
			Query: "DELETE FROM listener WHERE name = ?"},

		// This query finds the next change ... we have to LOCK the listener_property and
		// the whole listener. With the lock we make sure that a concurent NextChange
		// skips this listener. Otherwise the same listener could be triggered multiples times,
		// if multiple properties of the same listener changed at the same time. Also, we have to
		// do a "sub-query", a direct JOIN would lock unrelated changes.
		{Name: "nxt-change", Target: &d.stmtNextChange,
			Query: `SELECT l.id, l.name, JSON_KEYS(l.docs) AS docs FROM 
					(SELECT * FROM listener_property WHERE changed = 1 LIMIT 1 FOR UPDATE SKIP LOCKED) lp
					JOIN listener l ON l.id = lp.listener FOR UPDATE SKIP LOCKED
					`},

		{Name: "doc-created", Target: &d.stmtCreatedDoc,
			Query: `UPDATE document SET created = 1 WHERE id = ?`},
	}...)
}

// OpenDocument opens a doc as defined in the Detector specification
func (d *Detector) OpenDocument(ctx context.Context, name string) (propchange.DocumentOps, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("can't open transaction: %w", err)
	}

	// perform the action in a newly created tx.
	// if an error happend close the TX
	doc, err := d.doOpenDocument(name, tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	return doc, nil
}

func (d *Detector) doOpenDocument(name string, tx *sql.Tx) (*mysqlOpenDoc, error) {
	if len := len(name); len > MaxNameBytesLen {
		return nil, &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

	// first try to open the doc ...
	// maybe it exists already
	doc, err := d.tryOpenDoc(name, tx)
	if err == nil {
		// the doc exists
		return doc, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("can't open document: %w", err)
	}

	// the doc is new ... create it
	return d.insertNewDoc(name, tx)
}

func (d *Detector) tryOpenDoc(name string, tx *sql.Tx) (*mysqlOpenDoc, error) {
	row := tx.Stmt(d.stmtOpenDoc).QueryRow(name)
	docID := uint64(0)
	err := row.Scan(&docID)
	if err != nil {
		return nil, err
	}

	// the doc exists ... get the props
	rows, err := tx.Stmt(d.stmtGetProperties).Query(docID)
	if err != nil {
		return nil, err
	}

	// load the props
	props := map[string]*mysqlDocProp{}
	for rows.Next() {
		prop := &mysqlDocProp{}
		err = rows.Scan(&prop.id, &prop.name, &prop.revision)
		if err != nil {
			return nil, err
		}
		props[prop.name] = prop
	}

	state := docStateExisting
	if newProp := props[newDocProperty]; newProp.revision == 0 {
		state = docStateExistingNew
	}

	return &mysqlOpenDoc{tx: tx, docID: docID, detector: d, state: state, props: props}, nil
}

func (d *Detector) insertNewDoc(name string, tx *sql.Tx) (*mysqlOpenDoc, error) {
	res, err := tx.Stmt(d.stmtInsertDoc).Exec(name)
	if err != nil {
		return nil, err
	}
	docID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	// insert was successfull
	return &mysqlOpenDoc{tx: tx, docID: uint64(docID), state: docStateNew, detector: d, props: map[string]*mysqlDocProp{}}, nil
}

// AddListener adds a new Listener. Like defined in the Detector interface
func (d *Detector) AddListener(ctx context.Context, name string, filter []propchange.ChangeFilter) error {
	if name == "" {
		return propchange.ErrInvalidListenerName
	}
	if len(filter) == 0 {
		return propchange.ErrEmptyFilter
	}
	if len := len(name); len > MaxNameBytesLen {
		return &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

retry:
	try := 1

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("can't open transaction: %w", err)
	}

	err = d.doAddListener(name, filter, tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("rollback failed with %q for error: %w", rbErr, err)
		}

		mysqlErr := &mysql.MySQLError{}
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1213 && try < 2 {
			// deadlock error, this can happen if we access an existing listener
			// which was already triggered and will be now deleted ... rare case
			try++
			goto retry
		}

		return err
	}
	return tx.Commit()
}

func (d *Detector) doAddListener(name string, filter []propchange.ChangeFilter, tx *sql.Tx) error {
	// save the listener itself
	docsJSON := map[string]bool{}
	for _, f := range filter {
		docsJSON[f.Document] = true
	}
	docsJSONRaw, err := json.Marshal(docsJSON)
	if err != nil {
		return err
	}
	res, err := tx.Stmt(d.stmtNewListener).Exec(name, docsJSONRaw)
	if err != nil {
		return fmt.Errorf("insert new listener failed: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	// build json document that is used to bulk import the listener data
	insertJSON := make([]listenerFilterImport, len(filter))
	for idx, f := range filter {
		if f.NewDocument {
			if len(f.Properties) > 0 {
				// Discurage the usage of NewDocument and Properties at the same time.
				return fmt.Errorf("NewDocument can't be used if properties are set")
			}

			// listener wants to listen to the creation of the document
			// it is possible that it does not exists yet ...
			doc, err := d.doOpenDocument(f.Document, tx)
			if err != nil {
				return err
			}

			if doc.state == docStateNew {
				if err := doc.newProperty(newDocProperty, 0); err != nil {
					return err
				}
			}

			// add filter property
			f.Properties = map[string]uint64{newDocProperty: 0}
		}

		if len(f.Properties) == 0 {
			return propchange.ErrEmptyFilter
		}

		insertJSON[idx].Document = f.Document
		insertJSON[idx].Listener = uint64(id)

		props := make([]listenerFilterImportProp, len(f.Properties))
		propIdx := 0
		for prop, rev := range f.Properties {
			props[propIdx].Property = prop
			props[propIdx].Revision = rev
			propIdx++
		}
		insertJSON[idx].Properties = props
	}

	insertJSONRaw, err := json.Marshal(insertJSON)
	if err != nil {
		return err
	}

	// inster properties
	_, err = tx.Stmt(d.stmtAddListenerProps).Exec(insertJSONRaw)
	return err
}

func (d *Detector) GetListener(ctx context.Context, listener string) ([]propchange.ChangeFilter, error) {
	row, err := d.stmtGetListener.QueryContext(ctx, listener)
	if err != nil {
		return nil, err
	}
	defer row.Close()

	filters := []propchange.ChangeFilter{}
	currentFilter := (*propchange.ChangeFilter)(nil)

	var docName, propName string
	var propExpectedRev uint64

	for row.Next() {
		err = row.Scan(&docName, &propName, &propExpectedRev)
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(docName, ":") {
			// internal document
			continue
		}

		if currentFilter == nil || currentFilter.Document != docName {
			filters = append(filters, propchange.ChangeFilter{Document: docName})
			currentFilter = &filters[len(filters)-1]
		}

		if propName == newDocProperty {
			currentFilter.NewDocument = true
		} else {
			if currentFilter.Properties == nil {
				currentFilter.Properties = map[string]uint64{}
			}
			currentFilter.Properties[propName] = propExpectedRev
		}
	}

	if err = row.Err(); err != nil {
		return nil, err
	}

	if len(filters) == 0 {
		return nil, propchange.ErrListenerDoesNotExist(listener)
	}

	return filters, nil
}

// DelListener deletes a listener as specified by the Detector service
func (d *Detector) DelListener(ctx context.Context, listener string) error {
	_, err := d.stmtDelListener.ExecContext(ctx, listener)
	return err
}

// NextChange gets the next change event, as specified by the Detector service
func (d *Detector) NextChange(ctx context.Context) (propchange.OnChange, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("can't open transaction: %w", err)
	}

	// try to get the next change ...
	// if an error occurs close the TX
	change, err := d.doNextChange(tx)
	if err != nil {
		tx.Rollback()
	}
	return change, err
}

func (d *Detector) doNextChange(tx *sql.Tx) (propchange.OnChange, error) {
	stmt := tx.Stmt(d.stmtNextChange)
	defer stmt.Close()

	change := &mysqlChange{tx: tx, detector: d}

	var docs []byte
	err := stmt.QueryRow().Scan(&change.listenerID, &change.listener, &docs)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, propchange.ErrNoMoreChanges
		}
		return nil, fmt.Errorf("can't read next change: %w", err)
	}

	err = json.Unmarshal(docs, &change.documents)
	if err != nil {
		return nil, err
	}

	return change, nil
}

// Close closes the document and aborts all changes
func (m *mysqlOpenDoc) Close() error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosed
	}

	m.closed = true
	return m.tx.Rollback()
}

// Commit applies the changes and closes the document
// no additional operations can be done.
func (m *mysqlOpenDoc) Commit() error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosed
	}

	m.closed = true
	defer m.tx.Rollback()

	if m.state != docStateExisting {
		// the user created the document for the first time
		if newProp := m.props[newDocProperty]; newProp != nil {
			newProp.revision++
			newProp.changed = true
		} else {
			err := m.newProperty(newDocProperty, 1)
			if err != nil {
				return err
			}
		}

		// mark document as created by user
		_, err := m.tx.Stmt(m.detector.stmtCreatedDoc).Exec(m.docID)
		if err != nil {
			return err
		}
	}

	// apply changes now ...
	stmt := m.tx.Stmt(m.detector.stmtUpdateProperty)
	stmtDel := m.tx.Stmt(m.detector.stmtDelProp)
	stmtDelConv := m.tx.Stmt(m.detector.stmtDelPropConvert)

	for _, prop := range m.props {
		if !prop.changed {
			continue
		}

		if prop.deleted {
			_, err := stmtDelConv.Exec(prop.id)
			if err != nil {
				return err
			}
			_, err = stmtDel.Exec(prop.id)
			if err != nil {
				return err
			}
			continue
		}

		// yes, rev twice ... mysql has cant use args twice
		_, err := stmt.Exec(prop.revision, prop.revision, prop.id)
		if err != nil {
			return err
		}
	}

	return m.tx.Commit()
}

func (m *mysqlOpenDoc) DelProperty(name string) error {
	if name == "" || strings.HasPrefix(name, ":") {
		return propchange.ErrInvalidPropertyName(name)
	}

	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosed
	}

	prop := m.props[name]
	if prop == nil {
		// prop does not exists
		return nil
	}

	prop.deleted = true
	prop.changed = true
	return nil
}

func (m *mysqlOpenDoc) Delete() error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosed
	}

	m.closed = true
	defer m.tx.Rollback()

	if m.state != docStateExisting {
		// doc does not exists for real.
		return nil
	}

	// trigger listeners that listen for this doc props
	_, err := m.tx.Stmt(m.detector.stmtDelDocConvert).Exec(m.docID)
	if err != nil {
		return err
	}

	_, err = m.tx.Stmt(m.detector.stmtDelDoc).Exec(m.docID)
	if err != nil {
		return err
	}

	// delete does an implicit commit
	return m.tx.Commit()
}

func (m *mysqlOpenDoc) GetProperties() map[string]uint64 {
	m.m.Lock()
	defer m.m.Unlock()

	props := map[string]uint64{}
	for p, v := range m.props {
		if !strings.HasPrefix(p, ":") && !v.deleted {
			props[p] = v.revision
		}
	}
	return props
}

func (m *mysqlOpenDoc) SetProperty(name string, rev uint64) error {
	if name == "" || strings.HasPrefix(name, ":") {
		return propchange.ErrInvalidPropertyName(name)
	}
	if len := len(name); len > MaxNameBytesLen {
		return &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosed
	}

	prop := m.props[name]
	if prop == nil {
		return m.newProperty(name, rev)
	}
	prop.deleted = false
	if prop.revision >= rev {
		// value is already the same ... do nothing
		return nil
	}
	prop.revision = rev
	prop.changed = true
	return nil
}

func (m *mysqlOpenDoc) newProperty(name string, rev uint64) error {
	stmt := m.tx.Stmt(m.detector.stmtInsertProperty)
	defer stmt.Close()

	res, err := stmt.Exec(name, m.docID, rev)
	if err != nil {
		return fmt.Errorf("can't insert new property: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	m.props[name] = &mysqlDocProp{
		id:       uint64(id),
		name:     name,
		revision: rev,
	}
	return nil
}

func (m *mysqlOpenDoc) IsNew() bool {
	return m.state != docStateExisting
}

func (m *mysqlChange) Listener() string {
	return m.listener
}

func (m *mysqlChange) Documents() []string {
	return m.documents
}

func (m *mysqlChange) Close() error {
	return m.tx.Rollback()
}

// Commit closes the change and removes it from
// the assigned listener.
func (m *mysqlChange) Commit() error {
	stmt := m.tx.Stmt(m.detector.stmtDelListener)
	defer stmt.Close()

	_, err := stmt.Exec(m.listener)
	if err != nil {
		return fmt.Errorf("can't delete listener: %w", err)
	}
	return m.tx.Commit()
}

var _ propchange.Detector = (*Detector)(nil)
