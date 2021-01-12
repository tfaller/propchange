package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tfaller/go-sqlprepare"
	"github.com/tfaller/propchange"
)

// MaxNameBytesLen is the maximum length of bytes
// which can be used for document, property and listener names.
const MaxNameBytesLen = 1024

var errInvalidName = errors.New("invalid name")

// Detector implements the Detector with
// a MySQL > 8.0.4 backend
type Detector struct {
	db *sql.DB

	// needed prepared statements
	stmtOpenDoc          *sql.Stmt
	stmtDelDoc           *sql.Stmt
	stmtInsertDoc        *sql.Stmt
	stmtDelProperty      *sql.Stmt
	stmtGetProperties    *sql.Stmt
	stmtInsertProperty   *sql.Stmt
	stmtUpdateProperty   *sql.Stmt
	stmtNewListener      *sql.Stmt
	stmtAddListenerProps *sql.Stmt
	stmtAddListenerDocs  *sql.Stmt
	stmtDelListener      *sql.Stmt
	stmtNextChange       *sql.Stmt
}

// mysqlOpenDoc is an open document
type mysqlOpenDoc struct {
	m        sync.Mutex
	tx       *sql.Tx
	detector *Detector
	closed   bool
	isNew    bool
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
			Query: "INSERT INTO document (name) VALUES (?)"},

		{Name: "del-doc", Target: &d.stmtDelDoc,
			Query: "DELETE FROM document WHERE id = ?"},

		{Name: "del-prop", Target: &d.stmtDelProperty,
			Query: "DELETE FROM property WHERE id = ?"},

		{Name: "insert-prop", Target: &d.stmtInsertProperty,
			Query: "INSERT INTO property (name, document, revision) VALUES (?,?,?)"},

		{Name: "get-props", Target: &d.stmtGetProperties,
			Query: "SELECT id, name, revision FROM property WHERE document = ? FOR UPDATE"},

		{Name: "update-prop", Target: &d.stmtUpdateProperty,
			Query: `
			UPDATE property p LEFT JOIN listener_property lp ON lp.property = p.id
			SET p.revision = ?, lp.changed = lp.expected < ? WHERE p.id = ?`},

		{Name: "insert-listener", Target: &d.stmtNewListener,
			Query: "INSERT INTO listener (name) VALUES (?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)"},

		// adding listening props is complicated ... we have to lookup the document & property Ids
		// and compare if the listener already should trigger. To do this, without complex dynamic generated
		// SQL, we can use a JSON document -> we have only one network round trip.
		// Note: Because we check if the listener is updated already, we HAVE TO use "SELECT ... FOR UPDATE"
		// Otherwise we could have an race condition.
		{Name: "add-listener-props", Target: &d.stmtAddListenerProps,
			Query: `
			INSERT INTO listener_property (listener, property, expected, changed)
			(SELECT j.listener, prop.id, j.rev, (j.rev < prop.revision) FROM (
				SELECT * FROM JSON_TABLE(?, '$[*]' COLUMNS(
				doc VARBINARY(1024) PATH '$.doc',
				listener BIGINT PATH '$.listener',
				NESTED PATH '$.props[*]' COLUMNS (
					prop VARBINARY(1024) PATH '$.prop',
					rev BIGINT PATH '$.rev'
				))) AS x) AS j
			JOIN document doc ON doc.name = j.doc
			JOIN property prop ON prop.document = doc.id AND prop.name = j.prop FOR UPDATE)
			ON DUPLICATE KEY UPDATE expected = LEAST(VALUES(expected), expected), changed = (VALUES(changed) OR changed)
			`},

		{Name: "add-listener-docs", Target: &d.stmtAddListenerDocs,
			Query: `
			INSERT INTO listener_document (listener, document)
			SELECT j.listener, doc.id FROM JSON_TABLE(?, '$[*]' COLUMNS(
				doc VARCHAR(2048) PATH '$.doc',
				listener BIGINT PATH '$.listener')
			) AS j
			JOIN document doc ON doc.name = j.doc
			ON DUPLICATE KEY UPDATE document = document`,
		},

		{Name: "del-listener", Target: &d.stmtDelListener,
			Query: "DELETE FROM listener WHERE name = ?"},

		// This query finds the next change ... we have to LOCK the listener_property and
		// the whole listener (by JOIN-ing the listener). With the lock we make sure that a concurent NextChange
		// skips this listener. Otherwise the same listener could be triggered multiples times,
		// if multiple properties of the same listener changed at the same time
		{Name: "nxt-change", Target: &d.stmtNextChange,
			Query: `
			SELECT l.id, l.name, 
				(SELECT GROUP_CONCAT(d.name) FROM listener_document ld JOIN document d ON d.id = ld.document WHERE ld.listener = l.id) docs
			FROM listener_property lp JOIN listener l ON l.id = lp.listener
			WHERE lp.changed = 1 LIMIT 1 FOR UPDATE SKIP LOCKED`},
	}...)
}

// OpenDocument opens a doc as defined in the Detector specification
func (d *Detector) OpenDocument(ctx context.Context, name string) (propchange.DocumentOps, error) {
	if len := len(name); len > MaxNameBytesLen {
		return nil, &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

	if strings.Contains(name, ",") {
		return nil, errInvalidName
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("Cant open transaction: %w", err)
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

func (d *Detector) doOpenDocument(name string, tx *sql.Tx) (propchange.DocumentOps, error) {
	// first try to open the doc ...
	// maybe it exists already
	doc, err := d.tryOpenDoc(name, tx)
	if err == nil {
		// the doc exists
		return doc, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("Can't open document: %w", err)
	}

	// the doc is new ... create it
	return d.insertNewDoc(name, tx)
}

func (d *Detector) tryOpenDoc(name string, tx *sql.Tx) (propchange.DocumentOps, error) {
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

	return &mysqlOpenDoc{tx: tx, docID: docID, detector: d, props: props}, nil
}

func (d *Detector) insertNewDoc(name string, tx *sql.Tx) (propchange.DocumentOps, error) {
	res, err := tx.Stmt(d.stmtInsertDoc).Exec(name)
	if err != nil {
		return nil, err
	}
	docID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	// insert was successfull
	return &mysqlOpenDoc{tx: tx, docID: uint64(docID), isNew: true, detector: d, props: map[string]*mysqlDocProp{}}, nil
}

// AddListener adds a new Listener. Like defined in the Detector interface
func (d *Detector) AddListener(ctx context.Context, name string, filter []propchange.ChangeFilter) error {
	if len := len(name); len > MaxNameBytesLen {
		return &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("Can't open transaction: %w", err)
	}

	err = d.doAddListener(name, filter, tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("Rollback failed with %q for error: %w", rbErr, err)
		}
		return err
	}
	return tx.Commit()
}

func (d *Detector) doAddListener(name string, filter []propchange.ChangeFilter, tx *sql.Tx) error {
	// save the listener itself
	res, err := tx.Stmt(d.stmtNewListener).Exec(name)
	if err != nil {
		return fmt.Errorf("Insert new listener failed: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	// build json document that is used to bulk import the listener data
	insertJSON := make([]listenerFilterImport, len(filter))
	for idx, f := range filter {
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
	if err != nil {
		return err
	}

	// assign docs
	_, err = tx.Stmt(d.stmtAddListenerDocs).Exec(insertJSONRaw)
	return err
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
		return nil, err
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

	var docs string
	err := stmt.QueryRow().Scan(&change.listenerID, &change.listener, &docs)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, propchange.ErrNoMoreChanges
		}
		return nil, err
	}

	// docs is simply a comma seperated list of documents
	change.documents = strings.Split(docs, ",")

	return change, nil
}

// Close closes the document and aborts all changes
func (m *mysqlOpenDoc) Close() error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosedError
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
		return propchange.ErrDocAlreadyClosedError
	}

	// apply changes now ...
	stmt := m.tx.Stmt(m.detector.stmtUpdateProperty)
	defer stmt.Close()
	for _, prop := range m.props {
		if !prop.changed {
			continue
		}
		// yes, rev twice ... mysql has cant use args twice
		_, err := stmt.Exec(prop.revision, prop.revision, prop.id)
		if err != nil {
			m.closed = true
			m.tx.Rollback()
			return err
		}
	}
	m.closed = true
	return m.tx.Commit()
}

func (m *mysqlOpenDoc) DelProperty(name string) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosedError
	}

	prop := m.props[name]
	if prop == nil {
		// prop does not exists
		return nil
	}

	stmt := m.tx.Stmt(m.detector.stmtDelProperty)
	defer stmt.Close()

	_, err := stmt.Exec(prop.id)
	if err != nil {
		return err
	}
	delete(m.props, name)
	return nil
}

func (m *mysqlOpenDoc) Delete() error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosedError
	}

	stmt := m.tx.Stmt(m.detector.stmtDelDoc)
	defer stmt.Close()

	_, err := stmt.Exec(m.docID)
	if err != nil {
		return err
	}

	// delete does an implicit commit
	m.closed = true
	return m.tx.Commit()
}

func (m *mysqlOpenDoc) GetProperties() map[string]uint64 {
	m.m.Lock()
	defer m.m.Unlock()

	props := map[string]uint64{}
	for p, v := range m.props {
		props[p] = v.revision
	}
	return props
}

func (m *mysqlOpenDoc) SetProperty(name string, rev uint64) error {
	if len := len(name); len > MaxNameBytesLen {
		return &propchange.ErrTooLongName{Name: name, Len: len, MaxLen: MaxNameBytesLen}
	}

	m.m.Lock()
	defer m.m.Unlock()
	if m.closed {
		return propchange.ErrDocAlreadyClosedError
	}

	prop := m.props[name]
	if prop == nil {
		return m.newProperty(name, rev)
	}
	if prop.revision == rev {
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
		return err
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
	return m.isNew
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
		return err
	}
	return m.tx.Commit()
}

var _ propchange.Detector = (*Detector)(nil)
