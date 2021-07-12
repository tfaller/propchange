package mem

import (
	"context"
	"fmt"
	"sync"

	"github.com/tfaller/propchange"
)

// mem holds the complete state of the mem detector.
type mem struct {
	mDocs, mChanges, mListeners sync.Mutex

	docs        map[string]*doc
	changes     map[*listener]struct{}
	changesOpen map[*listener]struct{}
	listeners   map[string]*listener
}

// doc is registered document
type doc struct {
	m      sync.Mutex
	name   string
	exists bool
	props  map[string]*prop
}

// prop is a property of a doc
type prop struct {
	m         sync.Mutex
	revision  uint64
	listeners *listenerChain
}

// listeners listens to doc/prop changes
type listener struct {
	m     sync.Mutex
	name  string
	docs  map[string]struct{}
	props map[*prop]*listenerChain
}

// listenerChain is a doubly linked list
// that is used to listen for prop changes
type listenerChain struct {
	revision   uint64
	listener   *listener
	prev, next *listenerChain
}

// openDoc is an open doc
type openDoc struct {
	mem      *mem
	doc      *doc
	props    map[string]uint64
	propsDel map[string]struct{}
}

// openChange is an open change
type openChange struct {
	mem      *mem
	docs     []string
	listener *listener
}

// NewMem returns an in-memory propchange detector.
func NewMem() propchange.Detector {
	return &mem{
		docs:        map[string]*doc{},
		changes:     map[*listener]struct{}{},
		changesOpen: map[*listener]struct{}{},
		listeners:   map[string]*listener{},
	}
}

func (m *mem) AddListener(ctx context.Context, name string, filter []propchange.ChangeFilter) error {
	if name == "" {
		return propchange.ErrInvalidListenerName
	}
	if len(filter) == 0 {
		return propchange.ErrEmptyFilter
	}
	for _, f := range filter {
		if len(f.Properties) == 0 && !f.NewDocument {
			return propchange.ErrEmptyFilter
		}
	}

	l := m.getOrCreateListener(name)
	defer l.m.Unlock()

	// should the listener be triggered instantly?
	shouldTrigger := false

	for _, f := range filter {

		// reg as listen doc
		l.docs[f.Document] = struct{}{}

		if f.NewDocument {

			d := m.getDocOrNewDoc(f.Document)
			if d.exists {
				shouldTrigger = true
			} else {
				// add listen property
				prop := d.getOrCreateProp("")
				l.props[prop] = prop.listenTo(l, 0)
				prop.m.Unlock()
			}

			d.m.Unlock()
			continue
		}

		d := m.getDoc(f.Document)
		if d == nil || !d.exists {
			shouldTrigger = true
			if d != nil {
				d.m.Unlock()
			}
			continue
		}

		for p, rev := range f.Properties {
			prop := d.props[p]
			if prop == nil {
				shouldTrigger = true
				break
			}

			if _, exists := l.props[prop]; exists {
				// we listen already for this prop
				continue
			}

			prop.m.Lock()
			if prop.revision > rev {
				// already trigger
				shouldTrigger = true
				prop.m.Unlock()
				break
			}
			l.props[prop] = prop.listenTo(l, rev)
			prop.m.Unlock()
		}

		d.m.Unlock()
	}

	if shouldTrigger {
		m.addChange(l)
	}

	return nil
}

func (m *mem) DelListener(ctx context.Context, name string) error {
	l := m.getAndRemoveListener(name)
	if l == nil {
		return nil
	}
	defer l.m.Unlock()

	l.delete()

	return nil
}

func (m *mem) NextChange(ctx context.Context) (propchange.OnChange, error) {
	m.mChanges.Lock()
	defer m.mChanges.Unlock()

	for l := range m.changesOpen {
		l.m.Lock()
		delete(m.changesOpen, l)
		return &openChange{listener: l, mem: m}, nil
	}

	return nil, propchange.ErrNoMoreChanges
}

func (m *mem) OpenDocument(ctx context.Context, name string) (propchange.DocumentOps, error) {
	return &openDoc{
		mem:      m,
		doc:      m.getDocOrNewDoc(name),
		props:    map[string]uint64{},
		propsDel: map[string]struct{}{},
	}, nil
}

func (m *mem) getOrCreateListener(name string) *listener {
	m.mListeners.Lock()
	defer m.mListeners.Unlock()

	l := m.listeners[name]

	if l == nil {
		l = &listener{
			name:  name,
			docs:  map[string]struct{}{},
			props: map[*prop]*listenerChain{},
		}
		m.listeners[name] = l
	}

	l.m.Lock()
	return l
}

func (m *mem) getAndRemoveListener(name string) *listener {
	m.mListeners.Lock()
	defer m.mListeners.Unlock()

	l := m.listeners[name]
	if l == nil {
		return nil
	}

	delete(m.listeners, name)
	l.m.Lock()
	return l
}

func (m *mem) getDoc(name string) *doc {
	m.mDocs.Lock()
	defer m.mDocs.Unlock()

	d := m.docs[name]
	if d == nil {
		return nil
	}

	d.m.Lock()
	return d
}

func (m *mem) getDocOrNewDoc(name string) *doc {
	m.mDocs.Lock()
	defer m.mDocs.Unlock()

	d := m.docs[name]
	if d == nil {
		d = &doc{
			name:  name,
			props: map[string]*prop{},
		}
		m.docs[name] = d
	}

	d.m.Lock()
	return d
}

func (m *mem) addChange(l *listener) {
	m.mChanges.Lock()
	defer m.mChanges.Unlock()

	m.changes[l] = struct{}{}

	if _, exists := m.changesOpen[l]; !exists {
		m.changesOpen[l] = struct{}{}
	}
}

func (o *openDoc) Close() error {
	if o.doc == nil {
		return propchange.ErrDocAlreadyClosedError
	}

	o.doc.m.Unlock()
	o.doc = nil
	return nil
}

func (o *openDoc) Commit() error {
	if o.doc == nil {
		return propchange.ErrDocAlreadyClosedError
	}

	if !o.doc.exists {
		if newProp := o.doc.props[""]; newProp != nil {
			newProp.m.Lock()
			newProp.triggerChange(o.mem, ^uint64(0))
			newProp.m.Unlock()
			delete(o.doc.props, "")
		}
		o.doc.exists = true
	}

	for prop := range o.propsDel {
		p := o.doc.props[prop]
		if p != nil {
			p.m.Lock()
			p.triggerChange(o.mem, ^uint64(0))
			p.m.Unlock()
		}
		delete(o.doc.props, prop)
	}

	for name, rev := range o.props {
		prop := o.doc.getOrCreateProp(name)
		prop.triggerChange(o.mem, rev)
		prop.m.Unlock()
	}

	o.doc.m.Unlock()
	o.doc = nil
	return nil
}

func (o *openDoc) GetProperties() map[string]uint64 {
	props := map[string]uint64{}

	for name, prop := range o.doc.props {
		if name == "" {
			continue
		}
		if _, wasDel := o.propsDel[name]; wasDel {
			continue
		}
		if rev := o.props[name]; rev > prop.revision {
			props[name] = rev
		} else {
			props[name] = prop.revision
		}
	}

	for name, rev := range o.props {
		props[name] = rev
	}

	return props
}

func (o *openDoc) DelProperty(name string) error {
	if o.doc == nil {
		return propchange.ErrDocAlreadyClosedError
	}
	delete(o.props, name)
	o.propsDel[name] = struct{}{}
	return nil
}

func (d *openDoc) Delete() error {
	if d.doc == nil {
		return propchange.ErrDocAlreadyClosedError
	}
	defer d.doc.m.Unlock()

	if !d.doc.exists {
		return nil
	}

	d.mem.mDocs.Lock()
	delete(d.mem.docs, d.doc.name)
	d.mem.mDocs.Unlock()

	for _, prop := range d.doc.props {
		prop.m.Lock()
		prop.triggerChange(d.mem, ^uint64(0))
		prop.m.Unlock()
	}

	d.doc = nil
	return nil
}

func (o *openDoc) IsNew() bool {
	return !o.doc.exists
}

func (o *openDoc) SetProperty(name string, rev uint64) error {
	if o.doc == nil {
		return propchange.ErrDocAlreadyClosedError
	}
	if name == "" {
		return fmt.Errorf("empty string is not a valid property name")
	}
	if _, wasDel := o.propsDel[name]; wasDel {
		delete(o.propsDel, name)
	}
	if p, op := o.doc.props[name], o.props[name]; (p == nil || p.revision < rev) && (op == 0 || op < rev) {
		o.props[name] = rev
	}
	return nil
}

func (o *openChange) Close() error {
	o.mem.mChanges.Lock()
	defer o.mem.mChanges.Unlock()

	o.mem.changesOpen[o.listener] = struct{}{}
	o.listener.m.Unlock()

	return nil
}

func (o *openChange) Commit() error {

	o.mem.mListeners.Lock()
	delete(o.mem.listeners, o.listener.name)
	o.mem.mListeners.Unlock()

	o.mem.mChanges.Lock()
	delete(o.mem.changes, o.listener)
	o.mem.mChanges.Unlock()

	o.listener.delete()

	return nil
}

func (o *openChange) Documents() []string {

	if o.docs == nil {
		o.docs = make([]string, 0, len(o.listener.docs))
		for doc := range o.listener.docs {
			o.docs = append(o.docs, doc)
		}
	}

	return o.docs
}

func (o *openChange) Listener() string {
	return o.listener.name
}

func (d *doc) getOrCreateProp(name string) *prop {
	p := d.props[name]

	if p == nil {
		p = &prop{}
		d.props[name] = p
	}

	p.m.Lock()
	return p
}

// listenTo insters a listener into de doubly linked listener chain
func (p *prop) listenTo(l *listener, revision uint64) *listenerChain {
	current := p.listeners

	if current != nil {
		for revision > current.revision && current.next != nil {
			current = current.next
		}
	}

	current = &listenerChain{
		revision: revision,
		listener: l,
		prev:     current,
	}

	if current.prev == nil {
		p.listeners = current
	} else {
		current.next = current.prev.next
		current.prev.next = current
	}

	return current
}

func (p *prop) triggerChange(m *mem, rev uint64) {
	if p.revision >= rev {
		return
	}

	current := p.listeners

	for current != nil && current.revision <= rev {
		m.addChange(current.listener)
		current = current.next
	}

	p.revision = rev
	p.listeners = current
}

func (l *listener) delete() {
	for prop, chain := range l.props {
		prop.m.Lock()

		if chain.prev == nil {
			prop.listeners = chain.next
			if prop.listeners != nil {
				prop.listeners.prev = nil
			}
		} else {
			chain.prev.next = chain.next
			chain.prev.prev = chain.prev
		}

		prop.m.Unlock()
	}
}
