package propchange

import (
	"context"
	"errors"
	"fmt"
)

type ErrInvalidPropertyName string

type ErrListenerDoesNotExist string

// ErrDocAlreadyClosed indicates that the operation failed
// because the document was already closed.
var ErrDocAlreadyClosed = errors.New("doc was already closed")

// ErrNoMoreChanges indicates that no more changes were found
var ErrNoMoreChanges = errors.New("no more new changes found")

// ErrEmptyFilter indicates that no filter was provied.
var ErrEmptyFilter = errors.New("listener has no filter set")

// ErrInvalidListenerName indicates that a given listener name was invalid
var ErrInvalidListenerName = errors.New("listener name is invalid")

// Detector is a service that emits an event if given properties changed of an document.
// With this service it is possible to answer the question:
// "If X changes, what is affected by this change?"
// Simple question, but hard to answer, if 10, rather losely coupled documents (from multiple
// sources), might be affected out ouf millions of documents.
// A document is just a named collection of named properties. Each property has a revision
// number assigned to it. The revision number can only increase, never decrease.
// Changes to a document are indicated by changing a revisions of properties.
// Listeners can listen to changes of specific properties. A listener can listen to
// multiple properties, regardless of the document. If a single property changes of
// the defined filter, the listener is triggered.
// The "What changed?" question can be answered by simply using clever listeners.
// If we know, that property "@" of document "A" affects also document "B", we create
// one listener that listens to the "@" of "A" and also include "B" in the listener filter.
// Now, if "@" is changed, the listener gets trigger. We know, that we have to now do something
// with "B" (and maybe with "A" as well), because the listener was triggered. Note: A triggered
// listeners tells us only what documents were involved in the listener, not what actually changed.
// This is currently because of simplicity. This might change in the future.
// If the same listener also listened for property "#" of document "C", we could not say whether the
// change of "@" or "#" caused the listener to trigger. So a listener tells us THAT something changed,
// but not WHAT EXACTLY changed. We only know that at least one property of any of the filtered documents
// got changed. So one lister should only filter multiple documents if they are really effacted by eachother.
type Detector interface {
	// OpenDocument opens a document to perform
	// operation on the document
	OpenDocument(ctx context.Context, name string) (DocumentOps, error)

	// AddListener creates or updates a listener, that watches for properties to change.
	// If a listener already exists, that listeners gets updated. The given filter
	// is simply "appended" to the already existing listener. So if the existing listener
	// listened to doc "A", and the "new" listener filtered for doc "B", the updated
	// listener listens for document "A" and "B".
	// If a document or property does not exist, the listener will get instantly triggered.
	// This is because the property or document maybe existed, but got deleted in the meantime.
	// If it's known that a document does not exist, instead of listening for properties, the
	// listener can listen for document creation. A filter can either listen for the creation of
	// the document or for properties. This is because the filter would always trigger right away
	// if the document already exists.
	AddListener(ctx context.Context, name string, filter []ChangeFilter) error

	// GetListener gets the filter that represents a given listener. This function is basically there
	// to check whether a listener / filter exists. The returned filter might not represent the full
	// added filter. This can happen if a listener listens to non existing properties or documents.
	// Also, an already triggered listener might be returned as not existing.
	GetListener(ctx context.Context, name string) ([]ChangeFilter, error)

	// DelListener deletes a given listener. If the listeners does not exist, it is silently ignored.
	DelListener(ctx context.Context, listener string) error

	// NextChange returns the next change that was not
	// yet processed
	NextChange(ctx context.Context) (OnChange, error)
}

// ChangeFilter is the filter for that a listener listens.
// A event for this listener is triggered, if the new property revision
// is larger than the revision of the property in the filter
type ChangeFilter struct {
	Document    string
	NewDocument bool
	Properties  map[string]uint64
}

// OnChange is an change event
type OnChange interface {
	// Documents which are attached to this event
	// This inclues all documents that the listener registered to,
	// not just the document of which properties changed.
	Documents() []string
	// Listener which listened for the change
	Listener() string
	// Commit confirms that the event was processed.
	// The listener is removed from the system.
	Commit() error
	// Close closes but not commits the event.
	// The event will be later triggered again.
	Close() error
}

// DocumentOps are operations possible on an open
// document.
type DocumentOps interface {
	// IsNew tells that commiting this document,
	// causes to create a new document.
	IsNew() bool

	// GetProperties gets the current set properties
	GetProperties() map[string]uint64

	// DelProperty deletes the given property.
	// Deletion of a property does trigger a change. To simply update
	// a property use the set Property method again. SetProperty overwrite
	// any previous value. The property does not have to be deleted beforehand.
	DelProperty(name string) error

	// SetProperty sets a new Property or overwrites an existing one.
	// This will later trigger a change event, if the rev value is
	// greater than a given filter.
	// Note: The value should always atomically increase. Otherwise
	// changes would not trigger an event. However, less values are
	// ignored automatically. This prevents race conditions if,
	// two changes of the same property happened close to each other.
	// If change with rev (3) arrived earlier than change of rev (2),
	// only one change event will trigger -> the rev (3).
	// but this is fine, because rev (2) is in that moment already
	// outdated.
	SetProperty(name string, rev uint64) error

	// Delete deletes the whole document. Deletion does trigger a change.
	// Delete does an implicit Commit(). No additional operations are possible.
	Delete() error

	// Commit applies all changes done with DelProperty and SetProperty.
	// No close needed. No additional operations are possible.
	Commit() error

	// Close closes the document without committing any changes.
	// No additional operations are possible.
	Close() error
}

func (e ErrInvalidPropertyName) Error() string {
	return fmt.Sprintf("%q is an invalid property name", string(e))
}

func (e ErrListenerDoesNotExist) Error() string {
	return fmt.Sprintf("listener %q does not exist", string(e))
}

// ErrTooLongName is here to indicate that a document,
// property or listener name is too long.
type ErrTooLongName struct {
	Name string

	// Len is used if len(Name) does not reflect the real
	// current length. It could be different because of charset
	// converstions or other detector specifications.
	Len    int
	MaxLen int
}

func (e *ErrTooLongName) Error() string {
	return fmt.Sprintf("the names %q length of %v is larger than maximum of %v", e.Name, e.Len, e.MaxLen)
}
