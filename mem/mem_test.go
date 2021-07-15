package mem

import (
	"testing"

	"github.com/tfaller/propchange/internal/tests"
)

func TestSuit(t *testing.T) {
	tests.TestSuite(NewMem(), t)
}
