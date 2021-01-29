package pubsub

import (
	"github.com/google/uuid"
)

// DefaultIDGenerator is the default
// id generator.
//
// Users of the package can override
// it.
var DefaultIDGenerator IDGeneratorFunc = func() []byte {
	u := uuid.New()
	return u[:]
}

// IDGenerator is able to
// generate IDs.
type IDGenerator interface {
	New() []byte
}

// IDGeneratorFunc function helper to
// conform the IDGenerator interface.
type IDGeneratorFunc func() []byte

// New generate a new ID.
func (f IDGeneratorFunc) New() []byte {
	return f()
}

// NewID package level function to
// generate IDs using the default
// generator.
func NewID() []byte {
	return DefaultIDGenerator.New()
}
