package pubsub

import (
	"github.com/google/uuid"
)

// DefaultIDGenerator generates a v4 UUID.
// It will panic if it cannot generate the ID.
var DefaultIDGenerator IDGeneratorFunc = uuid.NewString

// IDGenerator is able to
// generate IDs.
type IDGenerator interface {
	New() string
}

// IDGeneratorFunc function helper to
// conform the IDGenerator interface.
type IDGeneratorFunc func() string

// New generate a new ID.
func (f IDGeneratorFunc) New() string {
	return f()
}

// NewID package level function to
// generate IDs using the default
// generator.
func NewID() string {
	return DefaultIDGenerator.New()
}
