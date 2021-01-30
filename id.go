package pubsub

import (
	"crypto/rand"
	"fmt"
	"io"
)

type ID []byte

// DefaultIDGenerator generates a v4 UUID.
// It will panic if it cannot generate the ID.
var DefaultIDGenerator IDGeneratorFunc = func() []byte {
	id := make([]byte, 16)
	_, err := io.ReadFull(DefaultRandReader, id)
	if err != nil {
		panic(fmt.Errorf("cannot read from random generator: %v", err))
	}
	id[6] = (id[6] & 0x0f) | 0x40 // Version 4
	id[8] = (id[8] & 0x3f) | 0x80 // Variant is 10
	return id
}

var DefaultRandReader = rand.Reader // random function

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
