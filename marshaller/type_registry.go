package marshaller

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	errUnregisteredType  = errors.New("unregistered type")
	errAlreadyRegistered = errors.New("type already registered")
)

type typeRegistry struct {
	types map[string]reflect.Type
	mx    sync.RWMutex
}

func (r *typeRegistry) register(key string, v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// named return
			err = fmt.Errorf("%w (%T): %v", errInvalidDataType, v, r)
		}
	}()

	r.mx.Lock()
	defer r.mx.Unlock()

	if r.types == nil {
		r.types = make(map[string]reflect.Type)
	}

	if _, found := r.types[key]; found {
		err = fmt.Errorf("%w: %q", errAlreadyRegistered, key)
		return // named return
	}

	r.types[key] = reflect.TypeOf(v).Elem()
	return
}

func (r *typeRegistry) getType(topic, name string) (reflect.Type, error) {
	r.mx.RLock()
	defer r.mx.RUnlock()

	if t, ok := r.types[name]; ok {
		return t, nil
	}

	if t, ok := r.types[topic]; ok {
		return t, nil
	}

	return nil, fmt.Errorf("%w: topic: %s name: %s", errUnregisteredType, topic, name)
}
