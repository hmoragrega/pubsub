package marshaller

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	ErrUnregisteredType  = errors.New("unregistered type")
	ErrInstantiatingType = errors.New("cannot instantiate type")
	ErrAlreadyRegistered = errors.New("type already registered")
	ErrInvalidDataType   = errors.New("invalid data type")
)

type TypeRegistry struct {
	types map[string]reflect.Type
	mx    sync.RWMutex
}

func (r *TypeRegistry) Register(key string, v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// named return
			err = fmt.Errorf("%w (%T): %v", ErrInvalidDataType, v, r)
		}
	}()

	r.mx.Lock()
	defer r.mx.Unlock()

	if r.types == nil {
		r.types = make(map[string]reflect.Type)
	}

	if _, found := r.types[key]; found {
		err = fmt.Errorf("%w: %q", ErrAlreadyRegistered, key)
		return // named return
	}

	r.types[key] = reflect.TypeOf(v).Elem()
	return
}

func (r *TypeRegistry) GetType(topic, name string) (reflect.Type, error) {
	r.mx.RLock()
	defer r.mx.RUnlock()

	if t, ok := r.types[name]; ok {
		return t, nil
	}

	if t, ok := r.types[topic]; ok {
		return t, nil
	}

	return nil, fmt.Errorf("%w: topic: %s name: %s", ErrUnregisteredType, topic, name)
}

func (r *TypeRegistry) GetNew(topic, name string) (interface{}, error) {
	t, err := r.GetType(topic, name)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s: %v", ErrInstantiatingType, t, r)
		}
	}()

	v := reflect.New(t).Interface()

	return v, err
}
