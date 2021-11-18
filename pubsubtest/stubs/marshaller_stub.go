package stubs

type MarshallerStub struct {
	MarshalFunc func(data interface{}) (payload []byte, version string, err error)
}

func (s *MarshallerStub) Marshal(data interface{}) (payload []byte, version string, err error) {
	return s.MarshalFunc(data)
}
