package queuex

import "github.com/bytedance/sonic"

type Marshaler interface {
	Marshal() (string, error)
	Unmarshal(string) error
}

type JsonMarshaler[T any] struct {
	Data T
}

func NewJsonMarshaler[T any](data T) *JsonMarshaler[T] {
	return &JsonMarshaler[T]{
		Data: data,
	}
}

func (j *JsonMarshaler[T]) Marshal() (string, error) {
	return sonic.MarshalString(j.Data)
}

func (j *JsonMarshaler[T]) Unmarshal(content string) error {
	return sonic.UnmarshalString(content, &j.Data)
}

type StringMarshaler string

func (s *StringMarshaler) Marshal() (string, error) {
	return string(*s), nil
}

func (s *StringMarshaler) Unmarshal(content string) error {
	*s = StringMarshaler(content)
	return nil
}
