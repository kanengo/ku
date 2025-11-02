package queuex

import "github.com/bytedance/sonic"

type Marshaler interface {
	Marshal() (string, error)
	Unmarshal(string) error
}

type JsonMarshaler[T any] struct {
	Data T
}

func NewJsonMarshaler[T any](data T) JsonMarshaler[T] {
	return JsonMarshaler[T]{
		Data: data,
	}
}

func (j *JsonMarshaler[T]) Marshal() (string, error) {
	return sonic.MarshalString(j.Data)
}

func (j *JsonMarshaler[T]) Unmarshal(data string) error {
	return sonic.UnmarshalString(data, &j.Data)
}
