package basex

import "errors"

var ErrResult = errors.New("result error")

type resultErr struct {
	err error
}

func (resultErr) Unwrap() error {
	return ErrResult
}

type Result[T any] struct {
	v   T
	err error
}

func (r Result[T]) Get() (T, error) {
	return r.v, r.err
}

func (r Result[T]) Must() T {
	if r.err != nil {
		panic(resultErr{err: r.err})
	}

	return r.v
}

func ResultError[T any](err error) Result[T] {
	return Result[T]{
		err: err,
	}
}

func ResultOk[T any](t T) Result[T] {
	return Result[T]{
		v: t,
	}
}

func ResultTuple[T any](t T, err error) Result[T] {
	return Result[T]{
		v:   t,
		err: err,
	}
}
