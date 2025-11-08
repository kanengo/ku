package convertx

import (
	"fmt"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/kanengo/ku/basex"
	"github.com/kanengo/ku/unsafex"
)

type Integer interface {
	int32 | int64 | uint64 | uint32 | uint16 | uint8 | uint | int | int16
}

func String2Integer[T Integer](s string) T {
	i, _ := strconv.ParseInt(s, 10, 64)

	return T(i)
}

func Convert[S, D any](s S, fn func(s S) D) D {
	return fn(s)
}

func Integer2String[T Integer](i T) string {
	return strconv.FormatInt(int64(i), 10)
}

func ToInt64(v any) int64 {
	switch v := v.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		return String2Integer[int64](v)
	default:
		return 0
	}
}

func Float2String[T float64 | float32](i T) string {
	return strconv.FormatFloat(float64(i), 'f', 4, 64) // "123.4568"
}

func String2Float[T float64 | float32](s string) T {
	i, _ := strconv.ParseFloat(s, 64)
	return T(i)
}

func ToString(v any) string {
	switch v := v.(type) {
	case int:
		return Integer2String(v)
	case int64:
		return Integer2String(v)
	case int32:
		return Integer2String(v)
	case uint64:
		return Integer2String(v)
	case uint32:
		return Integer2String(v)
	case uint16:
		return Integer2String(v)
	case uint8:
		return Integer2String(v)
	case uint:
		return Integer2String(v)
	case int16:
		return Integer2String(v)
	case float64:
		return Float2String(v)
	case float32:
		return Float2String(v)
	case string:
		return v
	case interface{ ToString() string }:
		return v.ToString()
	case interface{ String() string }:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func JsonString2Struct[T any](content string) basex.Result[T] {
	var data T
	err := sonic.UnmarshalString(content, &data)
	if err != nil {
		return basex.ResultError[T](err)
	}

	return basex.ResultOk(data)
}

func Json2Struct[T any](content []byte) basex.Result[T] {
	var data T
	err := sonic.UnmarshalString(unsafex.Bytes2String(content), &data)
	if err != nil {
		return basex.ResultError[T](err)
	}

	return basex.ResultOk(data)
}

func Struct2Json[T any](data T) basex.Result[[]byte] {
	return basex.ResultTuple(sonic.Marshal(data))
}

func Struct2JsonString[T any](data T) basex.Result[string] {
	return basex.ResultTuple(sonic.MarshalString(data))
}
