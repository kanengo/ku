package convertx

import "strconv"

type Integer interface {
	int32 | int64 | uint64 | uint32 | uint16 | uint8 | uint | int | int16
}

func StringToInteger[T Integer](s string) T {
	i, _ := strconv.ParseInt(s, 10, 64)

	return T(i)
}

func Convert[S, D any](s S, fn func(s S) D) D {
	return fn(s)
}

func IntegerToString[T Integer](i T) string {
	return strconv.FormatInt(int64(i), 10)
}
