package slicex

func Count[T any](s []T, f func(t T) bool) int {
	cnt := 0
	for _, v := range s {
		if f(v) {
			cnt += 1
		}
	}
	return cnt
}

func Map[S any, D any](ss []S, f func(S, int) D) []D {
	ret := make([]D, 0, len(ss))
	for k, v := range ss {
		ret = append(ret, f(v, k))
	}
	return ret
}
