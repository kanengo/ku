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
