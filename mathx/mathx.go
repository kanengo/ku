package mathx

func NextPowerOfTwo[T ~int | ~int64 | ~uint | ~uint64](x T) T {
	if x == 0 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
