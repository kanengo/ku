package hashx

import "testing"

func TestMd5String(t *testing.T) {
	t.Log(Md5String("hello"))
	t.Log(Md5String("FASFSF"))
}
