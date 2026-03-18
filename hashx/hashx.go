package hashx

import (
	"crypto/md5"
	"fmt"

	"github.com/kanengo/ku/unsafex"
)

func Md5String(str string) string {
	return fmt.Sprintf("%x", md5.Sum(unsafex.String2Bytes(str)))
}
