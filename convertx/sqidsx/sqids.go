package sqidsx

import (
	"github.com/sqids/sqids-go"
)

type Sqids struct {
	*sqids.Sqids
}

func NewSqids(alphabet string, minLength int) Sqids {
	sqids, err := sqids.New(sqids.Options{
		Alphabet:  alphabet,
		MinLength: uint8(minLength),
	})
	if err != nil {
		panic(err)
	}

	return Sqids{Sqids: sqids}
}
