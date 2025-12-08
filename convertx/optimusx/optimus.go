package optimusx

import (
	"github.com/pjebs/optimus-go"
)

type Optimus struct {
	optimus.Optimus
}

func NewOptimus(prime, random uint64) Optimus {
	o := optimus.NewCalculated(prime, random)
	return Optimus{Optimus: o}
}
