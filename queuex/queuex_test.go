package queuex

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonMarshaler(t *testing.T) {
	type task struct {
		Id   string
		Data string
	}
	m := JsonMarshaler[*task]{
		Data: nil,
	}
	// data, err := m.Marshal()
	// assert.NoError(t, err)
	// assert.Equal(t, `{"id":"1","data":"data"}`, data)

	err := m.Unmarshal(`{"Id":"2","Data":"data2"}`)
	fmt.Println("========", m.Data)
	assert.NoError(t, err)
	assert.Equal(t, &task{
		Id:   "2",
		Data: "data2",
	}, m.Data)
}
