package influx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewQueryRequest(t *testing.T) {
	q, err := NewQueryRequest("select * bar", "foo", "bar", "true")

	assert.NotNil(t, err)
	assert.Nil(t, q.Query)

	q, err = NewQueryRequest("select * from foo", "foo", "bar", "true")

	assert.NotNil(t, q)
	assert.Nil(t, err)
}

func TestNewWriteRequest(t *testing.T) {
	var lineData = []byte("weather,location=us-midwest,season=summer temperature=82 1465839830100400200")
	w, err := NewWriteRequest(lineData, "foo", "bar", "foo")

	assert.Nil(t, err)
	assert.NotNil(t, w)

	lineData = []byte("weather,location=us-midwest,season=summer,temperature=82 1465839830100400200")
	w, err = NewWriteRequest(lineData, "foo", "bar", "foo")

	assert.Nil(t, w.Points)
	assert.NotNil(t, err)
}