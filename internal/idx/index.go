package idx

import (
	"bytes"

	"go.mongodb.org/mongo-driver/bson"
)

type Index struct {
	Name string
	bson.Raw
}

func (i Index) GetName() string {
	return i.Name
}

func (a Index) Equal(b any) bool {
	return bytes.Equal(a.Raw, b.(Index).Raw)
}

func FromBson(specs []bson.Raw) []Index {
	wrapped := []Index{}
	for _, each := range specs {
		wrapped = append(wrapped, Index{each.Lookup("name").String(), each})
	}
	return wrapped
}
