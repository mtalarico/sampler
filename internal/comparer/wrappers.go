package comparer

import (
	"bytes"
	"sampler/internal/ns"

	"go.mongodb.org/mongo-driver/mongo"
)

type nsWrapper struct {
	Name string
	ns.Namespace
}

func (c nsWrapper) GetName() string {
	return c.Name
}

// TODO better metadata checking
func (c nsWrapper) Equal(to interface{}) bool {
	return bytes.Equal(c.Specification.Options, to.(nsWrapper).Specification.Options)
}

func wrapColls(specs []ns.Namespace) []nsWrapper {
	wrapped := []nsWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, nsWrapper{each.String(), each})
	}
	return wrapped
}

type indexWrapper struct {
	Name string
	*mongo.IndexSpecification
}

func (i indexWrapper) GetName() string {
	return i.Name
}

func (i indexWrapper) Equal(to interface{}) bool {
	return bytes.Equal(i.KeysDocument, to.(indexWrapper).KeysDocument)
}

func wrapIndexes(specs []*mongo.IndexSpecification) []indexWrapper {
	wrapped := []indexWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, indexWrapper{each.Name, each})
	}
	return wrapped
}
