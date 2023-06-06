package comparer

import (
	"bytes"
	"sampler/internal/ns"

	"go.mongodb.org/mongo-driver/mongo"
)

type collWrapper struct {
	Name string
	ns.Namespace
}

func (c collWrapper) GetName() string {
	return c.Name
}

func (c collWrapper) Equal(to interface{}) bool {
	return bytes.Equal(c.Specification.Options, to.(collWrapper).Specification.Options)
}

func wrapColls(specs []ns.Namespace) []collWrapper {
	wrapped := []collWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, collWrapper{each.String(), each})
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
