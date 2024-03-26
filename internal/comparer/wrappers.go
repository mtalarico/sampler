package comparer

import (
	"bytes"
	"sampler/internal/ns"

	"go.mongodb.org/mongo-driver/bson"
)

/// Wraps a namespace into a format that meets the NamedComparable trait -- used for sorting specifications (NS or Index)

type nsWrapper struct {
	Name string
	ns.Namespace
}

func (c nsWrapper) GetName() string {
	return c.Name
}

// TODO better metadata checking
func (c nsWrapper) Equal(to any) bool {
	a := c.Specification
	b := to.(nsWrapper).Specification
	return a.Name == b.Name &&
		a.ReadOnly == b.ReadOnly &&
		a.Type == b.Type &&
		a.IDIndex.Name == b.IDIndex.Name &&
		bytes.Equal(a.Options, b.Options)
}

func wrapColls(specs []ns.Namespace) []nsWrapper {
	wrapped := []nsWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, nsWrapper{each.String(), each})
	}
	return wrapped
}

// Wraps an index specification into a format that meets the NamedComparable trait -- used for sorting specifications (NS or Index)
type indexWrapper struct {
	Name string
	bson.Raw
}

func (i indexWrapper) GetName() string {
	return i.Name
}

// Note mongo Go driver doesn't support collation on IndexSpecifications yet, so we cannot check it
func (a indexWrapper) Equal(b any) bool {
	return bytes.Equal(a.Raw, b.(indexWrapper).Raw)
}

func wrapIndexes(specs []bson.Raw) []indexWrapper {
	wrapped := []indexWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, indexWrapper{each.Lookup("name").String(), each})
	}
	return wrapped
}
