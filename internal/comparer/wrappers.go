package comparer

import (
	"bytes"
	"sampler/internal/ns"

	"go.mongodb.org/mongo-driver/mongo"
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

// Wraps an index specification into a format that meets the NamedComparable trait -- used for sorting specifications (NS or Index)

type indexWrapper struct {
	Name string
	mongo.IndexSpecification
}

func (i indexWrapper) GetName() string {
	return i.Name
}

// Note mongo Go driver doesn't support collation on IndexSpecifications yet, so we cannot check it
func (i indexWrapper) Equal(to interface{}) bool {
	a := i.IndexSpecification
	b := to.(indexWrapper).IndexSpecification
	equal := bytes.Equal(a.KeysDocument, b.KeysDocument)
	if a.Clustered != nil && b.Clustered != nil {
		equal = equal && (*a.Clustered == *b.Clustered)
	} else if a.Clustered != nil || b.Clustered != nil {
		return false
	}
	if a.ExpireAfterSeconds != nil && b.ExpireAfterSeconds != nil {
		equal = equal && (*a.ExpireAfterSeconds == *b.ExpireAfterSeconds)
	} else if a.ExpireAfterSeconds != nil || b.ExpireAfterSeconds != nil {
		return false
	}
	if a.Sparse != nil && b.Sparse != nil {
		equal = equal && (*a.Sparse == *b.Sparse)
	} else if a.Sparse != nil || b.Sparse != nil {
		return false
	}
	if a.Unique != nil && b.Unique != nil {
		equal = equal && (*a.Unique == *b.Unique)
	} else if a.Unique != nil || b.Unique != nil {
		return false
	}

	equal = equal &&
		a.Version == b.Version &&
		a.Namespace == b.Namespace &&
		a.Name == b.Name

	return equal
}

func wrapIndexes(specs []mongo.IndexSpecification) []indexWrapper {
	wrapped := []indexWrapper{}
	for _, each := range specs {
		wrapped = append(wrapped, indexWrapper{each.Name, each})
	}
	return wrapped
}
