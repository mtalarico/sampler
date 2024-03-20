package comparer

import (
	"sampler/internal/util"
	"testing"

	"github.com/go-playground/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestBasicIndexSortAlreadyInOrder(t *testing.T) {
	a, err := bson.Marshal(bson.D{{"a", 1}})
	if err != nil {
		panic(err)
	}
	wrappedA := indexWrapper{
		"test1",
		mongo.IndexSpecification{
			Name:               "test1",
			Namespace:          "test.test",
			KeysDocument:       a,
			Version:            1,
			ExpireAfterSeconds: nil,
			Sparse:             nil,
			Unique:             nil,
			Clustered:          nil,
		},
	}
	b, err := bson.Marshal(bson.D{{"b", 1}})
	if err != nil {
		panic(err)
	}
	wrappedB := indexWrapper{
		"test1",
		mongo.IndexSpecification{
			Name:               "test2",
			Namespace:          "test.test",
			KeysDocument:       b,
			Version:            1,
			ExpireAfterSeconds: nil,
			Sparse:             nil,
			Unique:             nil,
			Clustered:          nil,
		},
	}
	expected := []indexWrapper{wrappedA, wrappedB}
	assert.Equal(t, expected, util.SortSpec([]indexWrapper{wrappedA, wrappedB}))
}

func TestBasicIndexSort(t *testing.T) {
	a, err := bson.Marshal(bson.D{{"a", 1}})
	if err != nil {
		panic(err)
	}
	wrappedA := indexWrapper{
		"a_1",
		mongo.IndexSpecification{
			Name:               "a_1",
			Namespace:          "test.test",
			KeysDocument:       a,
			Version:            1,
			ExpireAfterSeconds: nil,
			Sparse:             nil,
			Unique:             nil,
			Clustered:          nil,
		},
	}
	b, err := bson.Marshal(bson.D{{"b", 1}})
	if err != nil {
		panic(err)
	}
	wrappedB := indexWrapper{
		"b_1",
		mongo.IndexSpecification{
			Name:               "b_1",
			Namespace:          "test.test",
			KeysDocument:       b,
			Version:            1,
			ExpireAfterSeconds: nil,
			Sparse:             nil,
			Unique:             nil,
			Clustered:          nil,
		},
	}
	expected := []indexWrapper{wrappedA, wrappedB}
	actual := util.SortSpec([]indexWrapper{wrappedB, wrappedA})
	assert.Equal(t, expected, actual)
}
