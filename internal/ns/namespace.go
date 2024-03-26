package ns

import (
	"bytes"
	"context"
	"errors"

	"sampler/internal/util"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Namespace struct {
	Db            string
	Collection    string
	Specification *mongo.CollectionSpecification
}

func (ns Namespace) String() string {
	return ns.Db + "." + ns.Collection
}

func (ns Namespace) GetName() string {
	return ns.String()
}

func (src Namespace) Equal(tgt any) bool {
	a := src.Specification
	b := tgt.(Namespace).Specification
	return a.Name == b.Name &&
		a.ReadOnly == b.ReadOnly &&
		a.Type == b.Type &&
		a.IDIndex.Name == b.IDIndex.Name &&
		bytes.Equal(a.Options, b.Options)
}

var (
	// ExcludedSystemDBs are system databases that are excluded from verification.
	// added mongosync databases even though they are not fully "system" databases
	ExcludedSystemDBs = []string{"admin", "config", "local", "mongosync_monitor", "mongosync_reserved_for_internal_use"}

	// ExcludedSystemCollRegex is the regular expression representation of the excluded system collections.
	ExcludedSystemCollRegex = primitive.Regex{Pattern: `^system[.]`, Options: ""}
)

// Get a single user namespace
func GetOneUserCollections(client *mongo.Client, dbName string, collName string) (Namespace, error) {
	db := client.Database(dbName)
	filter := bson.D{{"name", collName}}
	specifications, err := db.ListCollectionSpecifications(context.TODO(), filter, nil)
	if err != nil {
		return Namespace{}, err
	}
	if len(specifications) < 1 {
		return Namespace{}, errors.New("collection '" + dbName + "." + collName + "' not found")
	}
	spec := specifications[0]
	return Namespace{Db: dbName, Collection: spec.Name, Specification: spec}, nil
}

// Lists all the user collections on a cluster.  Unlike mongosync, we don't use the internal $listCatalog, since we need to
// work on old versions without that command. This means this does not run with read concern majority.
func AllUserCollections(client *mongo.Client, includeViews bool, additionalExcludedDBs ...string) ([]Namespace, error) {
	excludedDBs := []string{}
	excludedDBs = append(excludedDBs, additionalExcludedDBs...)
	excludedDBs = append(excludedDBs, ExcludedSystemDBs...)

	dbNames, err := client.ListDatabaseNames(context.TODO(), bson.D{{"name", bson.D{{"$nin", excludedDBs}}}}, options.ListDatabases().SetNameOnly(true))
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("all user databases: %+v", dbNames)

	// iterate each db getting all collection's specification and adding it as a fully qualified namespace
	namespaces := []Namespace{}
	for _, dbName := range dbNames {
		db := client.Database(dbName)
		filter := bson.D{{"name", bson.D{{"$nin", bson.A{ExcludedSystemCollRegex}}}}}
		if !includeViews {
			filter = append(filter, bson.E{"type", bson.D{{"$ne", "view"}}})
		}
		specifications, err := db.ListCollectionSpecifications(context.TODO(), filter, nil)
		if err != nil {
			return nil, err
		}
		for _, spec := range specifications {
			log.Trace().Msgf("found coll spec %+v", spec)
			ns := Namespace{Db: dbName, Collection: spec.Name, Specification: spec}
			namespaces = append(namespaces, ns)
		}
	}
	return namespaces, nil
}

// checks to see if a collection is sharded. If it is, returns (true, <shard key>). If it is not, returns (false, nil)
func IsSharded(client *mongo.Client, dbName string, collName string) (bool, bson.Raw) {
	if util.IsMongos(client) {
		filter := bson.D{{"_id", dbName + "." + collName}}
		res := client.Database("config").Collection("collections").FindOne(context.TODO(), filter, nil)
		if raw, err := res.Raw(); err == nil {
			return true, raw.Lookup("key").Document()
		}
	}
	return false, nil
}
