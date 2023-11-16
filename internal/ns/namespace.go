package ns

import (
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
	Partitioned   bool
	PartitionKey  bson.Raw
	Specification *mongo.CollectionSpecification
}

func (ns Namespace) String() string {
	return ns.Db + "." + ns.Collection
}

var (
	// ExcludedSystemDBs are system databases that are excluded from verification.
	ExcludedSystemDBs = []string{"admin", "config", "local"}

	// ExcludedSystemCollRegex is the regular expression representation of the excluded system collections.
	ExcludedSystemCollRegex = primitive.Regex{Pattern: `^system[.]`, Options: ""}
)

func GetOneUserCollections(client *mongo.Client, dbName string, collName string) (Namespace, error) {
	db := client.Database(dbName)
	filter := bson.D{{"name", collName}}
	specifications, err := db.ListCollectionSpecifications(context.TODO(), filter, nil)
	if err != nil {
		return Namespace{}, err
	}
	if len(specifications) == 1 {
		return checkShardAndMakeNS(client, dbName, specifications[0]), nil
	}
	return Namespace{}, errors.New("no collection found")
}

// Lists all the user collections on a cluster.  Unlike mongosync, we don't use the internal $listCatalog, since we need to
// work on old versions without that command.  This means this does not run with read concern majority.
func AllUserCollections(client *mongo.Client, includeViews bool, additionalExcludedDBs ...string) ([]Namespace, error) {
	excludedDBs := []string{}
	excludedDBs = append(excludedDBs, additionalExcludedDBs...)
	excludedDBs = append(excludedDBs, ExcludedSystemDBs...)

	dbNames, err := client.ListDatabaseNames(context.TODO(), bson.D{{"name", bson.D{{"$nin", excludedDBs}}}}, options.ListDatabases().SetNameOnly(true))
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("all user databases: %+v", dbNames)

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
			log.Trace().Msgf("found coll spec %v", spec)
			ns := checkShardAndMakeNS(client, dbName, spec)
			namespaces = append(namespaces, ns)
		}
	}
	return namespaces, nil
}

func checkShardAndMakeNS(client *mongo.Client, dbName string, spec *mongo.CollectionSpecification) Namespace {
	ns := Namespace{Db: dbName, Collection: spec.Name, Specification: spec}
	if util.IsMongos(client) {
		filter := bson.D{{"_id", dbName + "." + spec.Name}}
		res := client.Database("config").Collection("collections").FindOne(context.TODO(), filter, nil)
		if raw, err := res.Raw(); err == nil {
			ns.Partitioned = true
			ns.PartitionKey = raw.Lookup("key").Document()
		}
	}
	return ns
}
