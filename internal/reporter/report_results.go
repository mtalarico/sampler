package reporter

import (
	"context"
	"sampler/internal/index"
	"sampler/internal/ns"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type location string

const (
	Source location = "source"
	Target location = "target"
)

type Reporter struct {
	metaClient mongo.Client
	metaDBName string
	startTime  time.Time
}

func NewReporter(meta *mongo.Client, dbName string, clean bool, startTime time.Time) Reporter {
	r := Reporter{
		metaClient: *meta,
		metaDBName: dbName,
		startTime:  startTime,
	}
	if clean {
		r.cleanMetaDB()
	}
	return r
}

func (r *Reporter) ReportMissingNamespace(namespace ns.Namespace) {
	reason := "missingNamespace"
	details := bson.D{
		{"location", Target},
	}
	r.insertReport(namespace, reason, details)
}

func (r *Reporter) ReportMismatchCount(namespace ns.Namespace, src int64, target int64) {
	reason := "countMismatch"
	report := bson.D{
		{"source", src},
		{"target", target},
	}
	r.insertReport(namespace, reason, report)
}

func (r *Reporter) ReportMismatchIndexes(namespace ns.Namespace, details index.IndexMismatchDetails) {
	for _, each := range details.MissingIndexOnSrc {
		r.reportMissingIndex(namespace, each, Source)
	}

	for _, each := range details.MissingIndexOnTgt {
		r.reportMissingIndex(namespace, each, Target)
	}

	for _, each := range details.IndexDifferent {
		r.reportDifferentIndex(namespace, each.Source, each.Target)
	}
}

// func (r *Reporter) ReportMismatchDoc(namespace ns.Namespace, details bsonutils.DocMismatchDetails) {
// 	for _, each := range details.MissingFieldOnSrc {
// 		// r.re(namespace, each, Source)
// 	}

// 	for _, each := range details.MissingFieldOnDst {
// 		// r.reportMissingIndex(namespace, each, Target)
// 	}

// 	for _, each := range details.FieldContentsDiffer {
// 		// r.reportDifferentIndex(namespace, each.Source, each.Target)
// 	}
// }

func (r *Reporter) reportMissingIndex(namespace ns.Namespace, index *mongo.IndexSpecification, location location) {
	reason := "missingIndex"
	details := bson.D{
		{"location", location},
		{"index", index},
	}
	r.insertReport(namespace, reason, details)
}

func (r *Reporter) reportDifferentIndex(namespace ns.Namespace, src *mongo.IndexSpecification, target *mongo.IndexSpecification) {
	reason := "diffIndex"
	details := bson.D{
		{"source", src},
		{"target", target},
	}
	r.insertReport(namespace, reason, details)
}

// func (r *Reporter) reportMissingDoc(namespace ns.Namespace, doc bson.Raw, location location) {
// 	reason := "missingDoc"
// 	details := bson.D{
// 		{"location", location},
// 		{"doc", doc},
// 	}
// 	r.insertReport(namespace, reason, details)
// }

// func (r *Reporter) reportDifferentDoc(namespace ns.Namespace, src bson.Raw, target bson.Raw) {
// 	reason := "diffDoc"
// 	details := bson.D{
// 		{"source", src},
// 		{"target", target},
// 	}
// 	r.insertReport(namespace, reason, details)
// }

func (r *Reporter) insertReport(namespace ns.Namespace, reason string, details []bson.E) {
	template := bson.D{
		{"reason", reason},
		{"run", r.startTime},
		{"ns", namespace},
	}
	report := append(template, details...)
	extJson, _ := bson.MarshalExtJSON(report, false, false)
	log.Debug().Str("c", reason).Str("ns", namespace.String()).Msgf("inserting report -- %s", extJson)
	_, err := r.metaCollection().InsertOne(context.TODO(), report)
	if err != nil {
		log.Error().Err(err).Str("c", reason).Str("ns", namespace.String()).Msgf("unable to insert report -- %s", extJson)
	}
}

func (r *Reporter) metaCollection() *mongo.Collection {
	return r.metaClient.Database(r.metaDBName).Collection("report")
}

func (r *Reporter) cleanMetaDB() {
	r.metaClient.Database(r.metaDBName).Drop(context.TODO())
}
