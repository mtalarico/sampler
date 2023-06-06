package reporter

import (
	"context"
	"sampler/internal/ns"
	"sampler/internal/worker"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type location string

const (
	Source        location = "source"
	Target        location = "target"
	NUM_REPORTERS          = 1
)

type Reporter struct {
	metaClient mongo.Client
	metaDBName string
	startTime  time.Time
	queue      chan report
	pool       *worker.Pool
}

type report struct {
	namespace ns.Namespace
	reason    string
	details   []bson.E
}

func NewReporter(meta *mongo.Client, dbName string, clean bool, startTime time.Time) Reporter {
	r := Reporter{
		metaClient: *meta,
		metaDBName: dbName,
		startTime:  startTime,
		queue:      make(chan report),
	}
	if clean {
		r.cleanMetaDB()
	}

	logger := log.With().Str("c", "reporter").Logger()
	pool := worker.NewWorkerPool(logger, 1, "reporterWorkers")

	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		r.processReports(iCtx, iLogger)
	})
	r.pool = &pool
	return r
}

func (r *Reporter) Done(ctx context.Context, logger zerolog.Logger) {
	logger.Debug().Msg("closing reporter queue and waiting for reporters to finish")
	close(r.queue)
	r.pool.Done()
}

func (r *Reporter) ReportMissingNamespace(namespace ns.Namespace) {
	reason := "missingNamespace"
	details := bson.D{
		{"location", Target},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) ReportMismatchCount(namespace ns.Namespace, src int64, target int64) {
	reason := "countMismatch"
	details := bson.D{
		{"source", src},
		{"target", target},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

// func (r *Reporter) ReportMismatchIndexes(namespace ns.Namespace, ) {
// 	for _, each := range details.MissingIndexOnSrc {
// 		r.reportMissingIndex(namespace, each, Source)
// 	}

// 	for _, each := range details.MissingIndexOnTgt {
// 		r.reportMissingIndex(namespace, each, Target)
// 	}

// 	for _, each := range details.IndexDifferent {
// 		r.reportDifferentIndex(namespace, each.Source, each.Target)
// 	}
// }

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
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) reportDifferentIndex(namespace ns.Namespace, src *mongo.IndexSpecification, target *mongo.IndexSpecification) {
	reason := "diffIndex"
	details := bson.D{
		{"source", src},
		{"target", target},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
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

func (r *Reporter) insertReport(rep report, logger zerolog.Logger) {
	template := bson.D{
		{"reason", rep.reason},
		{"run", r.startTime},
		{"ns", rep.namespace.String()},
	}
	report := append(template, rep.details...)
	extJson, _ := bson.MarshalExtJSON(report, false, false)
	logger.Debug().Msgf("inserting report -- %s", extJson)
	_, err := r.metaCollection().InsertOne(context.TODO(), report)
	if err != nil {
		logger.Error().Err(err).Msgf("unable to insert report -- %s", extJson)
	}
}

func (r *Reporter) processReports(ctx context.Context, logger zerolog.Logger) {
	for rep := range r.queue {
		logger = logger.With().Str("ns", rep.namespace.String()).Logger()
		r.insertReport(rep, logger)
	}
}

func (r *Reporter) metaCollection() *mongo.Collection {
	return r.metaClient.Database(r.metaDBName).Collection("report")
}

func (r *Reporter) cleanMetaDB() {
	r.metaClient.Database(r.metaDBName).Drop(context.TODO())
}
