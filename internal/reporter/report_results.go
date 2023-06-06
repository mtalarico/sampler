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
	"go.mongodb.org/mongo-driver/mongo/options"
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
	direction string
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

func (r *Reporter) ReportMissingNamespace(missing ns.Namespace, loc location) {
	reason := "missingNamespace"
	details := bson.D{
		{"location", loc},
	}
	rep := report{
		namespace: missing,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) ReportMismatchNamespace(source ns.Namespace, target ns.Namespace) {
	reason := "mismatchNamespace"
	details := bson.D{
		{"location", Target},
	}
	rep := report{
		namespace: source,
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

func (r *Reporter) ReportMissingIndex(namespace ns.Namespace, index *mongo.IndexSpecification, location location) {
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

func (r *Reporter) ReportMismatchIndex(namespace ns.Namespace, src *mongo.IndexSpecification, target *mongo.IndexSpecification) {
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

func (r *Reporter) ReportSampleSummary(namespace ns.Namespace, dir string, summary DocSummary) {
	reason := "docSummary"
	details := bson.D{
		{"missingSrc", summary.MissingOnSrc},
		{"missingTgt", summary.MissingOnTgt},
	}

	switch dir {
	case "src -> tgt":
		details = append(details, bson.E{"mismatches.srcToTgt", summary.Different})
	case "tgt -> src":
		details = append(details, bson.E{"mismatches.tgtToSrc", summary.Different})
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: dir,
	}
	r.queue <- rep
}

func (r *Reporter) appendDocSummary(rep report, logger zerolog.Logger) {
	filter := bson.D{
		{"reason", rep.reason},
		{"run", r.startTime},
		{"ns", rep.namespace.String()},
	}

	update := bson.D{
		{"$inc", rep.details},
	}

	extJson, _ := bson.MarshalExtJSON(filter, false, false)
	logger.Debug().Msgf("inserting report -- %s", extJson)
	opts := options.Update().SetUpsert(true)
	_, err := r.metaCollection().UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		logger.Error().Err(err).Msgf("unable to append to doc summary -- %s", extJson)
	}
}

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
		if rep.reason == "docSummary" {
			r.appendDocSummary(rep, logger)
		} else {
			r.insertReport(rep, logger)
		}
	}
}

func (r *Reporter) metaCollection() *mongo.Collection {
	return r.metaClient.Database(r.metaDBName).Collection("report")
}

func (r *Reporter) cleanMetaDB() {
	err := r.metaClient.Database(r.metaDBName).Drop(context.TODO())
	if err != nil {
		log.Error().Err(err).Msg("unable to drop meta collection")
	}
}
