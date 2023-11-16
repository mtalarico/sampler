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

type Reporter struct {
	metaClient mongo.Client
	metaDBName string
	startTime  time.Time
	queue      chan report
	pool       *worker.Pool
}

type report struct {
	namespace ns.Namespace
	reason    Reason
	details   []bson.E
	direction Direction
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

func (r *Reporter) MissingNamespace(missing ns.Namespace, loc Location) {
	reason := NS_MISSING
	details := bson.D{
		{"missingFrom", loc},
	}
	rep := report{
		namespace: missing,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchNamespace(source ns.Namespace, target ns.Namespace) {
	reason := NS_DIFF
	details := bson.D{
		{"source", source.String()},
		{"target", target.String()},
	}
	rep := report{
		namespace: source,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchCount(namespace ns.Namespace, src int64, target int64) {
	reason := COUNT_DIFF
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

func (r *Reporter) MissingIndex(namespace ns.Namespace, index *mongo.IndexSpecification, location Location) {
	reason := INDEX_MISSING
	details := bson.D{
		{"missingFrom", location},
		{"index", index},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchIndex(namespace ns.Namespace, src *mongo.IndexSpecification, target *mongo.IndexSpecification) {
	reason := INDEX_DIFF
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

func (r *Reporter) SampleSummary(namespace ns.Namespace, direction Direction, summary DocSummary) {
	reason := COLL_SUMMARY
	details := bson.D{
		{"docsMissing.src", summary.MissingOnSrc},
		{"docsMissing.dst", summary.MissingOnTgt},
	}

	switch direction {
	case SrcToDst:
		details = append(details, bson.E{"docsWithMismatches.srcToDst", summary.Different})
	case DstToSrc:
		details = append(details, bson.E{"docsWithMismatches.dstToSrc", summary.Different})
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchDoc(namespace ns.Namespace, direction Direction, a, b bson.Raw) {
	reason := DOC_DIFF
	details := bson.D{
		{"src", a},
		{"dst", b},
		{"direction", direction},
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
	}
	r.queue <- rep
}

func (r *Reporter) MissingDoc(namespace ns.Namespace, direction Direction, doc bson.Raw) {
	reason := DOC_MISSING
	details := bson.D{
		{"doc", doc},
		{"direction", direction},
	}

	switch direction {
	case SrcToDst:
		details = append(details, bson.E{"missingFrom", Target})
	case DstToSrc:
		details = append(details, bson.E{"missingFrom", Source})
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
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
	_, err := r.getCollection(rep.reason).UpdateOne(context.TODO(), filter, update, opts)
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
	_, err := r.getCollection(rep.reason).InsertOne(context.TODO(), report)
	if err != nil {
		logger.Error().Err(err).Msgf("unable to insert report -- %s", extJson)
	}
}

func (r *Reporter) processReports(ctx context.Context, logger zerolog.Logger) {
	for rep := range r.queue {
		logger = logger.With().Str("ns", rep.namespace.String()).Logger()
		if rep.reason == COLL_SUMMARY {
			r.appendDocSummary(rep, logger)
		} else {
			r.insertReport(rep, logger)
		}
	}
}

func (r *Reporter) getCollection(reason Reason) *mongo.Collection {
	switch reason {
	case DOC_DIFF, DOC_MISSING:
		return r.metaClient.Database(r.metaDBName).Collection("docs")
	default:
		return r.metaClient.Database(r.metaDBName).Collection("report")
	}
}

func (r *Reporter) cleanMetaDB() {
	err := r.metaClient.Database(r.metaDBName).Drop(context.TODO())
	if err != nil {
		log.Error().Err(err).Msg("unable to drop meta collection")
	}
}
