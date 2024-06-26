package reporter

import (
	"context"
	"sampler/internal/ns"
	"sampler/internal/util"
	"sampler/internal/worker"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Reporter struct {
	metaClient    mongo.Client
	metaDBName    string
	startTime     time.Time
	reportFullDoc bool
	queue         chan report
	pool          *worker.Pool
}

type report struct {
	namespace string
	reason    Reason
	details   bson.D
	direction util.Direction
}

// Create new reporter -- uses its own single thread pool and listens for reports to insert
// to the Meta DB until Reporter.Done() has been called
func NewReporter(meta *mongo.Client, dbName string, clean bool, startTime time.Time, reportFullDoc bool) Reporter {
	r := Reporter{
		metaClient:    *meta,
		metaDBName:    dbName,
		reportFullDoc: reportFullDoc,
		startTime:     startTime,
		queue:         make(chan report),
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

// close the reporting queue
func (r *Reporter) Done(ctx context.Context, logger zerolog.Logger) {
	logger.Debug().Msg("closing reporter queue and waiting for reporters to finish")
	close(r.queue)
	r.pool.Done()
}

func (r *Reporter) MissingNamespace(missing string, loc Location) {
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
		{"src", source},
		{"tgt", target},
	}
	rep := report{
		namespace: source.String(),
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchCount(namespace string, src int64, target int64) {
	reason := COUNT_DIFF
	details := bson.D{
		{"src", src},
		{"tgt", target},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) MissingIndex(namespace string, index bson.Raw, location Location) {
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

func (r *Reporter) MismatchIndex(namespace string, src bson.Raw, target bson.Raw) {
	reason := INDEX_DIFF
	details := bson.D{
		{"src", src},
		{"tgt", target},
	}
	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
	}
	r.queue <- rep
}

func (r *Reporter) SampleSummary(namespace string, direction util.Direction, summary DocSummary) {
	reason := COLL_SUMMARY
	details := bson.D{}

	switch direction {
	case util.TgtToSrc:
		details = append(details, bson.D{
			bson.E{"docsMissing.src", summary.Missing},
			bson.E{"docsWithMismatches.tgtToSrc", summary.Different},
		}...)
	case util.SrcToTgt:
		details = append(details, bson.D{
			bson.E{"docsMissing.tgt", summary.Missing},
			bson.E{"docsWithMismatches.srcToTgt", summary.Different},
		}...)
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
	}
	r.queue <- rep
}

func (r *Reporter) MismatchDoc(namespace string, direction util.Direction, a, b bson.Raw) {
	reason := DOC_DIFF
	details := bson.D{
		{"direction", direction},
		{"key", a.Lookup("_id")},
	}

	if r.reportFullDoc {
		details = append(details, primitive.E{"srcDoc", a})
		details = append(details, primitive.E{"tgtDoc", b})
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
	}
	r.queue <- rep
}

func (r *Reporter) MissingDoc(namespace string, direction util.Direction, doc bson.Raw) {
	reason := DOC_MISSING
	details := bson.D{
		{"key", doc.Lookup("_id")},
	}

	switch direction {
	case util.SrcToTgt:
		details = append(details, bson.E{"missingFrom", Target})
	case util.TgtToSrc:
		details = append(details, bson.E{"missingFrom", Source})
	}

	if r.reportFullDoc {
		details = append(details, bson.E{"doc", doc})
	}

	rep := report{
		namespace: namespace,
		reason:    reason,
		details:   details,
		direction: direction,
	}
	r.queue <- rep
}

func (r *Reporter) report(rep report, logger zerolog.Logger) {
	filter := bson.D{
		{"reason", rep.reason},
		{"run", r.startTime},
		{"ns", rep.namespace},
	}

	var update bson.D
	switch rep.reason {

	case COLL_SUMMARY:
		update = bson.D{
			{"$inc", rep.details},
		}
	case DOC_DIFF, DOC_MISSING:
		var doc bson.Raw
		doc, err := bson.Marshal(rep.details)
		if err != nil {
			log.Error().Err(err).Msg("[internal] cannot marshal details doc to bson.Raw")
		}
		filter = append(filter, bson.E{"key", doc.Lookup("key")})
		update = bson.D{
			{"$set", rep.details},
		}
	default:
		// if not updating an existing doc, manually add _id so the upsert filter is unique and an insert occurs
		filter = append(filter, bson.E{"_id", primitive.NewObjectID()})
		update = bson.D{
			{"$set", rep.details},
		}
	}

	logger.Debug().Msgf("appending summary -- {filter: %s, update: %s}", filter, update)
	opts := options.Update().SetUpsert(true)
	_, err := r.getCollection(rep.reason).UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		logger.Error().Err(err).Msgf("unable to append to doc summary  -- {filter: %s, update: %s}", filter, update)
	}
}

func (r *Reporter) processReports(ctx context.Context, logger zerolog.Logger) {
	logger.Info().Msgf("starting report processing, view with filter: { run: new Date(\"%s\") }", r.startTime.UTC().Format(time.RFC3339Nano))
	for rep := range r.queue {
		logger = logger.With().Str("ns", rep.namespace).Logger()
		r.report(rep, logger)
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
