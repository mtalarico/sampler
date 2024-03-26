package comparer

import (
	"context"
	"sampler/internal/cfg"
	"sampler/internal/reporter"
	"sampler/internal/worker"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
)

// hard code batch size of 100
const BATCH_SIZE int = 100
const NUM_WORKERS int = 4

// Conducts comparison between one or more namespaces.
// Comparison includes
//  1. metadata & index comparison
//  2. estimated document count
//  3. random sampling of documents (unordered field comparison)
type Comparer struct {
	config       cfg.Configuration
	sourceClient mongo.Client
	targetClient mongo.Client
	reporter     *reporter.Reporter
}

// init this comparer's reporter before returning internal struct
func NewComparer(config cfg.Configuration, source *mongo.Client, target *mongo.Client, meta *mongo.Client, startTime time.Time) Comparer {
	reporter := reporter.NewReporter(meta, config.MetaDBName, config.CleanMeta, startTime)

	return Comparer{
		config:       config,
		sourceClient: *source,
		targetClient: *target,
		reporter:     &reporter,
	}
}

func (c *Comparer) Compare(ctx context.Context) {
	logger := log.With().Logger()

	// create threads and start them listening to process namespaces put on the channel
	namespacesToProcess := make(chan namespacePair)
	pool := worker.NewWorkerPool(logger, NUM_WORKERS, "namespaceWorkers")
	pool.Start(func(innerCtx context.Context, innerLogger zerolog.Logger) {
		c.processNS(innerCtx, innerLogger, namespacesToProcess)
	})

	// use the main thread to go get namespaces and put them on worker channels
	c.streamNamespaces(ctx, logger, namespacesToProcess)

	// clean up and wait to signal to reporter that no more namespaces will be added reporting
	close(namespacesToProcess)
	pool.Done()
	c.reporter.Done(ctx, logger)
}

// Preforms comparison on a single namespace-pair
func (c *Comparer) CompareNs(ctx context.Context, logger zerolog.Logger, namespace namespacePair) {
	logger.Info().Msg("beginning validation")
	c.CompareEstimatedCounts(ctx, logger, namespace)
	c.CompareIndexes(ctx, logger, namespace)
	c.CompareSampleDocs(ctx, logger, namespace)
	logger.Info().Msg("finished validation")
}

func (c *Comparer) processNS(ctx context.Context, logger zerolog.Logger, jobs chan namespacePair) {
	for namespace := range jobs {
		logger = logger.With().Str("ns", namespace.String()).Logger()
		c.CompareNs(ctx, logger, namespace)
	}
}

// return a handle to the source collection for a namespace
func (c *Comparer) sourceCollection(db string, coll string) *mongo.Collection {
	return c.sourceClient.Database(db).Collection(coll)
}

// return a handle to the target collection for a namespace
func (c *Comparer) targetCollection(db string, coll string) *mongo.Collection {
	return c.targetClient.Database(db).Collection(coll)
}
