package comparer

import (
	"context"
	"sampler/internal/cfg"
	"sampler/internal/ns"
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

func NewComparer(config cfg.Configuration, source *mongo.Client, target *mongo.Client, meta *mongo.Client, startTime time.Time) Comparer {
	reporter := reporter.NewReporter(meta, config.MetaDBName, config.CleanMeta, startTime)

	return Comparer{
		config:       config,
		sourceClient: *source,
		targetClient: *target,
		reporter:     &reporter,
	}
}

func (c *Comparer) CompareAll(ctx context.Context) {
	namespacesToProcess := make(chan ns.Namespace)
	logger := log.With().Logger()

	pool := worker.NewWorkerPool(logger, NUM_WORKERS, "namespaceWorkers")
	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		c.processNS(iCtx, iLogger, namespacesToProcess)
	})

	c.streamNamespaces(ctx, namespacesToProcess)

	close(namespacesToProcess)
	pool.Done()
	c.reporter.Done(ctx, logger)
}

// Preforms comparison on a single namespace
func (c *Comparer) CompareNs(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) {
	if c.config.DryRun {
		logger.Debug().Msg("beginning dry run")
		c.GetEstimates(ctx, logger, namespace)
		// c.GetSampleSize(ctx, logger, namespace)
		logger.Debug().Msg("finished dry run")
		return
	}

	logger.Debug().Msg("beginning validation")
	checkCountsResult := c.CompareEstimatedCounts(ctx, logger, namespace)
	indexCompareResult := c.CompareIndexes(ctx, logger, namespace)
	// sampleForwardResult := c.CompareSampleDocs(ctx, logger, namespace, false)
	// sampleReverseResult := c.CompareSampleDocs(ctx, logger, namespace, true)

	if checkCountsResult && indexCompareResult { // && sampleForwardResult && sampleReverseResult {
		logger.Info().Msg("passed all validation checks")
	} else if !checkCountsResult && indexCompareResult { //&& sampleForwardResult && sampleReverseResult {
		logger.Warn().Msg("failed estimated count comparison, but passed other validation checks. Consider running countDocuments")
	} else {
		logger.Error().Msg("one or more validation checks failed")
	}
	logger.Debug().Msg("finished validation")
}

func (c *Comparer) processNS(ctx context.Context, logger zerolog.Logger, jobs chan ns.Namespace) {
	for namespace := range jobs {
		logger = logger.With().Str("ns", namespace.String()).Logger()
		c.CompareNs(ctx, logger, namespace)
	}
}

// return a handle to the source collection for a namespace
func (c *Comparer) sourceCollection(namespace ns.Namespace) *mongo.Collection {
	return c.sourceClient.Database(namespace.Db).Collection(namespace.Collection)
}

// return a handle to the target collection for a namespace
func (c *Comparer) targetCollection(namespace ns.Namespace) *mongo.Collection {
	return c.targetClient.Database(namespace.Db).Collection(namespace.Collection)
}
