package comparer

import (
	"context"
	"sampler/internal/cfg"
	"sampler/internal/ns"
	"sampler/internal/reporter"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// hard code batch size of 100
const BATCH_SIZE int = 100

// Conducts comparison between one or more namespaces. Comparison includes 1) estimated document count 2) index comparison 3) random sampling of documents (unordered field comparison)
type Comparer struct {
	config       cfg.Configuration
	sourceClient mongo.Client
	targetClient mongo.Client
	reporter     reporter.Reporter
}

func NewComparer(config cfg.Configuration, source *mongo.Client, target *mongo.Client, meta *mongo.Client, startTime time.Time) Comparer {
	reporter := reporter.NewReporter(meta, config.MetaDBName, config.CleanMeta, startTime)

	return Comparer{
		config:       config,
		sourceClient: *source,
		targetClient: *target,
		reporter:     reporter,
	}
}

// Iterates through all user namespaces and preforms comparison for each. If dry run is set, reports counts and exits
func (c *Comparer) CompareUserNamespaces() {
	c.forEachNamespace(c.CompareNs)
}

// Preforms comparison on a single given namespace
func (c *Comparer) CompareNs(namespace ns.Namespace) {
	if c.config.DryRun {
		log.Debug().Str("ns", namespace.String()).Msg("beginning dry run")
		c.GetEstimates(namespace)
		c.GetSampleSize(namespace)
		log.Debug().Str("ns", namespace.String()).Msg("finished dry run")
		return
	}

	if !c.namespaceExistsOnTarget(namespace) {
		log.Warn().Str("ns", namespace.String()).Msgf("%s does not exist on destination, skipping validation", namespace)
		c.reporter.ReportMissingNamespace(namespace)
		return
	}

	log.Debug().Str("ns", namespace.String()).Msg("beginning verification")
	checkCountsResult := c.CompareEstimatedCounts(namespace)
	indexCompareResult := c.CompareIndexes(namespace)
	sampleContentResult := c.CompareSampleDocs(namespace)

	if checkCountsResult && indexCompareResult && sampleContentResult {
		log.Info().Str("ns", namespace.String()).Msg("passed all validation checks")
	} else if !checkCountsResult && indexCompareResult && sampleContentResult {
		log.Warn().Str("ns", namespace.String()).Msg("failed estimated count comparison, but passed other validation checks. Consider running countDocuments")
	} else {
		log.Error().Str("ns", namespace.String()).Msg("one or more validation checks failed")
	}
	log.Debug().Str("ns", namespace.String()).Msg("finished verification")
}

// return a handle to the source collection for a namespace
func (c *Comparer) sourceCollection(namespace ns.Namespace) *mongo.Collection {
	return c.sourceClient.Database(namespace.Db).Collection(namespace.Collection)
}

// return a handle to the target collection for a namespace
func (c *Comparer) targetCollection(namespace ns.Namespace) *mongo.Collection {
	return c.targetClient.Database(namespace.Db).Collection(namespace.Collection)
}

// return a handle to the target collection for a namespace
func (c *Comparer) namespaceExistsOnTarget(namespace ns.Namespace) bool {
	filter := bson.D{{"name", namespace.Collection}}
	ret, err := c.targetClient.Database(namespace.Db).ListCollectionNames(context.TODO(), filter)
	if err != nil {
		log.Error().Err(err).Msg("")
	}
	if len(ret) > 0 {
		return true
	}
	return false
}

// Iterates through all user namespaces and calls the given function on each
func (c *Comparer) forEachNamespace(f func(ns.Namespace)) {
	namespaces, err := ns.ListAllUserCollections(context.TODO(), &c.sourceClient, false, c.config.MetaDBName)
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}
	if len(namespaces) == 0 {
		log.Warn().Msg("No user namespaces found.")
		return
	}
	for _, namespace := range namespaces {
		f(namespace)
	}
}
