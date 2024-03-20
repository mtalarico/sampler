package comparer

import (
	"context"

	"sampler/internal/diff"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (c *Comparer) CompareIndexes(ctx context.Context, logger zerolog.Logger, namespace namespacePair) {
	logger = logger.With().Str("c", "index").Logger()

	source, target := c.getIndexSpecs(ctx, namespace)
	wrappedSource, wrappedTarget := wrapIndexes(source), wrapIndexes(target)

	// sorted from getIndexSpecs
	comparison := diff.Diff(logger, wrappedSource, wrappedTarget)

	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Error().Msg("indexes are not the same.")
	} else {
		logger.Info().Msg("indexes match.")
	}
	for _, each := range comparison.MissingOnSrc {
		logger.Error().Msgf("%s is missing on the source", each.Name)
		c.reporter.MissingIndex(namespace.String(), each.IndexSpecification, "source")
	}
	for _, each := range comparison.MissingOnTgt {
		logger.Error().Msgf("%s is missing on the target", each.Name)
		c.reporter.MissingIndex(namespace.String(), each.IndexSpecification, "target")
	}
	for _, each := range comparison.Different {
		logger.Error().Msgf("%s is different between the source and target", each.Source.Name)
		c.reporter.MismatchIndex(namespace.String(), each.Source.IndexSpecification, each.Target.IndexSpecification)
	}
}

func (c *Comparer) getIndexSpecs(ctx context.Context, namespace namespacePair) ([]mongo.IndexSpecification, []mongo.IndexSpecification) {
	var sourceSpecs, targetSpecs []mongo.IndexSpecification
	sortedIndexesPipeline := bson.A{bson.D{{"$indexStats", bson.D{}}}, bson.D{{"$sort", bson.D{{"spec", 1}}}}, bson.D{{"$replaceRoot", bson.D{{"newRoot", "$spec"}}}}}
	sourceCursor, err := c.sourceCollection(namespace.Db, namespace.Collection).Aggregate(ctx, sortedIndexesPipeline)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	err = sourceCursor.All(ctx, &sourceSpecs)
	if err != nil {
		log.Fatal().Err(err).Msg("source index specification decoding error")
	}
	targetCursor, err := c.targetCollection(namespace.Db, namespace.Collection).Aggregate(ctx, sortedIndexesPipeline)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	err = targetCursor.All(ctx, &targetSpecs)
	if err != nil {
		log.Fatal().Err(err).Msg("target index specification decoding error")
	}

	return sourceSpecs, targetSpecs
}
