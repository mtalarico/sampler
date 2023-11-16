package comparer

import (
	"context"

	"sampler/internal/diff"
	"sampler/internal/ns"
	"sampler/internal/util"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
)

func (c *Comparer) CompareIndexes(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) {
	if c.config.DryRun {
		return
	}
	logger = logger.With().Str("c", "index").Logger()
	source, target := c.getIndexSpecs(ctx, namespace)
	wrappedSource, wrappedTarget := wrapIndexes(source), wrapIndexes(target)

	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)
	comparison := diff.Diff(logger, sortedSource, sortedTarget)

	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Error().Msg("indexes are not the same.")
	} else {
		logger.Info().Msg("indexes match.")
	}
	for _, each := range comparison.MissingOnSrc {
		logger.Error().Msgf("%s is missing on the target", each.Name)
		c.reporter.MissingIndex(namespace, each.IndexSpecification, "source")
	}
	for _, each := range comparison.MissingOnTgt {
		logger.Error().Msgf("%s is missing on the source", each.Name)
		c.reporter.MissingIndex(namespace, each.IndexSpecification, "target")
	}
	for _, each := range comparison.Different {
		logger.Error().Msgf("%s is different between the source and target", each.Source.Name)
		c.reporter.MismatchIndex(namespace, each.Source.IndexSpecification, each.Target.IndexSpecification)
	}
}

func (c *Comparer) getIndexSpecs(ctx context.Context, namespace ns.Namespace) ([]*mongo.IndexSpecification, []*mongo.IndexSpecification) {
	sourceSpecs, err := c.sourceCollection(namespace).Indexes().ListSpecifications(ctx, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	targetSpecs, err := c.targetCollection(namespace).Indexes().ListSpecifications(ctx, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	return sourceSpecs, targetSpecs
}
