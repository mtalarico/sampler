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

func (c *Comparer) CompareIndexes(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) bool {
	logger = logger.With().Str("c", "index").Logger()
	source, target := c.getIndexSpecs(ctx, namespace)
	wrappedSource, wrappedTarget := wrapIndexes(source), wrapIndexes(target)

	// sort first here
	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)
	mismatches := diff.Diff(sortedSource, sortedTarget)
	logger.Debug().Msgf("%s", mismatches.String())
	if len(mismatches.MissingOnSrc) > 0 || len(mismatches.MissingOnTgt) > 0 || len(mismatches.Different) > 0 {
		logger.Error().Msg("indexes are not the same.")
		// c.reporter.ReportMismatchIndexes(namespace, mismatches)
		return false
	}
	logger.Info().Msg("indexes match.")
	return true
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
