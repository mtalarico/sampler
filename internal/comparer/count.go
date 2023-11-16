package comparer

import (
	"context"
	"sampler/internal/ns"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (c *Comparer) CompareEstimatedCounts(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) {
	logger = logger.With().Str("c", "count").Logger()
	source, target := c.GetEstimates(ctx, namespace)
	logger.Info().Msgf("source estimate: %d, target estimate: %d", source, target)

	if c.config.DryRun {
		return
	}

	if source != target {
		c.reporter.MismatchCount(namespace, source, target)
		logger.Warn().Msg("estimated document counts don't match. (NOTE: this could be the result of metadata differences from unclean shutdowns, consider running a more exact countDocuments if all other tests pass)")
	} else {
		logger.Info().Msg("estimated document match")
	}
}

func (c *Comparer) GetEstimates(ctx context.Context, namespace ns.Namespace) (int64, int64) {
	sourceCount, err := c.sourceCollection(namespace).EstimatedDocumentCount(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	targetCount, err := c.targetCollection(namespace).EstimatedDocumentCount(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	return sourceCount, targetCount
}
