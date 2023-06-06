package comparer

import (
	"context"
	"sampler/internal/ns"

	"github.com/rs/zerolog"
)

func (c *Comparer) CompareEstimatedCounts(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) {
	logger = logger.With().Str("c", "count").Logger()
	source, target := c.GetEstimates(ctx, logger, namespace)
	if source != target {
		c.reporter.ReportMismatchCount(namespace, source, target)
		logger.Warn().Msg("estimated document counts don't match. (NOTE: this could be the result of metadata differences from unclean shutdowns, consider running a more exact countDocuments if all other tests pass)")
		return
	}
	logger.Info().Msg("estimated document match")
}

func (c *Comparer) GetEstimates(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) (int64, int64) {
	sourceCount, err := c.sourceCollection(namespace).EstimatedDocumentCount(context.TODO())
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}

	targetCount, err := c.targetCollection(namespace).EstimatedDocumentCount(context.TODO())
	if err != nil {
		logger.Fatal().Err(err).Msg("")
	}

	logger.Info().Msgf("source estimate: %d, target estimate: %d", sourceCount, targetCount)
	return sourceCount, targetCount
}
