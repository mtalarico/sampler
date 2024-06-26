package comparer

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (c *Comparer) CompareEstimatedCounts(ctx context.Context, logger zerolog.Logger, namespace namespacePair) {
	logger = logger.With().Str("c", "count").Logger()
	sourceCount, targetCount := c.GetEstimates(ctx, namespace)
	logger.Info().Msgf("source estimate docs: %d, target estimate docs: %d", sourceCount, targetCount)

	if sourceCount != targetCount {
		c.reporter.MismatchCount(namespace.String(), sourceCount, targetCount)
		logger.Warn().Msg("estimated document counts don't match. (NOTE: this could be the result of metadata differences from unclean shutdowns, consider running a more exact countDocuments if all other tests pass)")
	} else {
		logger.Info().Msg("estimated document match")
	}
}

func (c *Comparer) GetEstimates(ctx context.Context, namespace namespacePair) (int64, int64) {
	sourceCount, err := c.sourceCollection(namespace.Db, namespace.Collection).EstimatedDocumentCount(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	targetCount, err := c.targetCollection(namespace.Db, namespace.Collection).EstimatedDocumentCount(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	return sourceCount, targetCount
}
