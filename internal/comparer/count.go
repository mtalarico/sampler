package comparer

import (
	"context"
	"sampler/internal/ns"

	"github.com/rs/zerolog/log"
)

func (c *Comparer) CompareEstimatedCounts(namespace ns.Namespace) bool {
	source, target := c.GetEstimates(namespace)
	if source != target {
		c.reporter.ReportMismatchCount(namespace, source, target)
		log.Warn().Str("c", "count").Str("ns", namespace.String()).Msg("estimated document counts don't match. (NOTE: this could be the result of metadata differences from unclean shutdowns, consider running a more exact countDocuments)")
		return false
	}
	log.Info().Str("c", "count").Str("ns", namespace.String()).Msg("estimated document match")
	return true
}

func (c *Comparer) GetEstimates(namespace ns.Namespace) (int64, int64) {
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
