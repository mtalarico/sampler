package comparer

import (
	"context"

	"sampler/internal/index"
	"sampler/internal/ns"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/google/go-cmp/cmp"
)

func (c *Comparer) CompareIndexes(namespace ns.Namespace) bool {

	source, target := c.getIndexSpecs(namespace)

	isEqual := cmp.Equal(source, target)

	if !isEqual {
		diff := index.DiffIndexes(namespace, source, target)
		log.Debug().Str("c", "index").Str("ns", namespace.String()).Msgf("%s", diff.ToString())
		c.reporter.ReportMismatchIndexes(namespace, diff)
		log.Error().Str("c", "index").Str("ns", namespace.String()).Msg("indexes are not the same.")
		return false
	}
	log.Info().Str("c", "index").Str("ns", namespace.String()).Msg("indexes match.")
	return true
}

func (c *Comparer) getIndexSpecs(namespace ns.Namespace) ([]*mongo.IndexSpecification, []*mongo.IndexSpecification) {
	sourceSpecs, err := c.sourceCollection(namespace).Indexes().ListSpecifications(context.TODO(), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	targetSpecs, err := c.targetCollection(namespace).Indexes().ListSpecifications(context.TODO(), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	return sourceSpecs, targetSpecs
}
