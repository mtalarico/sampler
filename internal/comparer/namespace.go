package comparer

import (
	"context"
	"sampler/internal/diff"
	"sampler/internal/ns"
	"sampler/internal/util"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// streams back namespaces that are equal on both the source and target, reports namespaces that are different or missing
func (c *Comparer) streamNamespaces(ctx context.Context, logger zerolog.Logger, ret chan ns.Namespace) {
	logger = logger.With().Str("c", "namespace").Logger()

	source, target := c.getNamespaces(ctx)
	wrappedSource, wrappedTarget := wrapColls(source), wrapColls(target)
	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)

	comparison := diff.Diff(sortedSource, sortedTarget)
	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Error().Msg("indexes are not the same.")
	} else {
		logger.Info().Msg("indexes match.")
	}
	for _, each := range comparison.MissingOnSrc {
		c.reporter.ReportMissingNamespace(each.Namespace, "source")
	}
	for _, each := range comparison.MissingOnTgt {
		c.reporter.ReportMissingNamespace(each.Namespace, "target")
	}
	for _, each := range comparison.Different {
		c.reporter.ReportMismatchNamespace(each.Source.Namespace, each.Target.Namespace)
		logger.Trace().Msgf("putting %s on the channel", each.Source.Namespace)
		ret <- each.Source.Namespace
	}
	for _, each := range comparison.Equal {
		logger.Trace().Msgf("putting %s on the channel", each.Namespace)
		ret <- each.Namespace
	}

}

func (c *Comparer) getNamespaces(ctx context.Context) ([]ns.Namespace, []ns.Namespace) {
	source, err := ns.UserCollections(&c.sourceClient, false, c.config.MetaDBName)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	target, err := ns.UserCollections(&c.targetClient, false, c.config.MetaDBName)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return source, target
}
