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

	comparison := diff.Diff(logger, sortedSource, sortedTarget)
	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Error().Msg("there are namespace mismatches between source and destination")
	}
	for _, each := range comparison.MissingOnSrc {
		logger.Error().Str("ns", each.Namespace.String()).Msg("exists on the source but not the target")
		c.reporter.ReportMissingNamespace(each.Namespace, "source")
	}
	for _, each := range comparison.MissingOnTgt {
		logger.Error().Str("ns", each.Namespace.String()).Msg("exists on the target but not the source")
		c.reporter.ReportMissingNamespace(each.Namespace, "target")
	}
	for _, each := range comparison.Different {
		logger.Error().Str("ns", each.Source.Namespace.String()).Msg("different between the source and target")
		c.reporter.ReportMismatchNamespace(each.Source.Namespace, each.Target.Namespace)
		logger.Trace().Msgf("putting ns %s on channel", each.Source.Namespace)
		ret <- each.Source.Namespace
	}
	for _, each := range comparison.Equal {
		logger.Trace().Msgf("putting ns %s on channel", each.Namespace)
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
