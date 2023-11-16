package comparer

import (
	"context"
	"sampler/internal/diff"
	"sampler/internal/ns"
	"sampler/internal/util"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type namespaceMap struct {
	Source []ns.Namespace
	Target []ns.Namespace
}

// streams back namespaces that are equal on both the source and target, reports namespaces that are different or missing
func (c *Comparer) streamNamespaces(ctx context.Context, logger zerolog.Logger, ret chan ns.Namespace) {
	logger = logger.With().Str("c", "namespace").Logger()

	nsMap := c.getNamespaces(ctx)

	// if len(source) == 0 {
	// 	logger.Error().Msg("no namespaces found on source, nothing to compare")
	// 	return
	// }

	wrappedSource, wrappedTarget := wrapColls(nsMap.Source), wrapColls(nsMap.Target)
	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)

	comparison := diff.Diff(logger, sortedSource, sortedTarget)
	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Warn().Msg("there are namespace mismatches between source and destination")
		logger.Debug().Msgf("%s", comparison.String())

	}
	for _, each := range comparison.Equal {
		logger.Trace().Msgf("putting ns %s on channel", each.Namespace)
		ret <- each.Namespace
	}
	if c.config.DryRun {
		return
	}
	for _, each := range comparison.MissingOnSrc {
		logger.Error().Str("ns", each.Namespace.String()).Msgf("%s missing on the source", each.Namespace.String())
		c.reporter.MissingNamespace(each.Namespace, "source")
	}
	for _, each := range comparison.MissingOnTgt {
		logger.Error().Str("ns", each.Namespace.String()).Msgf("%s missing on the target", each.Namespace.String())
		c.reporter.MissingNamespace(each.Namespace, "target")
	}
	for _, each := range comparison.Different {
		logger.Error().Str("ns", each.Source.Namespace.String()).Msgf("%s different between the source and target", each.Source.String())
		c.reporter.MismatchNamespace(each.Source.Namespace, each.Target.Namespace)
		logger.Trace().Msgf("putting ns %s on channel", each.Source.Namespace)
		ret <- each.Source.Namespace
	}
}

func (c *Comparer) getNamespaces(ctx context.Context) namespaceMap {
	if len(*c.config.IncludeNS) > 0 {
		log.Info().Strs("includeNS", *c.config.IncludeNS).Msg("looking for included namespaces")
		return c.includedUserNamespaces(ctx, *c.config.IncludeNS)
	} else {
		log.Info().Msg("looking for all user namespaces")
		return c.allUserNamespaces(ctx)
	}
}

func (c *Comparer) includedUserNamespaces(ctx context.Context, included []string) namespaceMap {
	var source, target []ns.Namespace
	for _, each := range included {
		db, coll, err := util.SplitNamespace(each)
		if err != nil {
			log.Error().Err(err).Msg("error splitting namespace")
		}
		if eachSrc, err := ns.GetOneUserCollections(&c.sourceClient, db, coll); err == nil {
			source = append(source, eachSrc)
		}
		if eachTgt, err := ns.GetOneUserCollections(&c.targetClient, db, coll); err == nil {
			target = append(target, eachTgt)
		}
	}
	return namespaceMap{Source: source, Target: target}
}

func (c *Comparer) allUserNamespaces(ctx context.Context) namespaceMap {
	source, err := ns.AllUserCollections(&c.sourceClient, false, c.config.MetaDBName)
	if err != nil {
		log.Error().Err(err).Msg("")
	}
	target, err := ns.AllUserCollections(&c.targetClient, false, c.config.MetaDBName)
	if err != nil {
		log.Error().Err(err).Msg("")
	}
	return namespaceMap{Source: source, Target: target}
}
