package comparer

import (
	"context"
	"sampler/internal/diff"
	"sampler/internal/ns"
	"sampler/internal/util"

	"github.com/rs/zerolog/log"
)

func (c *Comparer) streamNamespaces(ctx context.Context, ret chan ns.Namespace) {
	source, err := ns.UserCollections(&c.sourceClient, false, c.config.MetaDBName)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	target, err := ns.UserCollections(&c.targetClient, false, c.config.MetaDBName)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	// sort first here
	wrappedSource, wrappedTarget := wrapColls(source), wrapColls(target)
	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)

	mismatches := diff.Diff(sortedSource, sortedTarget)
	log.Debug().Str("c", "namespace").Msgf("%s", mismatches.String())
	for _, each := range mismatches.Equal {
		log.Trace().Msgf("putting %s on the channel", each.Namespace)
		ret <- each.Namespace
	}
}
