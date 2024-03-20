package comparer

import (
	"context"
	"sampler/internal/diff"
	"sampler/internal/ns"
	"sampler/internal/util"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type namespacePair struct {
	Db            string
	Collection    string
	Partitioned   util.Pair[bool]
	PartitionKey  util.Pair[bson.Raw]
	Specification *mongo.CollectionSpecification
}

func (ns namespacePair) String() string {
	return ns.Db + "." + ns.Collection
}

func (ns namespacePair) Debug() string {
	base := "{ name: " + ns.Db + "." + ns.Collection + ", src: { partioned: " + strconv.FormatBool(ns.Partitioned.Source)
	if ns.Partitioned.Source {
		base += ", key: " + ns.PartitionKey.Source.String()
	}
	base += " }, dst: { partitioned: " + strconv.FormatBool(ns.Partitioned.Target)
	if ns.Partitioned.Target {
		base += ", key: " + ns.PartitionKey.Target.String()
	}
	return base + " }"
}

type namespaceMap struct {
	Source []ns.Namespace
	Target []ns.Namespace
}

// streams namespaces to worker threads that are present on both the source and target, reports namespaces that are different or missing
func (c *Comparer) streamNamespaces(ctx context.Context, logger zerolog.Logger, ret chan namespacePair) {
	logger = logger.With().Str("c", "namespace").Logger()

	nsMap := c.getNamespaces(ctx)
	if len(nsMap.Source) == 0 && len(nsMap.Target) == 0 {
		log.Info().Msg("no user namespaces found on the source or target... nothing to do")
		return
	}

	// wrap in a type that satisfies NamedComparable and sorty to be properly diff'd
	wrappedSource, wrappedTarget := wrapColls(nsMap.Source), wrapColls(nsMap.Target)
	sortedSource := util.SortSpec(wrappedSource)
	sortedTarget := util.SortSpec(wrappedTarget)

	comparison := diff.Diff(logger, sortedSource, sortedTarget)
	logger.Trace().Msgf("%s", comparison.String())
	if comparison.HasMismatches() {
		logger.Warn().Msg("there are namespace mismatches between source and target")
		logger.Debug().Msgf("%s", comparison.String())
	}
	for _, each := range comparison.Equal {
		c.makeNamespacePair(logger, each.Namespace, ret)
	}
	for _, each := range comparison.MissingOnSrc {
		logger.Error().Str("ns", each.Namespace.String()).Msgf("%s missing on the source", each.Namespace.String())
		c.reporter.MissingNamespace(each.Namespace.String(), "source")
	}
	for _, each := range comparison.MissingOnTgt {
		logger.Error().Str("ns", each.Namespace.String()).Msgf("%s missing on the target", each.Namespace.String())
		c.reporter.MissingNamespace(each.Namespace.String(), "target")
	}
	for _, each := range comparison.Different {
		logger.Error().Str("ns", each.Source.Namespace.String()).Msgf("%s different between the source and target", each.Source.String())
		c.reporter.MismatchNamespace(each.Source.Namespace, each.Target.Namespace)
		logger.Trace().Msgf("putting ns %s on channel", each.Source.Namespace)
		c.makeNamespacePair(logger, each.Source.Namespace, ret)
	}
}

// TODO: improve this to have fully independant sharded awareness
func (c *Comparer) makeNamespacePair(logger zerolog.Logger, namespace ns.Namespace, ret chan namespacePair) {
	sourceSharded, sourceKey := ns.IsSharded(&c.sourceClient, namespace.Db, namespace.Collection)
	targetSharded, targetKey := ns.IsSharded(&c.targetClient, namespace.Db, namespace.Collection)

	pair := namespacePair{
		Db:            namespace.Db,
		Collection:    namespace.Collection,
		Specification: namespace.Specification,
		Partitioned: util.Pair[bool]{
			Source: sourceSharded,
			Target: targetSharded,
		},
		PartitionKey: util.Pair[bson.Raw]{
			Source: sourceKey,
			Target: targetKey,
		},
	}

	logger.Trace().Msgf("putting ns-pair %s on channel", pair.Debug())
	ret <- pair
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

// do not need to worry about missing namespaces here, if the namespace is found, we'll find it in the diff
func (c *Comparer) includedUserNamespaces(ctx context.Context, included []string) namespaceMap {
	var source, target []ns.Namespace
	for _, each := range included {
		db, coll, err := util.SplitNamespace(each)
		if err != nil {
			log.Error().Err(err).Msg("")
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
