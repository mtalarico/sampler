package index

import (
	"sampler/internal/ns"
	"sampler/internal/util"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type indexPair struct {
	Source *mongo.IndexSpecification
	Target *mongo.IndexSpecification
}

type IndexMismatchDetails struct {
	MissingIndexOnSrc []*mongo.IndexSpecification
	MissingIndexOnTgt []*mongo.IndexSpecification
	IndexDifferent    []indexPair
}

func (m IndexMismatchDetails) ToString() string {
	var b strings.Builder
	b.WriteString("{ ")

	b.WriteString("MissingIndexOnSrc: [ ")
	for _, each := range m.MissingIndexOnSrc {
		b.WriteString(each.Name + " ")
	}
	b.WriteString("], ")

	b.WriteString("MissingIndexOnTgt: [ ")
	for _, each := range m.MissingIndexOnTgt {
		b.WriteString(each.Name)
	}
	b.WriteString("], ")

	b.WriteString("IndexDifferent: [ ")
	for _, each := range m.IndexDifferent {
		b.WriteString("src:" + each.Source.Name + "|tgt:" + each.Target.Name + " ")
	}
	b.WriteString("]")
	b.WriteString(" }")

	return b.String()
}

// Walk both slices determining if indexes are missing from the source, missing from the target, or are present on both and different.
func DiffIndexes(namespace ns.Namespace, source []*mongo.IndexSpecification, target []*mongo.IndexSpecification) IndexMismatchDetails {
	var MissingIndexOnSrc, MissingIndexOnTgt []*mongo.IndexSpecification
	var indexContentsDiffer []indexPair

	src := SortIndexSpec(source)
	tgt := SortIndexSpec(target)
	srcLen := len(src)
	tgtLen := len(tgt)
	if srcLen == 0 || tgtLen == 0 {
		log.Fatal().Str("c", "index").Str("ns", namespace.String()).Msg("something went horribly wrong, was the namespace dropped?")
	}
	maxLen := util.Max(srcLen, tgtLen)
	log.Debug().Str("c", "index").Str("ns", namespace.String()).Msgf("source index count: %d, target index count: %d", srcLen, tgtLen)
	for srcItr, tgtItr := 0, 0; util.Max(srcItr, tgtItr) < maxLen; {
		log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("srcItr %d, tgtItr %d, srcLen %d, tgtLen %d, maxLen %d", srcItr, tgtItr, srcLen, tgtLen, maxLen)

		if srcItr >= srcLen {
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msg("out of items on source")
			MissingIndexOnSrc = append(MissingIndexOnSrc, tgt[tgtItr])
			// c.reporter.ReportMissingIndex(namespace, target[tgtItr], reporter.Source)
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing tgt (src: %d, tgt: %d -> %d)", srcItr, tgtItr, tgtItr+1)
			tgtItr++
			continue
		}

		if tgtItr >= tgtLen {
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msg("out of items on target")
			MissingIndexOnTgt = append(MissingIndexOnTgt, src[srcItr])
			// c.reporter.ReportMissingIndex(namespace, source[srcItr], reporter.Target)
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing src (src: %d -> %d, tgt: -> %d)", srcItr, srcItr+1, tgtItr)
			srcItr++
			continue
		}

		srcName := util.FormatIndexName(src[srcItr].Name)
		tgtName := util.FormatIndexName(tgt[tgtItr].Name)

		// partway through, if the source is greater than the target, we diverged and are missing on the source and need to increment target until we get to equality or finish checking
		for tgtItr < tgtLen && tgtName < srcName {
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("srcName (%s) > tgtName (%s) -- missing %s on source", srcName, tgtName, tgtName)
			MissingIndexOnSrc = append(MissingIndexOnSrc, tgt[tgtItr])
			// c.reporter.ReportMissingIndex(namespace, target[tgtItr], reporter.Source)
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing tgt (src: %d, tgt: %d -> %d)", srcItr, tgtItr, tgtItr+1)
			tgtItr++
			if tgtItr < tgtLen {
				tgtName = util.FormatIndexName(target[tgtItr].Name)
			}
		}

		for srcItr < srcLen && srcName < tgtName {
			srcName = util.FormatIndexName(src[srcItr].Name)
			tgtName = util.FormatIndexName(target[tgtItr].Name)
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("tgtName (%s) > srcName (%s) -- missing %s on target", tgtName, srcName, srcName)
			MissingIndexOnTgt = append(MissingIndexOnTgt, src[srcItr])
			// c.reporter.ReportMissingIndex(namespace, source[srcItr], reporter.Target)
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing src (src: %d -> %d, tgt: -> %d)", srcItr, srcItr+1, tgtItr)
			srcItr++
			if srcItr < srcLen {
				srcName = util.FormatIndexName(src[srcItr].Name)
			}
		}

		srcName = util.FormatIndexName(source[srcItr].Name)
		tgtName = util.FormatIndexName(tgt[tgtItr].Name)

		// if we got past the last two loops, we are in the equality condition
		log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("checking name src %s, dst %s", srcName, tgtName)
		if !cmp.Equal(source[srcItr], tgt[tgtItr]) {
			log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("srcName (%s) != tgtName (%s)", srcName, tgtName)
			indexContentsDiffer = append(indexContentsDiffer, indexPair{Source: src[srcItr], Target: tgt[tgtItr]})
			// c.reporter.ReportDifferentIndex(namespace, source[srcItr], target[tgtItr])
		}

		log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing src: %d -> %d", srcItr, srcItr+1)
		srcItr++
		log.Trace().Str("c", "index").Str("ns", namespace.String()).Msgf("incrementing tgt: %d -> %d", tgtItr, tgtItr+1)
		tgtItr++
	}
	return IndexMismatchDetails{
		MissingIndexOnSrc,
		MissingIndexOnTgt,
		indexContentsDiffer,
	}
}

func SortIndexSpec(unsorted []*mongo.IndexSpecification) []*mongo.IndexSpecification {
	sorted := make([]*mongo.IndexSpecification, len(unsorted))
	copy(sorted, unsorted)
	sort.SliceStable(sorted, func(i, j int) bool {
		srcName := util.FormatIndexName(sorted[i].Name)
		tgtName := util.FormatIndexName(sorted[j].Name)
		return srcName < tgtName
	})
	return sorted
}
