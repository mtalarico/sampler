package diff

import (
	"sampler/internal/util"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog/log"
)

type NamedComparable interface {
	GetName() string
	Equal(interface{}) bool
}

type pair[T NamedComparable] struct {
	Source T
	Target T
}

type MismatchDetails[T NamedComparable] struct {
	MissingOnSrc []T
	MissingOnTgt []T
	Different    []pair[T]
	Equal        []T
}

func (m MismatchDetails[T]) String() string {
	var b strings.Builder
	b.WriteString("{ ")

	b.WriteString("MissingOnSrc: [ ")
	for _, each := range m.MissingOnSrc {
		b.WriteString(each.GetName() + " ")
	}
	b.WriteString("], ")

	b.WriteString("MissingOnTgt: [ ")
	for _, each := range m.MissingOnTgt {
		b.WriteString(each.GetName() + " ")
	}
	b.WriteString("], ")

	b.WriteString("Different: [ ")
	for _, each := range m.Different {
		b.WriteString("src:" + each.Source.GetName() + "|tgt:" + each.Target.GetName() + " ")
	}
	b.WriteString("], ")

	b.WriteString("Equal: [ ")
	for _, each := range m.Equal {
		b.WriteString(each.GetName() + " ")
	}
	b.WriteString("]")
	b.WriteString(" }")

	return b.String()
}

// Walk both slices determining if missing from the source, missing from the target, or are present on both and different.
// Sort source and target before diff
func Diff[T NamedComparable](source []T, target []T) MismatchDetails[T] {
	var missingOnSrc, missingOnTgt, equal []T
	var different []pair[T]

	srcLen := len(source)
	tgtLen := len(target)
	if srcLen == 0 || tgtLen == 0 {
		// we have pre-checked namespaces existence, should at least have _id
		log.Fatal().Str("c", "diff").Msg("something is wrong")
	}
	maxLen := util.Max(srcLen, tgtLen)
	log.Debug().Str("c", "diff").Msgf("source count: %d, target count: %d", srcLen, tgtLen)

	for srcItr, tgtItr := 0, 0; !(srcItr >= maxLen) || !(tgtItr >= maxLen); {
		log.Trace().Str("c", "diff").Msgf("srcItr %d, tgtItr %d, srcLen %d, tgtLen %d, maxLen %d", srcItr, tgtItr, srcLen, tgtLen, maxLen)

		// These two checks for an itr being exhausted come first to handle length mismatches > 1 (e.g. [a_1] vs [a_1, b_1, c_1])
		if srcItr >= srcLen {
			log.Trace().Str("c", "diff").Msg("out of items on source")
			missingOnSrc = append(missingOnSrc, target[tgtItr])
			log.Trace().Str("c", "diff").Msgf("incrementing tgt %d -> %d (src: %d)", tgtItr, tgtItr+1, srcItr)
			tgtItr++
			continue
		}

		if tgtItr >= tgtLen {
			log.Trace().Str("c", "diff").Msg("out of items on target")
			missingOnTgt = append(missingOnTgt, source[srcItr])
			log.Trace().Str("c", "diff").Msgf("incrementing src %d -> %d (tgt: %d)", srcItr, srcItr+1, tgtItr)
			srcItr++
			continue
		}

		// both iterators are not expired (they passed the above two ifs)
		srcName := source[srcItr].GetName()
		tgtName := target[tgtItr].GetName()

		if tgtName < srcName {
			log.Trace().Str("c", "diff").Msgf("srcName (%s) > tgtName (%s) -- missing %s on source", srcName, tgtName, tgtName)
			missingOnSrc = append(missingOnSrc, target[tgtItr])
			log.Trace().Str("c", "diff").Msgf("incrementing tgt %d -> %d (src: %d)", tgtItr, tgtItr+1, srcItr)
			tgtItr++
			continue
		}

		if srcName < tgtName {
			log.Trace().Str("c", "diff").Msgf("tgtName (%s) > srcName (%s) -- missing %s on target", tgtName, srcName, srcName)
			missingOnTgt = append(missingOnTgt, source[srcItr])
			log.Trace().Str("c", "diff").Msgf("incrementing src %d -> %d (tgt: %d)", srcItr, srcItr+1, tgtItr)
			srcItr++
			continue
		}

		// if we got past everything, we are in the equality condition and check the specs against equal other before moving each cursor forward one
		log.Trace().Str("c", "diff").Msgf("checking Name src %s (itr: %d), dst %s (itr: %d)", srcName, srcItr, tgtName, tgtItr)
		if !cmp.Equal(source[srcItr], target[tgtItr]) {
			log.Trace().Str("c", "diff").Msgf("srcName (%s) != tgtName (%s)", srcName, tgtName)
			log.Trace().Str("c", "diff").Msgf("src %+v | tgt %+v", source[srcItr], target[tgtItr])
			diffPair := pair[T]{Source: source[srcItr], Target: target[tgtItr]}
			different = append(different, diffPair)
			spew.Printf("%+v", source[srcItr])
			spew.Printf("%+v", target[tgtItr])
			srcItr, tgtItr = srcItr+1, tgtItr+1
			continue
		}

		log.Trace().Str("c", "diff").Msgf("incrementing src: %d -> %d, tgt %d -> %d", srcItr, srcItr+1, tgtItr, tgtItr+1)
		equal = append(equal, source[srcItr])
		srcItr, tgtItr = srcItr+1, tgtItr+1
	}
	return MismatchDetails[T]{
		MissingOnSrc: missingOnSrc,
		MissingOnTgt: missingOnTgt,
		Different:    different,
		Equal:        equal,
	}
}
