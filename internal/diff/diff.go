package diff

import (
	"sampler/internal/util"
	"sort"
	"strings"

	"github.com/rs/zerolog"
)

type NamedComparable interface {
	GetName() string
	Equal(any) bool
}

func SortSpec[T NamedComparable](spec []T) []T {
	sort.SliceStable(spec, func(a, b int) bool {
		src := spec[a].GetName()
		tgt := spec[b].GetName()
		return src < tgt
	})
	return spec
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

func (m MismatchDetails[T]) HasMismatches() bool {
	return len(m.MissingOnSrc) > 0 || len(m.MissingOnTgt) > 0 || len(m.Different) > 0
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

// Walk both sorted slices determining if missing from the source, missing from the target, or are present on both and different.
// ** assumed slices are sorted before-hand **
func Diff[T NamedComparable](logger zerolog.Logger, source []T, target []T) MismatchDetails[T] {
	var missingOnSrc, missingOnTgt, equal []T
	var different []pair[T]
	logger = logger.With().Str("c", "diff").Logger()

	srcLen, tgtLen := len(source), len(target)
	if srcLen == 0 {
		missingOnSrc = append(missingOnSrc, target...)
		return MismatchDetails[T]{
			MissingOnSrc: missingOnSrc,
			MissingOnTgt: missingOnTgt,
			Different:    different,
			Equal:        equal,
		}
	}
	if tgtLen == 0 {
		missingOnTgt = append(missingOnTgt, source...)
		return MismatchDetails[T]{
			MissingOnSrc: missingOnSrc,
			MissingOnTgt: missingOnTgt,
			Different:    different,
			Equal:        equal,
		}
	}
	maxLen := util.Max(srcLen, tgtLen)
	logger.Debug().Msgf("source count: %d, target count: %d", srcLen, tgtLen)

	for srcItr, tgtItr := 0, 0; srcItr < srcLen || tgtItr < tgtLen; {
		logger.Trace().Msgf("srcItr %d, tgtItr %d, srcLen %d, tgtLen %d, maxLen %d", srcItr, tgtItr, srcLen, tgtLen, maxLen)

		// Handle remaining elements on either side
		if srcItr >= srcLen {
			logger.Trace().Msg("out of items on source")
			missingOnSrc = append(missingOnSrc, target[tgtItr])
			logger.Trace().Msgf("incrementing tgt %d -> %d (src: %d)", tgtItr, tgtItr+1, srcItr)
			tgtItr++
			continue
		}
		if tgtItr >= tgtLen {
			logger.Trace().Msg("out of items on target")
			missingOnTgt = append(missingOnTgt, source[srcItr])
			logger.Trace().Msgf("incrementing src %d -> %d (tgt: %d)", srcItr, srcItr+1, tgtItr)
			srcItr++
			continue
		}

		// both iterators are not expired (they passed the above two ifs)
		srcName, tgtName := source[srcItr].GetName(), target[tgtItr].GetName()

		switch {
		case tgtName < srcName:
			logger.Trace().Msgf("srcName (%s) > tgtName (%s) -- missing %s on source", srcName, tgtName, tgtName)
			missingOnSrc = append(missingOnSrc, target[tgtItr])
			tgtItr++
		case srcName < tgtName:
			logger.Trace().Msgf("tgtName (%s) > srcName (%s) -- missing %s on target", tgtName, srcName, srcName)
			missingOnTgt = append(missingOnTgt, source[srcItr])
			srcItr++
		default: // Equality condition
			logger.Trace().Msgf("checking Name src %s (itr: %d), dst %s (itr: %d)", srcName, srcItr, tgtName, tgtItr)
			if !source[srcItr].Equal(target[tgtItr]) {
				logger.Trace().Msgf("srcName (%s) != tgtName (%s)", srcName, tgtName)
				logger.Trace().Msgf("src %+v | tgt %+v", source[srcItr], target[tgtItr])
				diffPair := pair[T]{Source: source[srcItr], Target: target[tgtItr]}
				different = append(different, diffPair)
			} else {
				logger.Trace().Msgf("incrementing src: %d -> %d, tgt %d -> %d", srcItr, srcItr+1, tgtItr, tgtItr+1)
				equal = append(equal, source[srcItr])
			}
			srcItr, tgtItr = srcItr+1, tgtItr+1
		}
	}

	return MismatchDetails[T]{
		MissingOnSrc: missingOnSrc,
		MissingOnTgt: missingOnTgt,
		Different:    different,
		Equal:        equal,
	}
}
