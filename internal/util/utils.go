package util

import (
	"math"
	"sort"
	"strings"
)

func Max(a int, b int) int {
	switch a > b {
	case true:
		return a
	default:
		return b
	}
}

func Min(a int, b int) int {
	switch a < b {
	case true:
		return a
	default:
		return b
	}
}

func GetSampleSize(N int64, z float64, eps float64) int64 {
	prop := 0.5 // 50% population proportion

	n := (math.Pow(z, 2) * prop * (1 - prop)) / math.Pow(eps, 2)
	n_prime := n / (1 + ((math.Pow(z, 2) * prop * (1 - prop)) / (math.Pow(eps, 2) * float64(N))))
	return int64(math.Round(n_prime))
}

func CleanPath(path string) string {
	cleaned, _ := strings.CutSuffix(path, "/")
	return cleaned
}

type Named interface {
	GetName() string
}

func SortSpec[T Named](spec []T) []T {
	sort.SliceStable(spec, func(a, b int) bool {
		src := spec[a].GetName()
		tgt := spec[b].GetName()
		return src < tgt
	})
	return spec
}
