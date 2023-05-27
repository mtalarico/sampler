package util

import (
	"math"
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

func FormatIndexName(raw string) string {
	minusAcending := strings.ReplaceAll(raw, "_1", "")
	minusDescending := strings.ReplaceAll(minusAcending, "_-1", "")
	minusText := strings.ReplaceAll(minusDescending, "_text", "")
	minusHash := strings.ReplaceAll(minusText, "_hashed", "")
	minusWildcard := strings.ReplaceAll(minusHash, ".$**", "")
	minus2dSphere := strings.ReplaceAll(minusWildcard, "_2dsphere", "")
	minus2d := strings.ReplaceAll(minus2dSphere, "_2d", "")
	minusGeoHaystack := strings.ReplaceAll(minus2d, "_geoHaystack", "")
	return minusGeoHaystack
}
