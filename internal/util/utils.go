package util

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Pair[T any] struct {
	Source T
	Target T
}

func Max(a int, b int) int {
	switch a > b {
	case true:
		return a
	default:
		return b
	}
}

func Max64(a int64, b int64) int64 {
	switch a > b {
	case true:
		return a
	default:
		return b
	}
}

func Min64(a int64, b int64) int64 {
	switch a < b {
	case true:
		return a
	default:
		return b
	}
}

// using Cochran's sample size to get num docs needed
// for a population size (p), the z-value from a z-table (z), and an error rate (eps)
func GetSampleSize(p int64, z float64, eps float64) int64 {
	prop := 0.5 // 50% population proportion

	n_zero := (math.Pow(z, 2) * prop * (1 - prop)) / math.Pow(eps, 2)
	n_prime := n_zero / (1 + ((math.Pow(z, 2) * prop * (1 - prop)) / (math.Pow(eps, 2) * float64(p))))
	return int64(math.Round(n_prime))
}

func CleanPath(path string) string {
	cleaned, _ := strings.CutSuffix(path, "/")
	return cleaned
}

func IsMongos(client *mongo.Client) bool {
	result := client.Database("admin").RunCommand(context.TODO(), bson.D{{"isdbgrid", 1}})
	res, err := result.Raw()
	if err != nil {
		code := res.Lookup("code").AsInt64()
		if code == 59 {
			return false
		} else {
			log.Error().Err(err)
			return false
		}
	}
	return true
}

func SplitNamespace(ns string) (string, string, error) {
	db, coll, found := strings.Cut(ns, ".")
	if !found {
		return "", "", errors.New("malformed ns format")
	}
	return db, coll, nil
}
