package comparer

import (
	"context"

	"github.com/rs/zerolog/log"

	"sampler/internal/doc"
	"sampler/internal/ns"
	"sampler/internal/util"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (c *Comparer) CompareSampleDocs(namespace ns.Namespace) bool {
	ret := true
	cursor := c.sampleCursor(namespace)
	defer cursor.Close(context.TODO())
	sourceBuffer := make(map[interface{}]bson.Raw, BATCH_SIZE)
	i := 0
	log.Debug().Str("c", "sampleDoc").Str("ns", namespace.String()).Msg("beginning document check")
	for cursor.Next(context.TODO()) {
		var rawDoc bson.Raw
		cursor.Decode(&rawDoc)
		id := rawDoc.Lookup("_id").String()
		log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("deseralized doc %v", rawDoc)

		sourceBuffer[id] = rawDoc
		i++

		if i%BATCH_SIZE == 0 {
			log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("checking batch %d", i)
			if !c.batchEqual(namespace, sourceBuffer) {
				ret = false
			}
			sourceBuffer = make(map[interface{}]bson.Raw, BATCH_SIZE)
		}
	}
	// if we counted more than one doc but the counter did not land on a clean batch size there should still be items in the map to be checked
	if i > 0 && i%100 != 0 && !c.batchEqual(namespace, sourceBuffer) {
		ret = false
	}
	log.Debug().Str("c", "sampleDoc").Str("ns", namespace.String()).Msg("finished document check")

	if !ret {
		log.Error().Str("c", "sampleDoc").Str("ns", namespace.String()).Msg("sample documents did not match.")
		return false
	} else {
		log.Info().Str("c", "sampleDoc").Str("ns", namespace.String()).Msg("sample documents match.")
		return true
	}
}

func (c *Comparer) GetSampleSize(namespace ns.Namespace) int64 {
	sourceColl := c.sourceCollection(namespace)
	approxDocs, err := sourceColl.EstimatedDocumentCount(context.TODO(), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	sampleSize := util.GetSampleSize(approxDocs, c.config.Compare.Zscore, c.config.Compare.ErrorRate)
	log.Info().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("using sample size of %d for estimated %d source documents", sampleSize, approxDocs)
	return sampleSize
}

func (c *Comparer) sampleCursor(namespace ns.Namespace) *mongo.Cursor {
	sourceColl := c.sourceCollection(namespace)
	sampleSize := c.GetSampleSize(namespace)

	pipeline := bson.A{bson.D{{"$sample", bson.D{{"size", sampleSize}}}}}
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(BATCH_SIZE))
	log.Debug().Str("c", "sampleDoc").Str("ns", namespace.String()).Any("pipeline", pipeline).Any("options", opts).Msg("aggregating")

	cursor, err := sourceColl.Aggregate(context.TODO(), pipeline, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return cursor
}

func (c *Comparer) batchEqual(namespace ns.Namespace, sourceBuffer map[interface{}]bson.Raw) bool {
	targetBuffer := c.batchFind(namespace, sourceBuffer)
	return c.compareEachInBatch(namespace, sourceBuffer, targetBuffer)
}

func (c *Comparer) batchFind(namespace ns.Namespace, sourceDocs map[interface{}]bson.Raw) map[interface{}]bson.Raw {
	targetBuffer := make(map[interface{}]bson.Raw, BATCH_SIZE)
	var keys []bson.RawValue

	for _, value := range sourceDocs {
		keys = append(keys, value.Lookup("_id"))
	}
	filter := bson.M{"_id": bson.M{"$in": keys}}

	log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("filter: %s", filter)
	cursor, err := c.targetCollection(namespace).Find(context.TODO(), filter)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		var rawDoc bson.Raw
		cursor.Decode(&rawDoc)
		id := rawDoc.Lookup("_id").String()

		targetBuffer[id] = rawDoc
	}
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return targetBuffer
}

func (c *Comparer) compareEachInBatch(namespace ns.Namespace, sourceDocs map[interface{}]bson.Raw, targetDocs map[interface{}]bson.Raw) bool {
	equal := true
	for key, sourceDoc := range sourceDocs {
		if targetDoc, ok := targetDocs[key]; ok {
			mismatchDetails, err := doc.BsonUnorderedCompareRawDocumentWithDetails(sourceDoc, targetDoc)
			if err != nil {
				log.Fatal().Err(err).Msg("")
			}
			if mismatchDetails != nil {
				if c.config.Compare.PrintWholeDoc {
					log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("doc %v on source is not equal to doc %v on destination", sourceDoc, targetDoc)
				} else {
					log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("_id %v on source is not equal to _id %v on destination", sourceDoc.Lookup("_id"), targetDoc.Lookup("_id"))
				}
				log.Trace().Msgf("\treason: %#v\n", mismatchDetails)
				equal = false
			}
		} else {
			log.Trace().Str("c", "sampleDoc").Str("ns", namespace.String()).Msgf("_id %v not found on target", sourceDoc)
			equal = false
		}
	}
	return equal
}
