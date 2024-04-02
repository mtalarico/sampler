package comparer

import (
	"context"
	"math"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"sampler/internal/doc"
	"sampler/internal/reporter"
	"sampler/internal/util"
	"sampler/internal/worker"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type documentBatch struct {
	dir   reporter.Direction
	batch batch
}

type batch map[string]bson.Raw

func (b batch) add(doc bson.Raw) {
	id := doc.Lookup("_id")
	key := id.String()
	if key != "" {
		b[key] = doc
	}
}

func (c *Comparer) CompareSampleDocs(ctx context.Context, logger zerolog.Logger, namespace namespacePair) {
	logger = logger.With().Str("c", "sampleDoc").Logger()
	source, target := c.sampleCursors(ctx, logger, namespace)
	defer source.Close(ctx)
	defer target.Close(ctx)
	// TODO variable batch size based on doc size (256MB)
	jobs := make(chan documentBatch, 100)

	pool := worker.NewWorkerPool(logger, NUM_WORKERS, "sampleDocWorkers", "sdw")
	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		c.processDocs(iCtx, iLogger, namespace, jobs)
	})

	logger.Info().Msg("beginning document sample")
	streamBatches(ctx, logger, jobs, reporter.SrcToDst, source)
	streamBatches(ctx, logger, jobs, reporter.DstToSrc, target)

	close(jobs)
	pool.Done()
	logger.Info().Msg("finished document sample")
}

func (c *Comparer) GetSampleSize(ctx context.Context, logger zerolog.Logger, namespace namespacePair) int64 {
	if c.config.Compare.ForceSampleSize > 0 {
		return c.config.Compare.ForceSampleSize
	}
	source, target := c.GetEstimates(ctx, namespace)
	// we warn about estimated counts, but they are not guarenteed to be equal, so sample from the larger of both collections
	max := util.Max64(source, target)
	ceiling := int64(math.Round(float64(max) * 0.04))
	sampleSize := util.GetSampleSize(max, c.config.Compare.Zscore, c.config.Compare.ErrorRate)
	if ceiling > 100 && sampleSize > ceiling {
		logger.Warn().Msgf("sample size %d too large, using maxSize %d", sampleSize, ceiling)
		return ceiling
	}
	return sampleSize
}

func (c *Comparer) sampleCursors(ctx context.Context, logger zerolog.Logger, namespace namespacePair) (*mongo.Cursor, *mongo.Cursor) {
	sampleSize := c.GetSampleSize(ctx, logger, namespace)
	logger.Info().Msgf("using sample size of %d", sampleSize)

	pipeline := bson.A{bson.D{{"$sample", bson.D{{"size", sampleSize}}}}, bson.D{{"$sort", bson.D{{"_id", 1}}}}}
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(BATCH_SIZE))
	logger.Debug().Any("pipeline", pipeline).Any("options", opts).Msg("aggregating")

	srcCursor, err := c.sourceCollection(namespace.Db, namespace.Collection).Aggregate(ctx, pipeline, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	tgtCursor, err := c.targetCollection(namespace.Db, namespace.Collection).Aggregate(ctx, pipeline, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return srcCursor, tgtCursor
}

// TODO VARIABLE BATCH SIZE
func streamBatches(ctx context.Context, logger zerolog.Logger, jobs chan documentBatch, dir reporter.Direction, cursor *mongo.Cursor) {
	logger = logger.With().Str("dir", string(dir)).Logger()
	docCount := 0
	batchCount := 0
	buffer := make(batch, BATCH_SIZE)
	logger.Debug().Msg("starting cursor walk")
	for cursor.Next(ctx) {
		var doc bson.Raw
		cursor.Decode(&doc)
		logger.Trace().Msgf("deseralized doc %v", doc)
		buffer.add(doc)
		docCount++

		if docCount%BATCH_SIZE == 0 {
			logger.Debug().Msgf("adding batch %d to be checked", batchCount+1)
			jobs <- documentBatch{
				dir:   dir,
				batch: buffer,
			}
			buffer = make(batch, BATCH_SIZE)
			batchCount++
		}
	}
	// if we counted more than one doc but the counter did not land on a clean batch size, flush the rest
	if len(buffer) != 0 {
		logger.Debug().Msgf("adding batch %d to be checked", batchCount+1)
		jobs <- documentBatch{
			dir:   dir,
			batch: buffer,
		}
		batchCount++
	}
}

func (c *Comparer) batchFind(ctx context.Context, logger zerolog.Logger, namespace namespacePair, toFind documentBatch) documentBatch {
	buffer := make(batch, BATCH_SIZE)
	var coll *mongo.Collection
	switch toFind.dir {
	case reporter.SrcToDst:
		coll = c.targetCollection(namespace.Db, namespace.Collection)
	case reporter.DstToSrc:
		coll = c.sourceCollection(namespace.Db, namespace.Collection)
	default:
		logger.Fatal().Msg("invalid comparison direction?")
	}

	filters := bson.A{}
	for _, value := range toFind.batch {
		filter := bson.D{{"_id", value.Lookup("_id")}}
		// if collection is sharded, include shard key value in query to target that shard
		if toFind.dir == reporter.SrcToDst && namespace.Partitioned.Target {
			logger.Trace().Msg("Target is sharded, adding shard key to filter")
			elems, err := namespace.PartitionKey.Target.Elements()
			if err != nil {
				log.Error().Err(err)
			}
			for _, each := range elems {
				filter = append(filter, bson.E{each.Key(), value.Lookup(each.Key())})
			}
		} else if toFind.dir == reporter.DstToSrc && namespace.Partitioned.Source {
			logger.Trace().Msg("Source is sharded, adding shard key to filter")
			elems, err := namespace.PartitionKey.Source.Elements()
			if err != nil {
				log.Error().Err(err)
			}
			for _, each := range elems {
				filter = append(filter, bson.E{each.Key(), value.Lookup(each.Key())})
			}
		}
		filters = append(filters, filter)
	}
	or := bson.D{{"$or", filters}}
	cursor, err := coll.Find(ctx, or, nil)

	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.Raw
		cursor.Decode(&doc)
		logger.Trace().Msgf("decoded doc %+v", doc)

		buffer.add(doc)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	logger.Trace().Msgf("filled buffer %+v", buffer)
	return documentBatch{
		dir:   toFind.dir,
		batch: buffer,
	}
}

func (c *Comparer) batchCompare(ctx context.Context, logger zerolog.Logger, namespace namespacePair, a documentBatch, b documentBatch) reporter.DocSummary {
	var summary reporter.DocSummary
	var outer, inner batch
	if len(b.batch) > len(a.batch) {
		outer, inner = b.batch, a.batch
	} else {
		outer, inner = a.batch, b.batch

	}
	logger.Trace().Msgf("outer: %s | inner: %s", outer, inner)
	for key, aDoc := range outer {
		logger.Trace().Msgf("checking key %s against %s", key, inner[key])
		if bDoc, ok := inner[key]; ok {
			logger.Trace().Msgf("comparing %v to %v", aDoc, bDoc)
			comparison, err := doc.BsonUnorderedCompareRawDocumentWithDetails(aDoc, bDoc)
			if err != nil {
				log.Error().Err(err).Msg("")
			}
			if comparison == nil {
				summary.Equal++
				continue
			}
			if len(comparison.MissingFieldOnDst) > 0 {
				logger.Debug().Msgf("%s is missing fields on the target", key)
			}
			if len(comparison.MissingFieldOnSrc) > 0 {
				logger.Debug().Msgf("%s is missing fields on the source", key)
			}
			if len(comparison.FieldContentsDiffer) > 0 {
				logger.Debug().Msgf("%s is different between the source and target", key)
			}
			c.reporter.MismatchDoc(namespace.String(), a.dir, aDoc, bDoc)
			summary.Different++
		} else {
			logger.Debug().Msgf("_id %v not found", key)
			c.reporter.MissingDoc(namespace.String(), a.dir, aDoc)
			summary.Missing++
		}
	}
	return summary
}

func (c *Comparer) processDocs(ctx context.Context, logger zerolog.Logger, namespace namespacePair, jobs chan documentBatch) {
	for processing := range jobs {
		logger = logger.With().Str("dir", string(processing.dir)).Logger()
		lookedUp := c.batchFind(ctx, logger, namespace, processing)
		summary := c.batchCompare(ctx, logger, namespace, processing, lookedUp)
		if summary.HasMismatches() {
			logger.Error().Msg("mismatch in batch")
			c.reporter.SampleSummary(namespace.String(), processing.dir, summary)
		}
	}

}
