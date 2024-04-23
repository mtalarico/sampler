package comparer

import (
	"context"
	"math"
	"sync"
	"time"

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

const retryInterval = 500 * time.Millisecond

type documentBatch struct {
	dir   util.Direction
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

type collectionTotals struct {
	ns               string
	lock             sync.Mutex
	sampledSrc       int64
	sampledTgt       int64
	missingSrc       int64
	missingTgt       int64
	mismatchSrcToTgt int64
	mismatchTgtToSrc int64
}

func (c *Comparer) CompareSampleDocs(ctx context.Context, logger zerolog.Logger, namespace namespacePair) {
	totals := collectionTotals{
		ns:               namespace.String(),
		lock:             sync.Mutex{},
		sampledSrc:       0,
		sampledTgt:       0,
		missingSrc:       0,
		missingTgt:       0,
		mismatchSrcToTgt: 0,
		mismatchTgtToSrc: 0,
	}
	logger = logger.With().Str("c", "sampleDoc").Logger()
	source, target := c.sampleCursors(ctx, logger, namespace)
	defer source.Close(ctx)
	defer target.Close(ctx)
	// TODO variable batch size based on doc size (256MB)
	jobs := make(chan documentBatch, 100)

	pool := worker.NewWorkerPool(logger, NUM_WORKERS, "sampleDocWorkers", "sdw")
	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		c.processDocs(iCtx, iLogger, namespace, jobs, &totals)
	})

	logger.Info().Msg("beginning document sample")
	streamBatches(ctx, logger, jobs, util.SrcToTgt, source, &totals)
	streamBatches(ctx, logger, jobs, util.TgtToSrc, target, &totals)

	close(jobs)
	pool.Done()
	logger.Info().Msg("finished document sample")
	// unnecessary locking, but rather safe than sorry
	totals.lock.Lock()
	if totals.mismatchSrcToTgt > 0 || totals.mismatchTgtToSrc > 0 || totals.missingSrc > 0 || totals.missingTgt > 0 {
		logger.Error().Msgf("sampling result -  %d missing on source | %d missing on target | %d out of %d sampled source documents mismatched | %d out of %d sampled target documents mismatched - failure", totals.missingSrc, totals.missingTgt, totals.mismatchSrcToTgt, totals.sampledSrc, totals.mismatchTgtToSrc, totals.sampledTgt)
	} else {
		logger.Info().Msgf("sampling result -  %d missing on source | %d missing on target | %d out of %d sampled source documents mismatched | %d out of %d sampled target documents mismatched - success", totals.missingSrc, totals.missingTgt, totals.mismatchSrcToTgt, totals.sampledSrc, totals.mismatchTgtToSrc, totals.sampledTgt)
	}
	totals.lock.Unlock()
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

	var srcCursor, tgtCursor *mongo.Cursor
	var err error

	// added retries to avoid $sample error described in HELP-46067
	for {
		srcCursor, err = c.sourceCollection(namespace.Db, namespace.Collection).Aggregate(ctx, pipeline, opts)
		if err == nil {
			break
		}
		logger.Debug().Err(err).Msgf("Error sampling source collection. Retrying...")
		time.Sleep(retryInterval)
	}

	for {
		tgtCursor, err = c.targetCollection(namespace.Db, namespace.Collection).Aggregate(ctx, pipeline, opts)
		if err == nil {
			break
		}
		logger.Debug().Err(err).Msgf("Error sampling target collection. Retrying...")
		time.Sleep(retryInterval)
	}

	return srcCursor, tgtCursor
}

// TODO VARIABLE BATCH SIZE
func streamBatches(ctx context.Context, logger zerolog.Logger, jobs chan documentBatch, dir util.Direction, cursor *mongo.Cursor, totals *collectionTotals) {
	logger = logger.With().Str("dir", string(dir)).Logger()
	docCount := 0
	batchCount := 0
	buffer := make(batch, BATCH_SIZE)
	logger.Debug().Msg("starting cursor walk")
	for cursor.Next(ctx) {
		var doc bson.Raw
		cursor.Decode(&doc)
		buffer.add(doc)
		docCount++

		if docCount%BATCH_SIZE == 0 {
			logger.Trace().Msgf("adding batch %d to be checked", batchCount+1)
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
		logger.Trace().Msgf("adding batch %d to be checked", batchCount+1)
		jobs <- documentBatch{
			dir:   dir,
			batch: buffer,
		}
		batchCount++
	}
}

func (c *Comparer) batchFind(ctx context.Context, logger zerolog.Logger, namespace namespacePair, toFind documentBatch) documentBatch {
	buffer := make(batch, BATCH_SIZE)
	useOr := false
	var coll *mongo.Collection
	switch toFind.dir {
	case util.SrcToTgt:
		coll = c.targetCollection(namespace.Db, namespace.Collection)
	case util.TgtToSrc:
		coll = c.sourceCollection(namespace.Db, namespace.Collection)
	default:
		logger.Fatal().Msg("invalid comparison direction?")
	}

	filters := bson.A{}
	if toFind.dir == util.SrcToTgt && namespace.Partitioned.Target {
		for _, value := range toFind.batch {

			// TODO reduce duplication in this code
			// if collection is sharded, include shard key value in query to target that shard
			filter := bson.D{}

			useOr = true
			elems, err := namespace.PartitionKey.Target.Elements()
			if err != nil {
				log.Error().Err(err)
			}
			for _, each := range elems {
				filter = append(filter, bson.E{each.Key(), value.Lookup(each.Key())})
			}
			filter = append(filter, bson.E{"_id", value.Lookup("_id")})
			filters = append(filters, filter)
		}
	} else if toFind.dir == util.TgtToSrc && namespace.Partitioned.Source {
		for _, value := range toFind.batch {

			// TODO reduce duplication in this code
			// if collection is sharded, include shard key value in query to target that shard
			filter := bson.D{}

			useOr = true
			elems, err := namespace.PartitionKey.Source.Elements()
			if err != nil {
				log.Error().Err(err)
			}
			for _, each := range elems {
				filter = append(filter, bson.E{each.Key(), value.Lookup(each.Key())})
			}
			filter = append(filter, bson.E{"_id", value.Lookup("_id")})
			filters = append(filters, filter)
		}
	} else {
		for _, value := range toFind.batch {
			filters = append(filters, value.Lookup("_id"))
		}
	}

	var query bson.D
	if useOr {
		query = bson.D{{"$or", filters}}
	} else {
		query = bson.D{{"_id", bson.D{{"$in", filters}}}}
	}
	log.Debug().Msgf("sending find: %+v", query)
	cursor, err := coll.Find(ctx, query, nil)

	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.Raw
		cursor.Decode(&doc)

		buffer.add(doc)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	logger.Trace().Msgf("buffer %s", buffer)
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
	logger.Trace().Msgf("comparing outer %s, inner %s", outer, inner)
	for key, aDoc := range outer {
		logger.Trace().Msgf("comparing key %s", key)
		if bDoc, ok := inner[key]; ok {
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

func (c *Comparer) processDocs(ctx context.Context, logger zerolog.Logger, namespace namespacePair, jobs chan documentBatch, totals *collectionTotals) {
	for processing := range jobs {
		dirLogger := logger.With().Str("dir", string(processing.dir)).Logger()
		lookedUp := c.batchFind(ctx, dirLogger, namespace, processing)
		summary := c.batchCompare(ctx, dirLogger, namespace, processing, lookedUp)
		if summary.HasMismatches() {
			c.reporter.SampleSummary(namespace.String(), processing.dir, summary)
			totals.lock.Lock()
			switch processing.dir {
			case util.SrcToTgt:
				totals.mismatchSrcToTgt += int64(summary.Different)
				totals.missingTgt += int64(summary.Missing)
				totals.sampledSrc += int64(summary.Equal) + int64(summary.Missing) + int64(summary.Different)
			case util.TgtToSrc:
				totals.mismatchTgtToSrc += int64(summary.Different)
				totals.missingSrc += int64(summary.Missing)
				totals.sampledTgt += int64(summary.Equal) + int64(summary.Missing) + int64(summary.Different)
			}
			totals.lock.Unlock()
		}
	}
}
