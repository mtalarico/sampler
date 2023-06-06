package comparer

import (
	"context"
	"math"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"sampler/internal/doc"
	"sampler/internal/ns"
	"sampler/internal/util"
	"sampler/internal/worker"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type documentBatch struct {
	dir   direction
	batch batch
}

type batch map[interface{}]bson.Raw

type direction string

func (d direction) String() string {
	return string(d)
}

const (
	SrcToTgt direction = "src -> tgt"
	TgtToSrc direction = "tgt -> src"
)

type DocSummary struct {
	MissingSrc int
	MissingTgt int
	Mismatch   int
	Equal      int
}

func (d *DocSummary) add(more DocSummary) {
	d.MissingSrc += more.MissingSrc
	d.MissingTgt += more.MissingTgt
	d.Mismatch += more.Mismatch
	d.Equal += more.Equal
}

func (b batch) add(doc bson.Raw) {
	id := doc.Lookup("_id").String()
	b[id] = doc
}

func (c *Comparer) CompareSampleDocs(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) {
	logger = logger.With().Str("c", "sampleDoc").Logger()
	source, target := c.sampleCursors(ctx, logger, namespace)
	defer source.Close(ctx)
	defer target.Close(ctx)
	// TODO variable batch size based on doc size ( 256MB)
	jobs := make(chan documentBatch, 100)

	pool := worker.NewWorkerPool(logger, NUM_WORKERS, "sampleDocWorkers", "sw")
	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		c.processDoc(iCtx, iLogger, namespace, jobs)
	})

	logger.Info().Msg("beginning document sample")
	streamBatches(ctx, logger, jobs, SrcToTgt, source)
	streamBatches(ctx, logger, jobs, TgtToSrc, target)

	close(jobs)
	pool.Done()
	logger.Info().Msg("finished document sample")
}

func (c *Comparer) GetSampleSize(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) int64 {
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

func (c *Comparer) sampleCursors(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace) (*mongo.Cursor, *mongo.Cursor) {
	sampleSize := c.GetSampleSize(ctx, logger, namespace)
	logger.Info().Msgf("using sample size of %d", sampleSize)

	pipeline := bson.A{bson.D{{"$sample", bson.D{{"size", sampleSize}}}}, bson.D{{"$sort", bson.D{{"_id", 1}}}}}
	opts := options.Aggregate().SetAllowDiskUse(true).SetBatchSize(int32(BATCH_SIZE))
	logger.Debug().Any("pipeline", pipeline).Any("options", opts).Msg("aggregating")

	srcCursor, err := c.sourceCollection(namespace).Aggregate(context.TODO(), pipeline, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	tgtCursor, err := c.targetCollection(namespace).Aggregate(context.TODO(), pipeline, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return srcCursor, tgtCursor
}

// TODO VARIABLE BATCH SIZE
func streamBatches(ctx context.Context, logger zerolog.Logger, jobs chan documentBatch, dir direction, cursor *mongo.Cursor) {
	logger = logger.With().Str("dir", dir.String()).Logger()
	docCount := 0
	batchCount := 0
	buffer := make(batch, BATCH_SIZE)
	logger.Debug().Msg("starting cursor walk")
	for cursor.Next(context.TODO()) {
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
	// if we counted more than one doc but the counter did not land on a clean batch size there should still be items in the map to be checked
	if len(buffer) != 0 {
		logger.Debug().Msgf("adding batch %d to be checked", batchCount+1)
		jobs <- documentBatch{
			dir:   dir,
			batch: buffer,
		}
		batchCount++
	}
}

func (c *Comparer) batchFind(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace, toFind documentBatch) documentBatch {
	buffer := make(batch, BATCH_SIZE)
	var keys []bson.RawValue

	for _, value := range toFind.batch {
		keys = append(keys, value.Lookup("_id"))
	}
	filter := bson.M{"_id": bson.M{"$in": keys}}

	log.Trace().Msgf("filter: %s", filter)

	var cursor *mongo.Cursor
	var err error
	opts := options.Find().SetSort(bson.M{"_id": 1})

	switch toFind.dir {
	case SrcToTgt:
		cursor, err = c.targetCollection(namespace).Find(context.TODO(), filter, opts)
	case TgtToSrc:
		cursor, err = c.sourceCollection(namespace).Find(context.TODO(), filter, opts)
	default:
		logger.Fatal().Msg("invalid comparison direction")
	}

	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		var doc bson.Raw
		cursor.Decode(&doc)
		buffer.add(doc)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	return documentBatch{
		dir:   toFind.dir,
		batch: buffer,
	}
}

func (c *Comparer) batchCompare(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace, a documentBatch, b documentBatch) DocSummary {
	var summary DocSummary
	for key, aDoc := range a.batch {
		if bDoc, ok := b.batch[key]; ok {
			logger.Trace().Msgf("comparing %v to %v", aDoc, bDoc)
			comparison, err := doc.BsonUnorderedCompareRawDocumentWithDetails(aDoc, bDoc)
			if err != nil {
				log.Error().Err(err).Msg("")
			}
			if comparison == nil {
				summary.Equal++
				continue
			}
			logger.Error().Msg("docs are not the same.")
			if len(comparison.MissingFieldOnDst) > 0 {
				logger.Error().Msgf("%s is missing fields on the target", key)
			}
			if len(comparison.MissingFieldOnSrc) > 0 {
				logger.Error().Msgf("%s is missing fields on the source", key)
			}
			if len(comparison.FieldContentsDiffer) > 0 {
				logger.Error().Msgf("%s is different between the source and target", key)
			}
			summary.Mismatch++
		} else {
			logger.Error().Msgf("_id %v not found", key)
			switch a.dir {
			case SrcToTgt:
				summary.MissingTgt++
			case TgtToSrc:
				summary.MissingSrc++
			}
		}
	}
	return summary
}

func (c *Comparer) processDoc(ctx context.Context, logger zerolog.Logger, namespace ns.Namespace, jobs chan documentBatch) {
	var summary DocSummary
	for processing := range jobs {
		logger = logger.With().Str("dir", processing.dir.String()).Logger()
		lookedUp := c.batchFind(ctx, logger, namespace, processing)
		summary.add(c.batchCompare(ctx, logger, namespace, processing, lookedUp))
	}
}
