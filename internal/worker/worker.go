package worker

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
)

type Pool struct {
	num    int
	name   string
	key    string
	logger zerolog.Logger
	wg     sync.WaitGroup
}

func NewWorkerPool(logger zerolog.Logger, numWorkers int, workerName string, keyOverride ...string) Pool {
	key := "w"
	if len(keyOverride) > 0 {
		key = keyOverride[0]
	}
	return Pool{
		num:    numWorkers,
		name:   workerName,
		key:    key,
		logger: logger,
		wg:     sync.WaitGroup{},
	}
}

// Starts worker pool with a given function to be processed by Pool.num workers
// takes in ctx and logger to capture worker num and ctx generated in this function
func (p *Pool) Start(process func(context.Context, zerolog.Logger)) {
	for i := 0; i < p.num; i++ {
		workerNum := i
		workerLogger := p.logger.With().Int(p.key, workerNum).Logger()
		p.wg.Add(1)
		workerLogger.Debug().Msgf("starting %s", p.name)
		go func(logger zerolog.Logger, num int, name string) {
			defer p.wg.Done()
			defer logger.Debug().Msgf("%s finished", name)
			process(context.TODO(), logger)
		}(workerLogger, i, p.name)
	}
}

func (p *Pool) Done() {
	p.wg.Wait()
}
