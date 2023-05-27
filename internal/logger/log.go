package logger

import (
	"io"
	"os"
	"sampler/internal/util"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Init(verbosity string, filepath string, startTime time.Time) {
	zerolog.TimeFieldFormat = time.RFC3339
	if filepath != "" {
		runLogFile, err := os.OpenFile(
			util.CleanPath(filepath)+"/sampler-"+startTime.Local().Format(time.RFC3339)+".log",
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0664,
		)
		if err != nil {
			panic(err)
		}
		fileLogger := zerolog.New(runLogFile).With().Logger()
		writers := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout}, fileLogger)
		log.Logger = log.Output(writers)
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	var level zerolog.Level
	switch verbosity {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	default:
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
}
