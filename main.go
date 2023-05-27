package main

// Copyright (C) MongoDB, Inc. 2020-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

import (
	"context"
	"sampler/internal/cfg"
	"sampler/internal/comparer"
	"sampler/internal/logger"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/rs/zerolog/log"
)

var compare comparer.Comparer

func connectMongo(config cfg.Configuration) (*mongo.Client, *mongo.Client, *mongo.Client) {
	sourceConfig := config.Source.MakeClientOptions()
	source, err := mongo.Connect(context.TODO(), sourceConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	targetConfig := config.Target.MakeClientOptions()
	target, err := mongo.Connect(context.TODO(), targetConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	metaConfig := config.Meta.MakeClientOptions()
	meta, err := mongo.Connect(context.TODO(), metaConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	return source, target, meta
}

func init() {
	startTime := time.Now()
	config := cfg.Init()
	logger.Init(config.Verbosity, config.LogFile, startTime)

	log.Debug().Msgf("%#v", config)

	source, target, meta := connectMongo(config)
	compare = comparer.NewComparer(config, source, target, meta, startTime)
}

func main() {
	if compare.IsDryRun() {
		compare.ReportCounts()
	} else {
		compare.CompareUserNamespaces()
	}
}
