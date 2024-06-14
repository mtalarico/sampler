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

var sampler comparer.Comparer

func connectMongo(config cfg.MongoOptions) *mongo.Client {
	opts := config.MakeClientOptions()
	source, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to source cluster")
	}
	return source
}

func init() {
	startTime := time.Now()
	config := cfg.Init()
	logger.Init(config.Verbosity, config.LogFile, startTime)

	log.Debug().Msgf("%#v", config)

	source := connectMongo(config.Source)
	target := connectMongo(config.Target)
	var meta *mongo.Client
	if config.Meta.URI != "" {
		meta = connectMongo(config.Meta)
	} else {
		meta = target
	}

	sampler = comparer.NewComparer(config, source, target, meta, startTime)
}

func main() {
	ctx := context.Background()
	sampler.Compare(ctx)
}
