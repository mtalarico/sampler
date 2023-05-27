package util

// Copyright (C) MongoDB, Inc. 2020-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	a := 0
	b := 1
	b_greater := Max(a, b)
	assert.Equal(t, 1, b_greater)

	c := 0
	d := 0
	equal := Max(c, d)
	assert.Equal(t, 0, equal)
}

func TestMin(t *testing.T) {
	a := 0
	b := 1
	a_lesser := Min(a, b)
	assert.Equal(t, 0, a_lesser)

	c := 0
	d := 0
	equal := Min(c, d)
	assert.Equal(t, 0, equal)
}
