package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	// Заполнить здесь ассерт, что b содержит zero и что b содержит one
	assert.True(t, slices.Contains(b[:], zero), "expected zero in b")
	assert.True(t, slices.Contains(b[:], one), "expected one in b")
}
