// Copyright (c) 2018, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltutils

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	for _, tc := range []struct {
		time  time.Time
		bytes []byte
	}{
		{
			time:  time.Time{},
			bytes: []byte{128, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{128, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(-math.MaxInt16, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{135, 208, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2000, 1, 1, 0, 0, 0, 1, time.UTC),
			bytes: []byte{135, 208, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			time:  time.Unix(0, 0),
			bytes: []byte{135, 178, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2018, 6, 28, 8, 15, 0, 659847297, time.UTC),
			bytes: []byte{135, 226, 54, 190, 80, 66, 53, 160, 129},
		},
		{
			time:  time.Date(2018, 6, 28, 8, 15, 0, 659847298, time.UTC),
			bytes: []byte{135, 226, 54, 190, 80, 66, 53, 160, 130},
		},
		{
			time:  time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC),
			bytes: []byte{136, 214, 30, 255, 235, 165, 42, 255, 255},
		},
		{
			time:  time.Date(math.MaxInt16, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{255, 255, 0, 0, 0, 0, 0, 0, 0},
		},
	} {
		bt := TimeToBytesUTC(tc.time)
		if !bytes.Equal(tc.bytes, bt) {
			t.Errorf("time %s: want %v, got %v", tc.time, tc.bytes, bt)
		}

		tm := BytesToTimeUTC(tc.bytes)
		if !tm.Equal(tc.time) {
			t.Errorf("bytes %v: want %s, got %s", tc.bytes, tc.time, tm)
		}
	}
}
