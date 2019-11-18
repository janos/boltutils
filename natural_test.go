// Copyright (c) 2019, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltutils_test

import (
	"math"
	"strconv"
	"testing"

	"resenje.org/boltutils"
)

func TestNatural(t *testing.T) {
	for _, tc := range []struct {
		in, out string
	}{
		{
			in:  "",
			out: "",
		},
		{
			in:  "a",
			out: "a",
		},
		{
			in:  "1",
			out: "\x00\x00\x00\x00\x00\x00\x00\x01",
		},
		{
			in:  "a1",
			out: "a\x00\x00\x00\x00\x00\x00\x00\x01",
		},
		{
			in:  "1a",
			out: "\x00\x00\x00\x00\x00\x00\x00\x01a",
		},
		{
			in:  "1a" + strconv.FormatUint(math.MaxUint64, 10) + "-",
			out: "\x00\x00\x00\x00\x00\x00\x00\x01a\xff\xff\xff\xff\xff\xff\xff\xff-",
		},
		{
			in:  "Go 1.11rc1 rocks",
			out: "Go \x00\x00\x00\x00\x00\x00\x00\x01.\x00\x00\x00\x00\x00\x00\x00\vrc\x00\x00\x00\x00\x00\x00\x00\x01 rocks",
		},
		{
			in:  strconv.FormatUint(math.MaxUint64, 10) + "1",
			out: "184467440737095516151",
		},
		{
			in:  strconv.FormatUint(math.MaxUint32, 10),
			out: "\x00\x00\x00\x00\xff\xff\xff\xff",
		},
		{
			in:  "340282366920938463463374607431768211455",
			out: "340282366920938463463374607431768211455",
		},
	} {
		out := boltutils.Natural(tc.in)
		if out != tc.out {
			t.Errorf("%q: got %q, expected %q", tc.in, out, tc.out)
		}
	}
}
