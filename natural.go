// Copyright (c) 2019, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltutils

import (
	"encoding/binary"
	"strconv"
	"unicode"
)

// Natural returns a string suitable for natural sort ordering.
// All numbers are encoded in big endian binary representation.
func Natural(in string) (out string) {
	for {
		start, end, number := getFirstNumber(in)
		if start < 0 {
			out += in
			break
		}
		out += in[:start]
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, number)
		out += string(b)
		in = in[end:]
		if in == "" {
			break
		}
	}
	return out
}

func getFirstNumber(in string) (start, end int, number uint64) {
	start = -1
	end = -1
	for i, r := range in {
		if unicode.IsDigit(r) {
			if start < 0 {
				start = i
			}
		} else {
			if start >= 0 && end < 0 {
				end = i
				break
			}
		}
	}
	if start >= 0 {
		if end < 0 {
			end = len(in)
		}
	} else {
		return -1, -1, 0
	}
	number, err := strconv.ParseUint(in[start:end], 10, 64)
	if err != nil {
		return -1, -1, 0
	}
	return start, end, number
}
