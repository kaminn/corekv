// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "math"

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	if len(f) < 2 {
		return false
	}

	k := f[len(f)-1]

	nBits := 8 * (len(f) - 1)
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % uint32(nBits)
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	// m = -numEntries * ln(fp) / (ln2)^2
	size := (-1 * float64(numEntries) * math.Log(fp)) / math.Pow(math.Log(2), 2)
	l := math.Ceil(size / float64(numEntries))
	return int(l)
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 1
	}

	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}

	nBits := len(keys) * bitsPerKey
	if nBits < 64 {
		nBits = 64
	}

	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	filter := make([]byte, nBytes+1)
	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	// Store k in final pos
	filter[nBytes] = uint8(k)

	return filter
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	// murmurhash1
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
