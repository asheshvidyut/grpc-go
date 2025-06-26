/*
 * Copyright 2024 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mem

import (
	"sort"
)

// Optimized buffer sizes tuned for common gRPC workloads
var optimizedBufferSizes = []int{
	512,       // Small messages
	4 << 10,   // 4KB (HTTP/2 frame size)
	16 << 10,  // 16KB (max HTTP/2 frame size)
	32 << 10,  // 32KB (common buffer size)
	64 << 10,  // 64KB (large messages)
	128 << 10, // 128KB (very large messages)
	1 << 20,   // 1MB (maximum reasonable size)
}

// OptimizedBufferPool provides better QPS performance through:
// 1. Workload-specific buffer sizes
// 2. Reduced pool selection overhead
// 3. Better cache locality
// 4. Fast path for common sizes
type OptimizedBufferPool struct {
	// Fast path for common sizes (avoid binary search)
	fastPools [8]*sizedBufferPool
	fastMask  uint32

	// Fallback for larger sizes
	fallbackPool simpleBufferPool
}

// NewOptimizedBufferPool creates a buffer pool optimized for high QPS workloads
func NewOptimizedBufferPool() *OptimizedBufferPool {
	pool := &OptimizedBufferPool{}

	// Optimize for common gRPC message sizes using powers of 2 for bit manipulation
	commonSizes := []int{
		1024,   // 2^10 - Small messages
		2048,   // 2^11 - Small-medium messages
		4096,   // 2^12 - Page size
		8192,   // 2^13 - Medium messages
		16384,  // 2^14 - HTTP/2 frame size
		32768,  // 2^15 - Default buffer size
		65536,  // 2^16 - Medium-large messages
		131072, // 2^17 - Large messages
	}

	for i, size := range commonSizes {
		pool.fastPools[i] = newSizedBufferPool(size)
	}

	// Use bit manipulation for fast size selection
	// fastMask = 7 (binary: 111) for 8 pools (indices 0-7)
	pool.fastMask = 7 // 2^3 - 1 for 8 pools

	return pool
}

func (p *OptimizedBufferPool) Get(size int) *[]byte {
	// Fast path: try to use one of the common sizes
	if size <= 131072 { // 2^17
		// Use bit manipulation for faster pool selection
		poolIdx := p.selectPoolFast(size)
		if poolIdx < 8 {
			return p.fastPools[poolIdx].Get(size)
		}
	}

	// Fallback for larger sizes
	return p.fallbackPool.Get(size)
}

func (p *OptimizedBufferPool) Put(buf *[]byte) {
	capacity := cap(*buf)

	// Fast path for common sizes
	if capacity <= 131072 { // 2^17
		poolIdx := p.selectPoolFast(capacity)
		if poolIdx < 8 {
			p.fastPools[poolIdx].Put(buf)
			return
		}
	}

	// Fallback
	p.fallbackPool.Put(buf)
}

// selectPoolFast uses bit manipulation for faster pool selection
func (p *OptimizedBufferPool) selectPoolFast(size int) uint32 {
	// Fast bit manipulation for power-of-2 sizes
	// Shift right by 10 bits and mask to get pool index
	// This is much faster than multiple if-else statements
	return (uint32(size) >> 10) & p.fastMask
}

// OptimizedBufferPoolConfig allows custom configuration of the optimized buffer pool
type OptimizedBufferPoolConfig struct {
	// Workload-specific buffer sizes (defaults to optimized sizes if not specified)
	BufferSizes []int

	// Pre-warm pools with this many buffers per size
	PreWarmCount int
}

// NewOptimizedBufferPoolWithConfig creates an optimized buffer pool with custom configuration
func NewOptimizedBufferPoolWithConfig(config OptimizedBufferPoolConfig) BufferPool {
	sizes := config.BufferSizes
	if len(sizes) == 0 {
		sizes = optimizedBufferSizes
	}

	preWarmCount := config.PreWarmCount
	if preWarmCount == 0 {
		preWarmCount = 100 // Default pre-warm count
	}

	pool := &optimizedBufferPool{
		sizedPools:   make([]*sizedBufferPool, len(sizes)),
		fallbackPool: simpleBufferPool{},
		config:       config,
	}

	// Initialize sized pools
	for i, size := range sizes {
		pool.sizedPools[i] = newSizedBufferPool(size)

		// Pre-warm the pool if enabled
		if preWarmCount > 0 {
			for j := 0; j < preWarmCount; j++ {
				buf := make([]byte, size)
				pool.sizedPools[i].Put(&buf)
			}
		}
	}

	return pool
}

// optimizedBufferPool implements the BufferPool interface with optimizations for QPS performance
type optimizedBufferPool struct {
	sizedPools   []*sizedBufferPool
	fallbackPool simpleBufferPool
	config       OptimizedBufferPoolConfig
}

// Get returns a buffer with the specified length from the optimized pool
func (p *optimizedBufferPool) Get(length int) *[]byte {
	// Find the appropriate pool for this size
	pool := p.getPool(length)

	// Try to get from the pool
	buf := pool.Get(length)

	return buf
}

// Put returns a buffer to the optimized pool
func (p *optimizedBufferPool) Put(buf *[]byte) {
	pool := p.getPool(cap(*buf))
	pool.Put(buf)
}

// getPool returns the appropriate pool for the given size
func (p *optimizedBufferPool) getPool(size int) BufferPool {
	// Use binary search to find the appropriate pool
	poolIdx := sort.Search(len(p.sizedPools), func(i int) bool {
		return p.sizedPools[i].defaultSize >= size
	})

	if poolIdx == len(p.sizedPools) {
		return &p.fallbackPool
	}

	return p.sizedPools[poolIdx]
}
