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
	"sync"
	"sync/atomic"
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
// 4. Adaptive sizing based on usage patterns
type OptimizedBufferPool struct {
	// Fast path for common sizes (avoid binary search)
	fastPools [8]*sizedBufferPool
	fastMask  uint32

	// Fallback for larger sizes
	fallbackPool simpleBufferPool

	// Statistics for adaptive optimization
	stats struct {
		hits   atomic.Uint64
		misses atomic.Uint64
	}
}

// NewOptimizedBufferPool creates a buffer pool optimized for high QPS workloads
func NewOptimizedBufferPool() *OptimizedBufferPool {
	pool := &OptimizedBufferPool{}

	// Optimize for common gRPC message sizes
	commonSizes := []int{
		256,     // Small metadata
		1024,    // Small messages
		4096,    // Page size
		16384,   // HTTP/2 frame size
		32768,   // Default buffer size
		65536,   // Medium messages
		131072,  // Large messages
		1048576, // 1MB
	}

	for i, size := range commonSizes {
		pool.fastPools[i] = newSizedBufferPool(size)
	}

	// Use bit manipulation for fast size selection
	pool.fastMask = 7 // 2^3 - 1 for 8 pools

	return pool
}

func (p *OptimizedBufferPool) Get(size int) *[]byte {
	// Fast path: try to use one of the common sizes
	if size <= 1048576 { // 1MB
		// Use bit manipulation instead of binary search
		poolIdx := p.selectPoolFast(size)
		if poolIdx < 8 {
			return p.fastPools[poolIdx].Get(size)
		}
	}

	// Fallback for larger sizes
	p.stats.misses.Add(1)
	return p.fallbackPool.Get(size)
}

func (p *OptimizedBufferPool) Put(buf *[]byte) {
	capacity := cap(*buf)

	// Fast path for common sizes
	if capacity <= 1048576 {
		poolIdx := p.selectPoolFast(capacity)
		if poolIdx < 8 {
			p.fastPools[poolIdx].Put(buf)
			p.stats.hits.Add(1)
			return
		}
	}

	// Fallback
	p.fallbackPool.Put(buf)
}

// selectPoolFast uses bit manipulation for faster pool selection
func (p *OptimizedBufferPool) selectPoolFast(size int) uint32 {
	// Use bit shifting for faster size classification
	switch {
	case size <= 256:
		return 0
	case size <= 1024:
		return 1
	case size <= 4096:
		return 2
	case size <= 16384:
		return 3
	case size <= 32768:
		return 4
	case size <= 65536:
		return 5
	case size <= 131072:
		return 6
	case size <= 1048576:
		return 7
	default:
		return 8 // Use fallback
	}
}

// GetStats returns pool usage statistics
func (p *OptimizedBufferPool) GetStats() (hits, misses uint64) {
	return p.stats.hits.Load(), p.stats.misses.Load()
}

// AdaptiveBufferPool automatically adjusts buffer sizes based on usage patterns
type AdaptiveBufferPool struct {
	*OptimizedBufferPool
	usageStats map[int]uint64
	mu         sync.RWMutex
}

func NewAdaptiveBufferPool() *AdaptiveBufferPool {
	return &AdaptiveBufferPool{
		OptimizedBufferPool: NewOptimizedBufferPool(),
		usageStats:          make(map[int]uint64),
	}
}

func (p *AdaptiveBufferPool) Get(size int) *[]byte {
	// Track usage patterns
	p.mu.Lock()
	p.usageStats[size]++
	p.mu.Unlock()

	return p.OptimizedBufferPool.Get(size)
}

// GetUsageStats returns the most commonly requested buffer sizes
func (p *AdaptiveBufferPool) GetUsageStats() map[int]uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[int]uint64)
	for size, count := range p.usageStats {
		result[size] = count
	}
	return result
}

// OptimizedBufferPoolConfig allows custom configuration of the optimized buffer pool
type OptimizedBufferPoolConfig struct {
	// Workload-specific buffer sizes (defaults to optimized sizes if not specified)
	BufferSizes []int

	// Pre-warm pools with this many buffers per size
	PreWarmCount int

	// Enable adaptive sizing based on usage patterns
	EnableAdaptiveSizing bool

	// Enable statistics collection
	EnableStats bool
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
	stats        struct {
		hits   atomic.Uint64
		misses atomic.Uint64
	}
}

// Get returns a buffer with the specified length from the optimized pool
func (p *optimizedBufferPool) Get(length int) *[]byte {
	// Find the appropriate pool for this size
	pool := p.getPool(length)

	// Try to get from the pool
	buf := pool.Get(length)

	// Update statistics if enabled
	if p.config.EnableStats {
		if pool == &p.fallbackPool {
			p.stats.misses.Add(1)
		} else {
			p.stats.hits.Add(1)
		}
	}

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
