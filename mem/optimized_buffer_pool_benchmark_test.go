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

package mem_test

import (
	"testing"

	"google.golang.org/grpc/mem"
)

// BenchmarkBufferPoolGetPut compares the performance of different buffer pools
func BenchmarkBufferPoolGetPut(b *testing.B) {
	benchmarks := []struct {
		name string
		pool mem.BufferPool
	}{
		{"DefaultTiered", mem.NewTieredBufferPool(256, 4096, 16384, 32768, 1048576)},
		{"Optimized", mem.NewOptimizedBufferPool()},
		{"Nop", mem.NopBufferPool{}},
	}

	sizes := []int{256, 1024, 4096, 16384, 32768, 65536, 131072, 1048576}

	for _, bm := range benchmarks {
		for _, size := range sizes {
			b.Run(bm.name+"_"+string(rune(size)), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf := bm.pool.Get(size)
					bm.pool.Put(buf)
				}
			})
		}
	}
}

// BenchmarkBufferPoolConcurrent tests concurrent access patterns
func BenchmarkBufferPoolConcurrent(b *testing.B) {
	benchmarks := []struct {
		name string
		pool mem.BufferPool
	}{
		{"DefaultTiered", mem.NewTieredBufferPool(256, 4096, 16384, 32768, 1048576)},
		{"Optimized", mem.NewOptimizedBufferPool()},
	}

	sizes := []int{1024, 16384, 65536}

	for _, bm := range benchmarks {
		for _, size := range sizes {
			b.Run(bm.name+"_Concurrent_"+string(rune(size)), func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						buf := bm.pool.Get(size)
						bm.pool.Put(buf)
					}
				})
			})
		}
	}
}

// BenchmarkBufferPoolMixedWorkload simulates real-world mixed buffer sizes
func BenchmarkBufferPoolMixedWorkload(b *testing.B) {
	benchmarks := []struct {
		name string
		pool mem.BufferPool
	}{
		{"DefaultTiered", mem.NewTieredBufferPool(256, 4096, 16384, 32768, 1048576)},
		{"Optimized", mem.NewOptimizedBufferPool()},
	}

	// Simulate mixed workload with different buffer sizes
	sizes := []int{256, 1024, 4096, 16384, 32768, 65536, 131072, 1048576}
	weights := []int{30, 25, 20, 15, 5, 3, 1, 1} // Frequency weights

	for _, bm := range benchmarks {
		b.Run(bm.name+"_MixedWorkload", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Select size based on weights
				totalWeight := 0
				for _, w := range weights {
					totalWeight += w
				}

				rand := i % totalWeight
				weightSum := 0
				selectedSize := sizes[0]

				for j, weight := range weights {
					weightSum += weight
					if rand < weightSum {
						selectedSize = sizes[j]
						break
					}
				}

				buf := bm.pool.Get(selectedSize)
				bm.pool.Put(buf)
			}
		})
	}
}

// BenchmarkBufferPoolReuse tests buffer reuse efficiency
func BenchmarkBufferPoolReuse(b *testing.B) {
	benchmarks := []struct {
		name string
		pool mem.BufferPool
	}{
		{"DefaultTiered", mem.NewTieredBufferPool(256, 4096, 16384, 32768, 1048576)},
		{"Optimized", mem.NewOptimizedBufferPool()},
	}

	size := 16384 // HTTP/2 frame size

	for _, bm := range benchmarks {
		b.Run(bm.name+"_Reuse", func(b *testing.B) {
			b.ReportAllocs()

			// Pre-allocate some buffers
			buffers := make([]*[]byte, 100)
			for i := 0; i < 100; i++ {
				buffers[i] = bm.pool.Get(size)
			}

			// Return all buffers
			for i := 0; i < 100; i++ {
				bm.pool.Put(buffers[i])
			}

			// Now test reuse
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := bm.pool.Get(size)
				bm.pool.Put(buf)
			}
		})
	}
}

// BenchmarkAdaptiveBufferPool tests the adaptive buffer pool
func BenchmarkAdaptiveBufferPool(b *testing.B) {
	adaptivePool := mem.NewAdaptiveBufferPool()
	defaultPool := mem.NewTieredBufferPool(256, 4096, 16384, 32768, 1048576)

	benchmarks := []struct {
		name string
		pool mem.BufferPool
	}{
		{"Default", defaultPool},
		{"Adaptive", adaptivePool},
	}

	sizes := []int{1024, 16384, 65536}

	for _, bm := range benchmarks {
		for _, size := range sizes {
			b.Run(bm.name+"_Adaptive_"+string(rune(size)), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf := bm.pool.Get(size)
					bm.pool.Put(buf)
				}
			})
		}
	}
}
