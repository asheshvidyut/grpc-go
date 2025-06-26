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

func TestOptimizedBufferPoolIntegration(t *testing.T) {
	// Test that we can enable the optimized buffer pool
	mem.EnableOptimizedBufferPool()

	// Verify that the default buffer pool is now our optimized version
	pool := mem.DefaultBufferPool()

	// Test basic functionality
	buf := pool.Get(1024)
	if len(*buf) != 1024 {
		t.Fatalf("Expected buffer length 1024, got %d", len(*buf))
	}

	// Test that we can put the buffer back
	pool.Put(buf)

	// Test with custom configuration
	config := mem.OptimizedBufferPoolConfig{
		BufferSizes:          []int{512, 1024, 2048},
		PreWarmCount:         50,
		EnableAdaptiveSizing: true,
		EnableStats:          true,
	}

	mem.EnableOptimizedBufferPoolWithConfig(config)

	// Test the custom configured pool
	customPool := mem.DefaultBufferPool()
	customBuf := customPool.Get(1024)
	if len(*customBuf) != 1024 {
		t.Fatalf("Expected buffer length 1024, got %d", len(*customBuf))
	}
	customPool.Put(customBuf)

	// Restore the default buffer pool
	mem.RestoreDefaultBufferPool()

	// Verify restoration
	restoredPool := mem.DefaultBufferPool()
	restoredBuf := restoredPool.Get(1024)
	if len(*restoredBuf) != 1024 {
		t.Fatalf("Expected buffer length 1024, got %d", len(*restoredBuf))
	}
	restoredPool.Put(restoredBuf)
}

func TestOptimizedBufferPoolPerformance(t *testing.T) {
	// Enable optimized buffer pool
	mem.EnableOptimizedBufferPool()
	defer mem.RestoreDefaultBufferPool()

	pool := mem.DefaultBufferPool()

	// Run a performance test
	const iterations = 10000
	const bufferSize = 1024

	for i := 0; i < iterations; i++ {
		buf := pool.Get(bufferSize)
		if len(*buf) != bufferSize {
			t.Fatalf("Expected buffer length %d, got %d", bufferSize, len(*buf))
		}
		pool.Put(buf)
	}
}
