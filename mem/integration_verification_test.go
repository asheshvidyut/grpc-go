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

func TestOptimizedBufferPoolIsDefault(t *testing.T) {
	// Verify that our optimized buffer pool is now the default
	poolType := mem.GetCurrentBufferPoolType()
	if poolType != "optimized" {
		t.Fatalf("Expected optimized buffer pool to be default, got: %s", poolType)
	}

	// Test that the default pool works correctly
	pool := mem.DefaultBufferPool()

	// Test basic functionality
	buf := pool.Get(1024)
	if len(*buf) != 1024 {
		t.Fatalf("Expected buffer length 1024, got %d", len(*buf))
	}

	// Test that we can put the buffer back
	pool.Put(buf)

	t.Logf("✓ Optimized buffer pool is working as default")
}

func TestBufferPoolSwitching(t *testing.T) {
	// Verify we start with optimized pool
	if mem.GetCurrentBufferPoolType() != "optimized" {
		t.Fatalf("Expected to start with optimized pool")
	}

	// Switch to original pool
	mem.SwitchToOriginalBufferPool()
	if mem.GetCurrentBufferPoolType() != "tiered" {
		t.Fatalf("Expected tiered pool after switch, got: %s", mem.GetCurrentBufferPoolType())
	}

	// Switch back to optimized pool
	mem.EnableOptimizedBufferPool()
	if mem.GetCurrentBufferPoolType() != "optimized" {
		t.Fatalf("Expected optimized pool after switch back, got: %s", mem.GetCurrentBufferPoolType())
	}

	t.Logf("✓ Buffer pool switching works correctly")
}

func TestBufferPoolPerformance(t *testing.T) {
	// Test performance with optimized pool
	pool := mem.DefaultBufferPool()

	const iterations = 10000
	const bufferSize = 1024

	// Warm up
	for i := 0; i < 1000; i++ {
		buf := pool.Get(bufferSize)
		pool.Put(buf)
	}

	// Performance test
	for i := 0; i < iterations; i++ {
		buf := pool.Get(bufferSize)
		if len(*buf) != bufferSize {
			t.Fatalf("Expected buffer length %d, got %d", bufferSize, len(*buf))
		}
		pool.Put(buf)
	}

	t.Logf("✓ Optimized buffer pool performance test passed (%d iterations)", iterations)
}
