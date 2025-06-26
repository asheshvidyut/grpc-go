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
	"google.golang.org/grpc/internal"
)

// EnableOptimizedBufferPool replaces the default buffer pool with our optimized version
// that provides better QPS performance through workload-specific tuning and reduced overhead.
// Note: This is now the default behavior, so this function is mainly for explicit control.
func EnableOptimizedBufferPool() {
	// Create our optimized buffer pool with workload-specific tuning
	optimizedPool := NewOptimizedBufferPool()

	// Replace the default buffer pool
	internal.SetDefaultBufferPoolForTesting.(func(BufferPool))(optimizedPool)
}

// EnableOptimizedBufferPoolWithConfig allows custom configuration of the optimized buffer pool
func EnableOptimizedBufferPoolWithConfig(config OptimizedBufferPoolConfig) {
	// Create our optimized buffer pool with custom configuration
	optimizedPool := NewOptimizedBufferPoolWithConfig(config)

	// Replace the default buffer pool
	internal.SetDefaultBufferPoolForTesting.(func(BufferPool))(optimizedPool)
}

// RestoreDefaultBufferPool restores the original default buffer pool
func RestoreDefaultBufferPool() {
	originalPool := NewTieredBufferPool(defaultBufferPoolSizes...)
	internal.SetDefaultBufferPoolForTesting.(func(BufferPool))(originalPool)
}

// SwitchToOriginalBufferPool switches to the original tiered buffer pool implementation
func SwitchToOriginalBufferPool() {
	UseOriginalBufferPool()
}

// GetCurrentBufferPoolType returns a string indicating which type of buffer pool is currently active
func GetCurrentBufferPoolType() string {
	pool := DefaultBufferPool()

	// Check if it's our optimized pool (OptimizedBufferPool)
	if _, ok := pool.(*OptimizedBufferPool); ok {
		return "optimized"
	}

	// Check if it's our optimized pool (optimizedBufferPool)
	if optPool, ok := pool.(*optimizedBufferPool); ok {
		_ = optPool.config // Access config to verify it's our optimized pool
		return "optimized"
	}

	// Check if it's the original tiered pool
	if _, ok := pool.(*tieredBufferPool); ok {
		return "tiered"
	}

	// Check if it's the nop pool
	if _, ok := pool.(NopBufferPool); ok {
		return "nop"
	}

	// Check if it's the simple pool
	if _, ok := pool.(*simpleBufferPool); ok {
		return "simple"
	}

	return "unknown"
}
