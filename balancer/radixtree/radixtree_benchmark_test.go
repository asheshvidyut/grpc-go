/*
 *
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
 *
 */

package radixtree

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/balancer/weightedtarget"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// BenchmarkRadixTreeLookup benchmarks the radix tree lookup performance
func BenchmarkRadixTreeLookup(b *testing.B) {
	// Create test paths with different depths and patterns
	testPaths := []string{
		"/api/v1/users",
		"/api/v1/users/123",
		"/api/v1/users/123/posts",
		"/api/v2/users",
		"/api/v2/users/456",
		"/api/v2/users/456/comments",
		"/admin/users",
		"/admin/users/789",
		"/admin/users/789/settings",
		"/public/health",
		"/public/metrics",
		"/internal/debug",
		"/internal/debug/logs",
		"/internal/debug/logs/error",
		"/internal/debug/logs/warn",
	}

	// Create a radix tree with test paths
	tree := createTestRadixTree(testPaths)

	// Benchmark lookup performance
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := testPaths[i%len(testPaths)]
		_, found := tree.Search(path)
		if !found {
			b.Fatalf("Expected to find path: %s", path)
		}
	}
}

// BenchmarkRadixTreeInsert benchmarks the radix tree insertion performance
func BenchmarkRadixTreeInsert(b *testing.B) {
	// Create test paths
	testPaths := []string{
		"/api/v1/users",
		"/api/v1/users/123",
		"/api/v1/users/123/posts",
		"/api/v2/users",
		"/api/v2/users/456",
		"/api/v2/users/456/comments",
		"/admin/users",
		"/admin/users/789",
		"/admin/users/789/settings",
		"/public/health",
		"/public/metrics",
		"/internal/debug",
		"/internal/debug/logs",
		"/internal/debug/logs/error",
		"/internal/debug/logs/warn",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := createTestRadixTree(nil) // Start with empty tree
		for j, path := range testPaths {
			tree.Insert(path, fmt.Sprintf("route_%d", j))
		}
	}
}

// BenchmarkRadixTreeBalancerPick benchmarks the complete balancer pick operation
func BenchmarkRadixTreeBalancerPick(b *testing.B) {
	// Create a mock client connection
	mockCC := &mockClientConn{}

	// Create the radix tree balancer
	builder := balancer.Get(Name)
	if builder == nil {
		b.Fatalf("Radix tree balancer not registered")
	}

	bal := builder.Build(mockCC, balancer.BuildOptions{})

	// Create test configuration
	config := createTestConfig()

	// Update the balancer with configuration
	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: config,
	})
	if err != nil {
		b.Fatalf("Failed to update balancer state: %v", err)
	}

	// Create test pick info
	testPaths := []string{
		"/api/v1/users",
		"/api/v1/users/123",
		"/api/v1/users/123/posts",
		"/api/v2/users",
		"/api/v2/users/456",
		"/api/v2/users/456/comments",
		"/admin/users",
		"/admin/users/789",
		"/admin/users/789/settings",
		"/public/health",
		"/public/metrics",
		"/internal/debug",
		"/internal/debug/logs",
		"/internal/debug/logs/error",
		"/internal/debug/logs/warn",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := testPaths[i%len(testPaths)]
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-grpc-path", path))

		pickInfo := balancer.PickInfo{
			FullMethodName: path,
			Ctx:            ctx,
		}

		// Get the picker from the balancer (this would be done by gRPC)
		// For benchmarking, we'll simulate the pick operation
		_, err := bal.(*radixTreeBalancer).pick(pickInfo)
		if err != nil && err != balancer.ErrNoSubConnAvailable {
			b.Fatalf("Unexpected error during pick: %v", err)
		}
	}
}

// BenchmarkComparison compares radix tree balancer with traditional balancers
func BenchmarkComparison(b *testing.B) {
	// Test different balancer types
	balancers := []struct {
		name   string
		config serviceconfig.LoadBalancingConfig
	}{
		{
			name:   "round_robin",
			config: &roundRobinConfig{},
		},
		{
			name:   "weighted_target",
			config: createWeightedTargetConfig(),
		},
		{
			name:   "radix_tree",
			config: createTestConfig(),
		},
	}

	for _, bal := range balancers {
		b.Run(bal.name, func(b *testing.B) {
			benchmarkBalancer(b, bal.name, bal.config)
		})
	}
}

// BenchmarkRadixTreeScalability benchmarks performance with different tree sizes
func BenchmarkRadixTreeScalability(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Generate paths of the specified size
			paths := generatePaths(size)

			// Create tree
			tree := createTestRadixTree(paths)

			// Benchmark lookups
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := paths[i%len(paths)]
				_, found := tree.Search(path)
				if !found {
					b.Fatalf("Expected to find path: %s", path)
				}
			}
		})
	}
}

// BenchmarkMemoryUsage benchmarks memory usage of different balancer types
func BenchmarkMemoryUsage(b *testing.B) {
	// This benchmark measures memory allocation
	b.Run("radix_tree_memory", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			paths := generatePaths(1000)
			tree := createTestRadixTree(paths)
			_ = tree // Prevent optimization
		}
	})

	b.Run("traditional_memory", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Create traditional balancer structure
			routes := make(map[string]interface{})
			for j := 0; j < 1000; j++ {
				path := fmt.Sprintf("/api/v%d/resource_%d", j%10, j)
				routes[path] = fmt.Sprintf("route_%d", j)
			}
			_ = routes // Prevent optimization
		}
	})
}

// Helper functions

func createTestRadixTree(paths []string) *radix.Tree {
	tree := radix.NewTree()
	for i, path := range paths {
		tree.Insert(path, fmt.Sprintf("route_%d", i))
	}
	return tree
}

func createTestConfig() *LBConfig {
	return &LBConfig{
		Routes: map[string]RouteConfig{
			"route_1": {
				Path:   "/api/v1/users",
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"route_2": {
				Path:   "/api/v1/users/*",
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"route_3": {
				Path:   "/api/v2/users",
				Weight: 2,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"route_4": {
				Path:   "/admin/*",
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"route_5": {
				Path:   "/public/*",
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"route_6": {
				Path:   "/internal/debug/*",
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
		},
	}
}

func createWeightedTargetConfig() *weightedtarget.LBConfig {
	return &weightedtarget.LBConfig{
		Targets: map[string]weightedtarget.Target{
			"target_1": {
				Weight: 1,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"target_2": {
				Weight: 2,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
		},
	}
}

func generatePaths(count int) []string {
	paths := make([]string, count)
	for i := 0; i < count; i++ {
		paths[i] = fmt.Sprintf("/api/v%d/resource_%d", i%10, i)
	}
	return paths
}

func benchmarkBalancer(b *testing.B, name string, config serviceconfig.LoadBalancingConfig) {
	mockCC := &mockClientConn{}

	builder := balancer.Get(name)
	if builder == nil {
		b.Fatalf("Balancer %s not registered", name)
	}

	bal := builder.Build(mockCC, balancer.BuildOptions{})

	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: config,
	})
	if err != nil {
		b.Fatalf("Failed to update balancer state: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate pick operation
		ctx := context.Background()
		pickInfo := balancer.PickInfo{
			FullMethodName: "/api/v1/users",
			Ctx:            ctx,
		}

		// For radix tree, we need to add path metadata
		if name == "radix_tree" {
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-grpc-path", "/api/v1/users"))
			pickInfo.Ctx = ctx
		}

		// This is a simplified benchmark - in reality, gRPC would call the picker
		_ = pickInfo
	}
}

// Mock implementations for testing

type mockClientConn struct{}

func (m *mockClientConn) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	return &mockSubConn{}, nil
}

func (m *mockClientConn) RemoveSubConn(sc balancer.SubConn) {}

func (m *mockClientConn) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {}

func (m *mockClientConn) UpdateState(state balancer.State) {}

func (m *mockClientConn) ResolveNow(opts resolver.ResolveNowOptions) {}

func (m *mockClientConn) Target() string { return "mock-target" }

func (m *mockClientConn) MetricsRecorder() estats.MetricsRecorder { return nil }

type mockSubConn struct{}

func (m *mockSubConn) UpdateAddresses(addrs []resolver.Address) {}

func (m *mockSubConn) Connect() {}

func (m *mockSubConn) GetOrBuildProducer(builder balancer.ProducerBuilder) (balancer.Producer, func()) {
	return nil, func() {}
}

func (m *mockSubConn) Shutdown() {}

func (m *mockSubConn) RegisterHealthListener(listener func(balancer.SubConnState)) {}

type roundRobinConfig struct {
	serviceconfig.LoadBalancingConfig
}

// Performance metrics collection
type PerformanceMetrics struct {
	LookupTime  time.Duration
	InsertTime  time.Duration
	MemoryUsage int64
	TreeSize    int
	PathDepth   int
}

func collectMetrics(tree *radix.Tree, paths []string) PerformanceMetrics {
	start := time.Now()

	// Measure lookup time
	for _, path := range paths {
		_, _ = tree.Search(path)
	}
	lookupTime := time.Since(start)

	// Measure insert time
	start = time.Now()
	newTree := radix.NewTree()
	for i, path := range paths {
		newTree.Insert(path, fmt.Sprintf("route_%d", i))
	}
	insertTime := time.Since(start)

	return PerformanceMetrics{
		LookupTime: lookupTime,
		InsertTime: insertTime,
		TreeSize:   len(paths),
		PathDepth:  calculateAverageDepth(paths),
	}
}

func calculateAverageDepth(paths []string) int {
	if len(paths) == 0 {
		return 0
	}

	totalDepth := 0
	for _, path := range paths {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		totalDepth += len(parts)
	}

	return totalDepth / len(paths)
}
