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
	"testing"
	"time"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/balancer/weightedtarget"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

const defaultTestTimeout = 5 * time.Second

// TestRadixTreeIntegration demonstrates how to use the radix tree load balancer
func (s) TestRadixTreeIntegration(t *testing.T) {
	// Create a test client connection with radix tree balancer
	config := &LBConfig{
		Routes: map[string]RouteConfig{
			"api-v1": {
				Path:   "/api/v1/*",
				Weight: 70,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
				},
			},
			"api-v2": {
				Path:   "/api/v2/*",
				Weight: 30,
				Addresses: []resolver.Address{
					{Addr: "backend3:8080"},
				},
			},
			"admin": {
				Path:   "/admin/*",
				Weight: 10,
				Addresses: []resolver.Address{
					{Addr: "admin-backend:8080"},
				},
			},
		},
	}

	// Create balancer
	cc := testutils.NewBalancerClientConn(t)
	bal := &radixTreeBalancer{
		cc:       cc,
		tree:     radix.NewTree(),
		routes:   make(map[string]*routeInfo),
		subConns: make(map[balancer.SubConn]*subConnInfo),
	}

	// Update with configuration
	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: config,
		ResolverState: resolver.State{
			Addresses: []resolver.Address{
				{Addr: "backend1:8080"},
				{Addr: "backend2:8080"},
				{Addr: "backend3:8080"},
				{Addr: "admin-backend:8080"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Verify routes were created
	if len(bal.routes) != 3 {
		t.Errorf("Expected 3 routes, got %d", len(bal.routes))
	}

	// Verify sub-connections were created
	if len(bal.subConns) != 4 {
		t.Errorf("Expected 4 sub-connections, got %d", len(bal.subConns))
	}

	// Test path matching
	testCases := []struct {
		name     string
		path     string
		expected string
	}{
		{"api-v1 path", "/api/v1/users", "api-v1"},
		{"api-v2 path", "/api/v2/users", "api-v2"},
		{"admin path", "/admin/dashboard", "admin"},
		{"nested api-v1 path", "/api/v1/users/123/posts", "api-v1"},
		{"nested api-v2 path", "/api/v2/users/456/comments", "api-v2"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			routeID, found := bal.tree.Search(tc.path)
			if !found {
				t.Errorf("Path %s not found in radix tree", tc.path)
				return
			}
			if routeID != tc.expected {
				t.Errorf("Expected route %s for path %s, got %s", tc.expected, tc.path, routeID)
			}
		})
	}
}

// TestEnhancedRadixTreeIntegration demonstrates the enhanced features
func (s) TestEnhancedRadixTreeIntegration(t *testing.T) {
	// Create enhanced configuration with multi-route support
	config := &EnhancedLBConfig{
		Routes: map[string]EnhancedRouteConfig{
			"api-v1": {
				Path:     "/api/v1/*",
				Weight:   70,
				Priority: 1,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
				},
				Metadata: map[string]string{
					"version": "v1",
					"env":     "production",
				},
			},
			"api-v2": {
				Path:     "/api/v2/*",
				Weight:   30,
				Priority: 2,
				Addresses: []resolver.Address{
					{Addr: "backend3:8080"},
				},
				Metadata: map[string]string{
					"version": "v2",
					"env":     "staging",
				},
			},
		},
		EnableMultiRoute:       true,
		RouteSelectionStrategy: "priority",
	}

	// Create enhanced balancer
	cc := testutils.NewBalancerClientConn(t)
	bal := &enhancedRadixTreeBalancer{
		cc:       cc,
		tree:     radix.NewTree(),
		routes:   make(map[string]*enhancedRouteInfo),
		subConns: make(map[balancer.SubConn]*subConnInfo),
	}

	// Update with configuration
	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: config,
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Test multi-route matching
	matches := bal.findRoutesForPath("/api/v1/users/123")
	if len(matches) != 1 {
		t.Errorf("Expected 1 match for /api/v1/users/123, got %d", len(matches))
	}

	// Test route listing by prefix
	apiRoutes := bal.listRoutesByPrefix("api-")
	if len(apiRoutes) != 2 {
		t.Errorf("Expected 2 API routes, got %d", len(apiRoutes))
	}
}

// TestBalancerComparison compares different balancer types
func (s) TestBalancerComparison(t *testing.T) {
	// Test configurations for different balancer types
	testCases := []struct {
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
			config: createRadixTreeConfig(),
		},
		{
			name:   "enhanced_radix_tree",
			config: createEnhancedRadixTreeConfig(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create balancer
			cc := testutils.NewBalancerClientConn(t)
			builder := balancer.Get(tc.name)
			if builder == nil {
				t.Skipf("Balancer %s not available", tc.name)
			}

			bal := builder.Build(cc, balancer.BuildOptions{})

			// Update with configuration
			err := bal.UpdateClientConnState(balancer.ClientConnState{
				BalancerConfig: tc.config,
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{Addr: "backend1:8080"},
						{Addr: "backend2:8080"},
						{Addr: "backend3:8080"},
					},
				},
			})
			if err != nil {
				t.Errorf("UpdateClientConnState failed for %s: %v", tc.name, err)
			}

			// Verify balancer was created successfully
			if bal == nil {
				t.Errorf("Balancer %s was not created", tc.name)
			}
		})
	}
}

// Helper functions to create test configurations

type roundRobinConfig struct {
	serviceconfig.LoadBalancingConfig
}

func createWeightedTargetConfig() *weightedtarget.LBConfig {
	return &weightedtarget.LBConfig{
		Targets: map[string]weightedtarget.Target{
			"target_1": {
				Weight: 70,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
			"target_2": {
				Weight: 30,
				ChildPolicy: &serviceconfig.LoadBalancingConfig{
					Name: roundrobin.Name,
				},
			},
		},
	}
}

func createRadixTreeConfig() *LBConfig {
	return &LBConfig{
		Routes: map[string]RouteConfig{
			"api-v1": {
				Path:   "/api/v1/*",
				Weight: 70,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
				},
			},
			"api-v2": {
				Path:   "/api/v2/*",
				Weight: 30,
				Addresses: []resolver.Address{
					{Addr: "backend3:8080"},
				},
			},
		},
	}
}

func createEnhancedRadixTreeConfig() *EnhancedLBConfig {
	return &EnhancedLBConfig{
		Routes: map[string]EnhancedRouteConfig{
			"api-v1": {
				Path:     "/api/v1/*",
				Weight:   70,
				Priority: 1,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
				},
			},
			"api-v2": {
				Path:     "/api/v2/*",
				Weight:   30,
				Priority: 2,
				Addresses: []resolver.Address{
					{Addr: "backend3:8080"},
				},
			},
		},
		EnableMultiRoute:       true,
		RouteSelectionStrategy: "priority",
	}
}

// TestRealWorldScenario simulates a real-world load balancing scenario
func (s) TestRealWorldScenario(t *testing.T) {
	// Create a realistic configuration with multiple services
	config := &EnhancedLBConfig{
		Routes: map[string]EnhancedRouteConfig{
			"user-service": {
				Path:     "/api/users/*",
				Weight:   40,
				Priority: 1,
				Addresses: []resolver.Address{
					{Addr: "user-service-1:8080"},
					{Addr: "user-service-2:8080"},
					{Addr: "user-service-3:8080"},
				},
				Metadata: map[string]string{
					"service": "user",
					"version": "v1",
				},
			},
			"product-service": {
				Path:     "/api/products/*",
				Weight:   30,
				Priority: 1,
				Addresses: []resolver.Address{
					{Addr: "product-service-1:8080"},
					{Addr: "product-service-2:8080"},
				},
				Metadata: map[string]string{
					"service": "product",
					"version": "v1",
				},
			},
			"order-service": {
				Path:     "/api/orders/*",
				Weight:   20,
				Priority: 1,
				Addresses: []resolver.Address{
					{Addr: "order-service-1:8080"},
				},
				Metadata: map[string]string{
					"service": "order",
					"version": "v1",
				},
			},
			"admin-service": {
				Path:     "/admin/*",
				Weight:   10,
				Priority: 2, // Higher priority for admin
				Addresses: []resolver.Address{
					{Addr: "admin-service:8080"},
				},
				Metadata: map[string]string{
					"service": "admin",
					"version": "v1",
				},
			},
		},
		EnableMultiRoute:       true,
		RouteSelectionStrategy: "priority",
	}

	// Create enhanced balancer
	cc := testutils.NewBalancerClientConn(t)
	bal := &enhancedRadixTreeBalancer{
		cc:       cc,
		tree:     radix.NewTree(),
		routes:   make(map[string]*enhancedRouteInfo),
		subConns: make(map[balancer.SubConn]*subConnInfo),
	}

	// Update with configuration
	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: config,
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Test various request paths
	testPaths := []struct {
		path     string
		expected string
	}{
		{"/api/users/123", "user-service"},
		{"/api/users/123/profile", "user-service"},
		{"/api/products/456", "product-service"},
		{"/api/products/456/reviews", "product-service"},
		{"/api/orders/789", "order-service"},
		{"/admin/dashboard", "admin-service"},
		{"/admin/users", "admin-service"},
	}

	for _, tc := range testPaths {
		t.Run(tc.path, func(t *testing.T) {
			matches := bal.findRoutesForPath(tc.path)
			if len(matches) == 0 {
				t.Errorf("No routes found for path %s", tc.path)
				return
			}

			// Check if the expected route is in the matches
			found := false
			for _, match := range matches {
				if match == tc.expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected route %s not found in matches for path %s", tc.expected, tc.path)
			}
		})
	}

	// Test route management features
	apiRoutes := bal.listRoutesByPrefix("api-")
	if len(apiRoutes) != 3 {
		t.Errorf("Expected 3 API routes, got %d", len(apiRoutes))
	}

	adminRoutes := bal.listRoutesByPrefix("admin-")
	if len(adminRoutes) != 1 {
		t.Errorf("Expected 1 admin route, got %d", len(adminRoutes))
	}
}
