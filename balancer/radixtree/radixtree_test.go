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
	"encoding/json"
	"testing"
	"time"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/metadata"
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

func (s) TestRadixTreeBalancer_BasicRouting(t *testing.T) {
	// Create test configuration
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
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Verify routes were created
	if len(bal.routes) != 2 {
		t.Errorf("Expected 2 routes, got %d", len(bal.routes))
	}

	// Verify sub-connections were created
	if len(bal.subConns) != 3 {
		t.Errorf("Expected 3 sub-connections, got %d", len(bal.subConns))
	}
}

func (s) TestRadixTreeBalancer_PathMatching(t *testing.T) {
	// Create test configuration with various path patterns
	config := &LBConfig{
		Routes: map[string]RouteConfig{
			"users": {
				Path:   "/users/*",
				Weight: 50,
				Addresses: []resolver.Address{
					{Addr: "user-service:8080"},
				},
			},
			"products": {
				Path:   "/products/*",
				Weight: 30,
				Addresses: []resolver.Address{
					{Addr: "product-service:8080"},
				},
			},
			"orders": {
				Path:   "/orders/*",
				Weight: 20,
				Addresses: []resolver.Address{
					{Addr: "order-service:8080"},
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
				{Addr: "user-service:8080"},
				{Addr: "product-service:8080"},
				{Addr: "order-service:8080"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Test path matching
	testCases := []struct {
		name     string
		path     string
		expected string
	}{
		{"users path", "/users/123", "users"},
		{"products path", "/products/456", "products"},
		{"orders path", "/orders/789", "orders"},
		{"nested users path", "/users/123/profile", "users"},
		{"nested products path", "/products/456/details", "products"},
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

func (s) TestRadixTreeBalancer_Picker(t *testing.T) {
	// Create test configuration
	config := &LBConfig{
		Routes: map[string]RouteConfig{
			"api": {
				Path:   "/api/*",
				Weight: 100,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
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
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Set sub-connections to ready state
	for sc := range bal.subConns {
		bal.UpdateSubConnState(sc, balancer.SubConnState{
			ConnectivityState: connectivity.Ready,
		})
	}

	// Create picker
	picker := &radixTreePicker{
		tree:   bal.tree,
		routes: bal.routes,
	}

	// Test picking with path from metadata
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-grpc-path", "/api/users"))
	result, err := picker.Pick(balancer.PickInfo{
		Ctx: ctx,
	})
	if err != nil {
		t.Errorf("Picker.Pick failed: %v", err)
	}
	if result.SubConn == nil {
		t.Error("Expected non-nil SubConn from picker")
	}

	// Test picking with method name
	result, err = picker.Pick(balancer.PickInfo{
		FullMethodName: "/service.UserService/GetUser",
	})
	if err != nil {
		t.Errorf("Picker.Pick failed: %v", err)
	}
	if result.SubConn == nil {
		t.Error("Expected non-nil SubConn from picker")
	}
}

func (s) TestRadixTreeBalancer_ConfigParsing(t *testing.T) {
	// Test JSON configuration parsing
	jsonConfig := `{
		"routes": {
			"api-v1": {
				"path": "/api/v1/*",
				"weight": 70,
				"addresses": [
					{"addr": "backend1:8080"},
					{"addr": "backend2:8080"}
				]
			},
			"api-v2": {
				"path": "/api/v2/*",
				"weight": 30,
				"addresses": [
					{"addr": "backend3:8080"}
				]
			}
		},
		"defaultRoute": "api-v1"
	}`

	var cfg LBConfig
	err := json.Unmarshal([]byte(jsonConfig), &cfg)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	// Verify configuration
	if len(cfg.Routes) != 2 {
		t.Errorf("Expected 2 routes, got %d", len(cfg.Routes))
	}

	if cfg.DefaultRoute != "api-v1" {
		t.Errorf("Expected default route 'api-v1', got '%s'", cfg.DefaultRoute)
	}

	// Verify route details
	apiV1, exists := cfg.Routes["api-v1"]
	if !exists {
		t.Error("Route 'api-v1' not found")
	} else {
		if apiV1.Path != "/api/v1/*" {
			t.Errorf("Expected path '/api/v1/*', got '%s'", apiV1.Path)
		}
		if apiV1.Weight != 70 {
			t.Errorf("Expected weight 70, got %d", apiV1.Weight)
		}
		if len(apiV1.Addresses) != 2 {
			t.Errorf("Expected 2 addresses, got %d", len(apiV1.Addresses))
		}
	}
}

func (s) TestRadixTreeBalancer_StateManagement(t *testing.T) {
	// Create test configuration
	config := &LBConfig{
		Routes: map[string]RouteConfig{
			"api": {
				Path:   "/api/*",
				Weight: 100,
				Addresses: []resolver.Address{
					{Addr: "backend1:8080"},
					{Addr: "backend2:8080"},
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
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateClientConnState failed: %v", err)
	}

	// Test state transitions
	var subConns []balancer.SubConn
	for sc := range bal.subConns {
		subConns = append(subConns, sc)
	}

	// Set first sub-connection to connecting
	bal.UpdateSubConnState(subConns[0], balancer.SubConnState{
		ConnectivityState: connectivity.Connecting,
	})

	// Verify overall state is connecting
	if bal.state.ConnectivityState != connectivity.Connecting {
		t.Errorf("Expected state Connecting, got %v", bal.state.ConnectivityState)
	}

	// Set first sub-connection to ready
	bal.UpdateSubConnState(subConns[0], balancer.SubConnState{
		ConnectivityState: connectivity.Ready,
	})

	// Verify overall state is ready
	if bal.state.ConnectivityState != connectivity.Ready {
		t.Errorf("Expected state Ready, got %v", bal.state.ConnectivityState)
	}

	// Verify picker is created
	if bal.state.Picker == nil {
		t.Error("Expected non-nil picker when state is ready")
	}
}

func (s) TestRadixTreeBalancer_ErrorHandling(t *testing.T) {
	// Test with invalid configuration
	cc := testutils.NewBalancerClientConn(t)
	bal := &radixTreeBalancer{
		cc:       cc,
		tree:     radix.NewTree(),
		routes:   make(map[string]*routeInfo),
		subConns: make(map[balancer.SubConn]*subConnInfo),
	}

	// Test with nil config
	err := bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: nil,
	})
	if err == nil {
		t.Error("Expected error with nil config")
	}

	// Test with wrong config type
	err = bal.UpdateClientConnState(balancer.ClientConnState{
		BalancerConfig: &serviceconfig.LoadBalancingConfig{},
	})
	if err == nil {
		t.Error("Expected error with wrong config type")
	}
}
