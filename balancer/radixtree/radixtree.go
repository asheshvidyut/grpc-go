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

// Package radixtree implements a radix tree-based load balancer for path-based routing.
// This balancer uses an optimized radix tree from github.com/asheshvidyut/prefix-search-optimized-radix
// to efficiently route requests based on URL paths, HTTP methods, headers, or other hierarchical criteria.
package radixtree

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	internalgrpclog "google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// Name is the name of the radix tree balancer.
const Name = "radix_tree_experimental"

var logger = grpclog.Component("radixtree")

func init() {
	balancer.Register(builder{})
}

type builder struct{}

func (bb builder) Name() string {
	return Name
}

func (bb builder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	bal := &radixTreeBalancer{
		cc:       cc,
		tree:     radix.NewTree(),
		routes:   make(map[string]*routeInfo),
		subConns: make(map[balancer.SubConn]*subConnInfo),
	}
	bal.logger = internalgrpclog.NewPrefixLogger(logger, fmt.Sprintf("[%p] ", bal))
	bal.logger.Infof("Created")
	return bal
}

// routeInfo contains information about a route and its associated sub-connections
type routeInfo struct {
	path     string
	weight   int
	subConns []balancer.SubConn
	state    connectivity.State
	config   RouteConfig
}

// subConnInfo contains information about a sub-connection
type subConnInfo struct {
	routeID string
	addr    resolver.Address
}

// radixTreeBalancer implements the radix tree load balancer
type radixTreeBalancer struct {
	cc       balancer.ClientConn
	logger   *internalgrpclog.PrefixLogger
	tree     *radix.Tree
	routes   map[string]*routeInfo
	subConns map[balancer.SubConn]*subConnInfo
	mu       sync.RWMutex
	state    balancer.State
}

// LBConfig represents the configuration for the radix tree balancer
type LBConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	// Routes defines the path-to-balancer mappings
	Routes map[string]RouteConfig `json:"routes"`

	// DefaultRoute is the route to use when no path matches
	DefaultRoute string `json:"defaultRoute,omitempty"`
}

// RouteConfig defines a single route configuration
type RouteConfig struct {
	// Path is the URL path pattern (e.g., "/api/v1/*", "/users/:id")
	Path string `json:"path"`
	// Weight is the routing weight for this path
	Weight int `json:"weight,omitempty"`
	// ChildPolicy defines the load balancing policy for this route
	ChildPolicy *serviceconfig.LoadBalancingConfig `json:"childPolicy,omitempty"`
	// Addresses are the backend addresses for this route
	Addresses []resolver.Address `json:"addresses,omitempty"`
}

func (bb builder) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg LBConfig
	if err := json.Unmarshal(c, &cfg); err != nil {
		return nil, fmt.Errorf("radixtree: unable to unmarshal LB policy config: %s, error: %v", string(c), err)
	}
	return &cfg, nil
}

func (b *radixTreeBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cfg, ok := s.BalancerConfig.(*LBConfig)
	if !ok {
		return fmt.Errorf("radixtree: unexpected balancer config type: %T", s.BalancerConfig)
	}

	// Clear existing tree and routes
	b.tree = radix.NewTree()
	b.routes = make(map[string]*routeInfo)

	// Build the radix tree from configuration
	for routeID, route := range cfg.Routes {
		// Create route info
		routeInfo := &routeInfo{
			path:   route.Path,
			weight: route.Weight,
			config: route,
			state:  connectivity.Idle,
		}

		// Use addresses from route config or resolver state
		addresses := route.Addresses
		if len(addresses) == 0 {
			// Filter addresses by route path if available
			addresses = s.ResolverState.Addresses
		}

		// Create sub-connections for this route
		for _, addr := range addresses {
			sc, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{})
			if err != nil {
				b.logger.Errorf("Failed to create sub-connection for route %s: %v", routeID, err)
				continue
			}

			routeInfo.subConns = append(routeInfo.subConns, sc)
			b.subConns[sc] = &subConnInfo{
				routeID: routeID,
				addr:    addr,
			}
		}

		// Store route information
		b.routes[routeID] = routeInfo

		// Insert into radix tree
		err := b.tree.Insert(route.Path, routeID)
		if err != nil {
			b.logger.Errorf("Failed to insert route %s into radix tree: %v", routeID, err)
		}
	}

	// Update balancer state
	b.updateBalancerState()
	return nil
}

func (b *radixTreeBalancer) ResolverError(err error) {
	b.logger.Errorf("Resolver error: %v", err)
}

func (b *radixTreeBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subConnInfo, exists := b.subConns[sc]
	if !exists {
		b.logger.Errorf("UpdateSubConnState called for unknown sub-connection: %v", sc)
		return
	}

	routeInfo, exists := b.routes[subConnInfo.routeID]
	if !exists {
		b.logger.Errorf("Route not found for sub-connection: %v", subConnInfo.routeID)
		return
	}

	// Update route state based on sub-connection states
	routeInfo.state = state.ConnectivityState
	b.updateBalancerState()
}

func (b *radixTreeBalancer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Close all sub-connections
	for sc := range b.subConns {
		b.cc.RemoveSubConn(sc)
	}
}

func (b *radixTreeBalancer) ExitIdle() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Exit idle for all sub-connections
	for sc := range b.subConns {
		sc.Connect()
	}
}

// updateBalancerState updates the overall balancer state and picker
func (b *radixTreeBalancer) updateBalancerState() {
	// Determine overall connectivity state
	overallState := connectivity.Idle
	hasReady := false
	hasConnecting := false
	hasTransientFailure := false

	for _, route := range b.routes {
		switch route.state {
		case connectivity.Ready:
			hasReady = true
		case connectivity.Connecting:
			hasConnecting = true
		case connectivity.TransientFailure:
			hasTransientFailure = true
		}
	}

	if hasReady {
		overallState = connectivity.Ready
	} else if hasConnecting {
		overallState = connectivity.Connecting
	} else if hasTransientFailure {
		overallState = connectivity.TransientFailure
	}

	// Create picker
	var picker balancer.Picker
	if overallState == connectivity.Ready {
		picker = &radixTreePicker{
			tree:   b.tree,
			routes: b.routes,
			logger: b.logger,
		}
	} else {
		picker = &errPicker{err: balancer.ErrNoSubConnAvailable}
	}

	// Update state
	b.state = balancer.State{
		ConnectivityState: overallState,
		Picker:            picker,
	}
	b.cc.UpdateState(b.state)
}

// radixTreePicker implements the picker interface for radix tree routing
type radixTreePicker struct {
	tree   *radix.Tree
	routes map[string]*routeInfo
	logger *internalgrpclog.PrefixLogger
}

func (p *radixTreePicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// Extract path from metadata or method name
	path := p.extractPath(info)

	// Look up the appropriate route in the radix tree
	routeID, found := p.tree.Search(path)
	if !found {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// Get route information
	route, exists := p.routes[routeID]
	if !exists {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// Check if route has ready sub-connections
	if len(route.subConns) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// Simple round-robin selection for now
	// In a more sophisticated implementation, you could use weighted round-robin
	selectedSC := route.subConns[0] // For simplicity, always pick the first one

	return balancer.PickResult{
		SubConn: selectedSC,
	}, nil
}

// extractPath extracts the routing path from the pick info
func (p *radixTreePicker) extractPath(info balancer.PickInfo) string {
	// Try to get path from metadata first
	if md, ok := metadata.FromOutgoingContext(info.Ctx); ok {
		if paths := md.Get("x-grpc-path"); len(paths) > 0 {
			return paths[0]
		}
	}

	// Fall back to method name
	// Convert gRPC method name to path-like format
	// e.g., "/service.UserService/GetUser" -> "/service/UserService/GetUser"
	method := info.FullMethodName
	if strings.HasPrefix(method, "/") {
		return method
	}
	return "/" + method
}

// errPicker is a picker that always returns an error
type errPicker struct {
	err error
}

func (p *errPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{}, p.err
}
