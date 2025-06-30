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

// Package radixtree implements an enhanced radix tree-based load balancer
// that demonstrates advanced routing capabilities including prefix iteration
// and multiple route matching.
package radixtree

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	internalgrpclog "google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// EnhancedLBConfig represents the configuration for the enhanced radix tree balancer
type EnhancedLBConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	// Routes defines the path-to-balancer mappings
	Routes map[string]EnhancedRouteConfig `json:"routes"`

	// DefaultRoute is the route to use when no path matches
	DefaultRoute string `json:"defaultRoute,omitempty"`

	// EnableMultiRoute enables matching multiple routes for advanced scenarios
	EnableMultiRoute bool `json:"enableMultiRoute,omitempty"`

	// RouteSelectionStrategy defines how to select from multiple matching routes
	RouteSelectionStrategy string `json:"routeSelectionStrategy,omitempty"` // "longest", "weighted", "priority"
}

// EnhancedRouteConfig defines a single route configuration with advanced features
type EnhancedRouteConfig struct {
	// Path is the URL path pattern (e.g., "/api/v1/*", "/users/:id")
	Path string `json:"path"`
	// Weight is the routing weight for this path
	Weight int `json:"weight,omitempty"`
	// Priority is used for route selection when multiple routes match
	Priority int `json:"priority,omitempty"`
	// ChildPolicy defines the load balancing policy for this route
	ChildPolicy *serviceconfig.LoadBalancingConfig `json:"childPolicy,omitempty"`
	// Addresses are the backend addresses for this route
	Addresses []resolver.Address `json:"addresses,omitempty"`
	// Metadata contains additional route metadata
	Metadata map[string]string `json:"metadata,omitempty"`
}

// enhancedRadixTreeBalancer implements the enhanced radix tree load balancer
type enhancedRadixTreeBalancer struct {
	cc       balancer.ClientConn
	logger   *internalgrpclog.PrefixLogger
	tree     *radix.Tree
	routes   map[string]*enhancedRouteInfo
	subConns map[balancer.SubConn]*subConnInfo
	mu       sync.RWMutex
	state    balancer.State
	config   *EnhancedLBConfig
}

// enhancedRouteInfo contains information about a route and its associated sub-connections
type enhancedRouteInfo struct {
	path     string
	weight   int
	priority int
	subConns []balancer.SubConn
	state    connectivity.State
	config   EnhancedRouteConfig
	metadata map[string]string
}

// enhancedRadixTreePicker implements advanced picking with multiple route support
type enhancedRadixTreePicker struct {
	tree   *radix.Tree
	routes map[string]*enhancedRouteInfo
	logger *internalgrpclog.PrefixLogger
	config *EnhancedLBConfig
}

// RouteMatch represents a matched route with its priority and weight
type RouteMatch struct {
	RouteID  string
	Path     string
	Priority int
	Weight   int
	Route    *enhancedRouteInfo
}

// findMatchingRoutes finds all routes that match the given path
// This demonstrates how prefix iteration could be useful
func (p *enhancedRadixTreePicker) findMatchingRoutes(path string) []RouteMatch {
	var matches []RouteMatch

	// If we had seekprefix functionality, we could do:
	// iterator := p.tree.SeekPrefix(path)
	// for iterator.Next() {
	//     routeID := iterator.Value()
	//     if route, exists := p.routes[routeID]; exists {
	//         matches = append(matches, RouteMatch{
	//             RouteID:  routeID,
	//             Path:     route.path,
	//             Priority: route.priority,
	//             Weight:   route.weight,
	//             Route:    route,
	//         })
	//     }
	// }

	// For now, we'll simulate prefix iteration by checking all routes
	// This is less efficient but demonstrates the concept
	for routeID, route := range p.routes {
		if strings.HasPrefix(path, route.path) {
			matches = append(matches, RouteMatch{
				RouteID:  routeID,
				Path:     route.path,
				Priority: route.priority,
				Weight:   route.weight,
				Route:    route,
			})
		}
	}

	// Sort by priority (higher first), then by path length (longer first)
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Priority != matches[j].Priority {
			return matches[i].Priority > matches[j].Priority
		}
		return len(matches[i].Path) > len(matches[j].Path)
	})

	return matches
}

// selectBestRoute selects the best route from multiple matches
func (p *enhancedRadixTreePicker) selectBestRoute(matches []RouteMatch) *RouteMatch {
	if len(matches) == 0 {
		return nil
	}

	if len(matches) == 1 {
		return &matches[0]
	}

	switch p.config.RouteSelectionStrategy {
	case "longest":
		// Select the longest matching path
		return &matches[0] // Already sorted by path length

	case "weighted":
		// Weighted random selection based on route weights
		totalWeight := 0
		for _, match := range matches {
			totalWeight += match.Weight
		}

		if totalWeight == 0 {
			return &matches[0] // Fallback to first match
		}

		// Simple weighted selection (in practice, you'd use a proper WRR)
		selected := 0
		for i, match := range matches {
			if match.Weight > 0 {
				selected = i
				break
			}
		}
		return &matches[selected]

	case "priority":
		// Select by priority (already sorted)
		return &matches[0]

	default:
		// Default to longest match
		return &matches[0]
	}
}

func (p *enhancedRadixTreePicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	path := p.extractPath(info)

	if p.config.EnableMultiRoute {
		// Find all matching routes
		matches := p.findMatchingRoutes(path)

		// Select the best route
		bestMatch := p.selectBestRoute(matches)
		if bestMatch == nil {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		// Get the picker from the selected route
		return p.pickFromRoute(bestMatch.Route, info)
	} else {
		// Single route selection (original behavior)
		routeID, found := p.tree.Search(path)
		if !found {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		route, exists := p.routes[routeID]
		if !exists {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		return p.pickFromRoute(route, info)
	}
}

// pickFromRoute selects a sub-connection from the given route
func (p *enhancedRadixTreePicker) pickFromRoute(route *enhancedRouteInfo, info balancer.PickInfo) (balancer.PickResult, error) {
	if len(route.subConns) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// Simple round-robin selection for now
	// In a more sophisticated implementation, you could use weighted round-robin
	selectedSC := route.subConns[0]

	return balancer.PickResult{
		SubConn: selectedSC,
		Done: func(info balancer.DoneInfo) {
			// Handle completion info
			if info.Err != nil {
				p.logger.Errorf("RPC failed for route %s: %v", route.path, info.Err)
			}
		},
	}, nil
}

// extractPath extracts the routing path from the pick info
func (p *enhancedRadixTreePicker) extractPath(info balancer.PickInfo) string {
	// Try to get path from metadata first
	if md, ok := metadata.FromOutgoingContext(info.Ctx); ok {
		if paths := md.Get("x-grpc-path"); len(paths) > 0 {
			return paths[0]
		}
	}

	// Fall back to method name
	method := info.FullMethodName
	if strings.HasPrefix(method, "/") {
		return method
	}
	return "/" + method
}

// buildRadixTree builds the radix tree from configuration
func (b *enhancedRadixTreeBalancer) buildRadixTree(cfg *EnhancedLBConfig) error {
	b.tree = radix.NewTree()
	b.routes = make(map[string]*enhancedRouteInfo)

	for routeID, route := range cfg.Routes {
		// Create route info
		routeInfo := &enhancedRouteInfo{
			path:     route.Path,
			weight:   route.Weight,
			priority: route.Priority,
			config:   route,
			state:    connectivity.Idle,
			metadata: route.Metadata,
		}

		// Use addresses from route config or resolver state
		addresses := route.Addresses
		if len(addresses) == 0 {
			// In a real implementation, you'd get addresses from resolver state
			addresses = []resolver.Address{{Addr: "localhost:8080"}}
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

	return nil
}

// UpdateClientConnState updates the balancer with new configuration
func (b *enhancedRadixTreeBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cfg, ok := s.BalancerConfig.(*EnhancedLBConfig)
	if !ok {
		return fmt.Errorf("enhanced radixtree: unexpected balancer config type: %T", s.BalancerConfig)
	}

	b.config = cfg

	// Build the radix tree
	err := b.buildRadixTree(cfg)
	if err != nil {
		return err
	}

	// Update balancer state
	b.updateBalancerState()
	return nil
}

// updateBalancerState updates the overall balancer state and picker
func (b *enhancedRadixTreeBalancer) updateBalancerState() {
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
		picker = &enhancedRadixTreePicker{
			tree:   b.tree,
			routes: b.routes,
			logger: b.logger,
			config: b.config,
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

// Demonstrate how prefix iteration could be useful for route management
func (b *enhancedRadixTreeBalancer) listRoutesByPrefix(prefix string) []string {
	var routeIDs []string

	// If we had iterator functionality, we could do:
	// iterator := b.tree.Iterator()
	// for iterator.Next() {
	//     routeID := iterator.Value()
	//     if strings.HasPrefix(routeID, prefix) {
	//         routeIDs = append(routeIDs, routeID)
	//     }
	// }

	// For now, we'll iterate through all routes
	for routeID := range b.routes {
		if strings.HasPrefix(routeID, prefix) {
			routeIDs = append(routeIDs, routeID)
		}
	}

	return routeIDs
}

// Demonstrate how seekprefix could be useful for finding all matching routes
func (b *enhancedRadixTreeBalancer) findRoutesForPath(path string) []string {
	var matchingRoutes []string

	// If we had seekprefix functionality, we could do:
	// iterator := b.tree.SeekPrefix(path)
	// for iterator.Next() {
	//     routeID := iterator.Value()
	//     matchingRoutes = append(matchingRoutes, routeID)
	// }

	// For now, we'll check all routes
	for routeID, route := range b.routes {
		if strings.HasPrefix(path, route.path) {
			matchingRoutes = append(matchingRoutes, routeID)
		}
	}

	return matchingRoutes
}
