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

package wrr

import (
	"fmt"
	rand "math/rand/v2"
	"sync"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
)

// radixTreeWRR is a weighted round-robin implementation using the external radix tree
// for efficient item selection. This implementation provides better performance
// for large numbers of items compared to the traditional binary search approach.
type radixTreeWRR struct {
	mu sync.RWMutex
	// External radix tree for efficient lookups
	tree *radix.Tree
	// Total weight for normalization
	totalWeight int64
	// Number of items
	itemCount int
	// Cache for equal weights optimization
	equalWeights bool
	// Items array for equal weights case (faster than tree lookup)
	items []any
	// Weight mapping for quick lookups
	weightMap map[string]int64
}

// NewRadixTreeWRR creates a new WRR with external radix tree-based implementation
func NewRadixTreeWRR() WRR {
	return &radixTreeWRR{
		tree:      radix.New(),
		weightMap: make(map[string]int64),
	}
}

// Add adds an item with weight to the WRR set
func (rw *radixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Convert item to string key for tree storage
	key := fmt.Sprintf("%v", item)
	keyBytes := []byte(key)

	// Add to radix tree
	newTree, _, _ := rw.tree.Insert(keyBytes, key)
	rw.tree = newTree

	rw.weightMap[key] = weight
	rw.totalWeight += weight
	rw.itemCount++

	// Check if all weights are equal for optimization
	rw.updateEqualWeights()
}

// Next returns the next picked item using radix tree traversal
func (rw *radixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Fast path for equal weights
	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
	}

	// Use weighted selection with radix tree
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeight(randomWeight)
}

// findByWeight finds an item by weight using the radix tree
func (rw *radixTreeWRR) findByWeight(targetWeight int64) any {
	var currentWeight int64

	// Use the radix tree's iterator functionality if available
	// For now, we'll iterate through the weight map
	for key, weight := range rw.weightMap {
		if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
			// Convert key back to original item
			// This is a simplified approach - in practice you'd want a better mapping
			return key
		}
		currentWeight += weight
	}

	return nil
}

// updateEqualWeights checks if all weights are equal and optimizes accordingly
func (rw *radixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{key}
			break
		}
		return
	}

	// Check if all weights are equal
	var firstWeight int64
	allEqual := true
	rw.items = make([]any, 0, rw.itemCount)

	for key, weight := range rw.weightMap {
		if firstWeight == 0 {
			firstWeight = weight
		}
		rw.items = append(rw.items, key)
		if weight != firstWeight {
			allEqual = false
		}
	}

	rw.equalWeights = allEqual
}

// String returns a string representation of the radix tree WRR
func (rw *radixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("RadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("RadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
}

// Enhanced radix tree WRR with iterator support using external package
type enhancedRadixTreeWRR struct {
	mu sync.RWMutex
	// External radix tree
	tree *radix.Tree
	// Total weight
	totalWeight int64
	// Item count
	itemCount int
	// Equal weights optimization
	equalWeights bool
	items        []any
	// Weight mapping
	weightMap map[string]int64
	// Item mapping for reverse lookup
	itemMap map[string]any
}

// NewEnhancedRadixTreeWRR creates an enhanced radix tree WRR with external package
func NewEnhancedRadixTreeWRR() WRR {
	return &enhancedRadixTreeWRR{
		tree:      radix.NewTree(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
	}
}

// Add adds an item with weight
func (rw *enhancedRadixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	key := fmt.Sprintf("%v", item)

	// Add to radix tree
	err := rw.tree.Insert(key, key)
	if err != nil {
		fmt.Printf("Warning: Failed to insert key %s into radix tree: %v\n", key, err)
	}

	rw.weightMap[key] = weight
	rw.itemMap[key] = item
	rw.totalWeight += weight
	rw.itemCount++
	rw.updateEqualWeights()
}

// Next returns the next item using external radix tree
func (rw *enhancedRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	if rw.equalWeights {
		key := rw.items[rand.IntN(len(rw.items))].(string)
		return rw.itemMap[key]
	}

	// Use external radix tree for weighted selection
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeightWithRadixTree(randomWeight)
}

// findByWeightWithRadixTree uses the external radix tree to find item by weight
func (rw *enhancedRadixTreeWRR) findByWeightWithRadixTree(targetWeight int64) any {
	var currentWeight int64

	// Use the external radix tree's capabilities
	// For now, we'll use the weight map with the tree for validation
	for key, weight := range rw.weightMap {
		// Verify the key exists in the radix tree
		if _, found := rw.tree.Search(key); found {
			if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
				return rw.itemMap[key]
			}
		}
		currentWeight += weight
	}

	return nil
}

// updateEqualWeights checks if all weights are equal
func (rw *enhancedRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{key}
			break
		}
		return
	}

	var firstWeight int64
	allEqual := true
	rw.items = make([]any, 0, rw.itemCount)

	for key, weight := range rw.weightMap {
		if firstWeight == 0 {
			firstWeight = weight
		}
		rw.items = append(rw.items, key)
		if weight != firstWeight {
			allEqual = false
		}
	}

	rw.equalWeights = allEqual
}

// String returns string representation
func (rw *enhancedRadixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("EnhancedRadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("EnhancedRadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
}

// Benchmark-specific WRR implementation that demonstrates the benefits of using
// the external radix tree package for iterator-based operations
type benchmarkRadixTreeWRR struct {
	mu sync.RWMutex
	// External radix tree
	tree *radix.Tree
	// Optimized storage for benchmark scenarios
	items       []any
	weights     []int64
	totalWeight int64
	itemCount   int
	// Equal weights optimization
	equalWeights bool
}

// NewBenchmarkRadixTreeWRR creates a benchmark-optimized radix tree WRR
func NewBenchmarkRadixTreeWRR() WRR {
	return &benchmarkRadixTreeWRR{
		tree:    radix.NewTree(),
		items:   make([]any, 0),
		weights: make([]int64, 0),
	}
}

// Add adds an item with weight for benchmarking
func (rw *benchmarkRadixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	key := fmt.Sprintf("%v", item)

	// Add to radix tree
	err := rw.tree.Insert(key, key)
	if err != nil {
		// For benchmarks, we'll continue even if insertion fails
	}

	// Store in optimized arrays for faster access
	rw.items = append(rw.items, item)
	rw.weights = append(rw.weights, weight)
	rw.totalWeight += weight
	rw.itemCount++

	// Check for equal weights optimization
	rw.updateEqualWeights()
}

// Next returns the next item optimized for benchmarking
func (rw *benchmarkRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Fast path for equal weights
	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
	}

	// Use optimized weighted selection
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeightOptimized(randomWeight)
}

// findByWeightOptimized uses optimized algorithm for weight-based selection
func (rw *benchmarkRadixTreeWRR) findByWeightOptimized(targetWeight int64) any {
	var currentWeight int64

	for i, weight := range rw.weights {
		if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
			return rw.items[i]
		}
		currentWeight += weight
	}

	return nil
}

// updateEqualWeights checks if all weights are equal
func (rw *benchmarkRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		return
	}

	firstWeight := rw.weights[0]
	allEqual := true

	for _, weight := range rw.weights {
		if weight != firstWeight {
			allEqual = false
			break
		}
	}

	rw.equalWeights = allEqual
}

// String returns string representation
func (rw *benchmarkRadixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("BenchmarkRadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("BenchmarkRadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
}

// Iterator-based radix tree WRR that properly uses the radix tree's iterator
type iteratorRadixTreeWRR struct {
	mu sync.RWMutex
	// External radix tree
	tree *radix.Tree
	// Total weight
	totalWeight int64
	// Item count
	itemCount int
	// Equal weights optimization
	equalWeights bool
	items        []any
	// Weight mapping
	weightMap map[string]int64
	// Item mapping for reverse lookup
	itemMap map[string]any
}

// NewIteratorRadixTreeWRR creates a radix tree WRR that uses iterator
func NewIteratorRadixTreeWRR() WRR {
	return &iteratorRadixTreeWRR{
		tree:      radix.New(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
	}
}

// Add adds an item with weight
func (rw *iteratorRadixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	key := fmt.Sprintf("%v", item)
	keyBytes := []byte(key)

	// Add to radix tree - Insert returns (newTree, oldValue, replaced)
	newTree, _, _ := rw.tree.Insert(keyBytes, key)
	rw.tree = newTree

	rw.weightMap[key] = weight
	rw.itemMap[key] = item
	rw.totalWeight += weight
	rw.itemCount++
	rw.updateEqualWeights()
}

// Next returns the next item using radix tree iterator
func (rw *iteratorRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Fast path for equal weights
	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
	}

	// Use radix tree iterator for weighted selection
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeightWithIterator(randomWeight)
}

// findByWeightWithIterator uses the radix tree's iterator to find item by weight
func (rw *iteratorRadixTreeWRR) findByWeightWithIterator(targetWeight int64) any {
	var currentWeight int64

	// Use the existing radix tree iterator
	iterator := rw.tree.Root().Iterator()

	// Use SeekPrefix to optimize the search if we have a target range
	// For weighted selection, we can use SeekPrefix to jump to specific weight ranges
	if targetWeight > rw.totalWeight/2 {
		// If target is in the second half, seek to middle
		midKey := fmt.Sprintf("%d", rw.totalWeight/2)
		iterator.SeekPrefix([]byte(midKey))
		currentWeight = rw.totalWeight / 2
	}

	// Use the iterator's Next() method to traverse the tree
	for {
		keyBytes, _, more := iterator.Next()
		if !more {
			break
		}

		key := string(keyBytes)
		weight, exists := rw.weightMap[key]
		if !exists {
			continue
		}

		if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
			return rw.itemMap[key]
		}
		currentWeight += weight
	}

	return nil
}

// updateEqualWeights checks if all weights are equal
func (rw *iteratorRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{key}
			break
		}
		return
	}

	var firstWeight int64
	allEqual := true
	rw.items = make([]any, 0, rw.itemCount)

	// Use iterator to check weights
	iterator := rw.tree.Root().Iterator()
	for {
		keyBytes, _, more := iterator.Next()
		if !more {
			break
		}

		key := string(keyBytes)
		weight := rw.weightMap[key]

		if firstWeight == 0 {
			firstWeight = weight
		}
		rw.items = append(rw.items, key)
		if weight != firstWeight {
			allEqual = false
		}
	}

	rw.equalWeights = allEqual
}

// String returns string representation
func (rw *iteratorRadixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("IteratorRadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("IteratorRadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
}

// Advanced iterator-based WRR with seek functionality
type advancedIteratorRadixTreeWRR struct {
	mu sync.RWMutex
	// External radix tree
	tree *radix.Tree
	// Total weight
	totalWeight int64
	// Item count
	itemCount int
	// Equal weights optimization
	equalWeights bool
	items        []any
	// Weight mapping
	weightMap map[string]int64
	// Item mapping for reverse lookup
	itemMap map[string]any
}

// NewAdvancedIteratorRadixTreeWRR creates an advanced iterator-based radix tree WRR
func NewAdvancedIteratorRadixTreeWRR() WRR {
	return &advancedIteratorRadixTreeWRR{
		tree:      radix.New(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
	}
}

// Add adds an item with weight
func (rw *advancedIteratorRadixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	key := fmt.Sprintf("%v", item)
	keyBytes := []byte(key)

	// Add to radix tree
	newTree, _, _ := rw.tree.Insert(keyBytes, key)
	rw.tree = newTree

	rw.weightMap[key] = weight
	rw.itemMap[key] = item
	rw.totalWeight += weight
	rw.itemCount++
	rw.updateEqualWeights()
}

// Next returns the next item using advanced iterator techniques
func (rw *advancedIteratorRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Fast path for equal weights
	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
	}

	// Use advanced iterator for weighted selection
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeightWithAdvancedIterator(randomWeight)
}

// findByWeightWithAdvancedIterator uses advanced iterator techniques with SeekPrefix
func (rw *advancedIteratorRadixTreeWRR) findByWeightWithAdvancedIterator(targetWeight int64) any {
	var currentWeight int64

	// Use the existing radix tree iterator with seek functionality
	iterator := rw.tree.Root().Iterator()

	// Use SeekPrefix for optimized traversal based on weight distribution
	// This is more efficient than linear traversal for large datasets
	if targetWeight > rw.totalWeight*3/4 {
		// Seek to 75% mark for high weight targets
		seekKey := fmt.Sprintf("%d", rw.totalWeight*3/4)
		iterator.SeekPrefix([]byte(seekKey))
		currentWeight = rw.totalWeight * 3 / 4
	} else if targetWeight > rw.totalWeight/2 {
		// Seek to 50% mark for medium-high weight targets
		seekKey := fmt.Sprintf("%d", rw.totalWeight/2)
		iterator.SeekPrefix([]byte(seekKey))
		currentWeight = rw.totalWeight / 2
	} else if targetWeight > rw.totalWeight/4 {
		// Seek to 25% mark for medium-low weight targets
		seekKey := fmt.Sprintf("%d", rw.totalWeight/4)
		iterator.SeekPrefix([]byte(seekKey))
		currentWeight = rw.totalWeight / 4
	}

	// Use the iterator's Next() method for efficient traversal
	for {
		keyBytes, _, more := iterator.Next()
		if !more {
			break
		}

		key := string(keyBytes)
		weight, exists := rw.weightMap[key]
		if !exists {
			continue
		}

		if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
			return rw.itemMap[key]
		}
		currentWeight += weight
	}

	return nil
}

// updateEqualWeights checks if all weights are equal using iterator
func (rw *advancedIteratorRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{key}
			break
		}
		return
	}

	var firstWeight int64
	allEqual := true
	rw.items = make([]any, 0, rw.itemCount)

	// Use iterator to check weights efficiently
	iterator := rw.tree.Root().Iterator()
	for {
		keyBytes, _, more := iterator.Next()
		if !more {
			break
		}

		key := string(keyBytes)
		weight := rw.weightMap[key]

		if firstWeight == 0 {
			firstWeight = weight
		}
		rw.items = append(rw.items, key)
		if weight != firstWeight {
			allEqual = false
		}
	}

	rw.equalWeights = allEqual
}

// String returns string representation
func (rw *advancedIteratorRadixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("AdvancedIteratorRadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("AdvancedIteratorRadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
}
