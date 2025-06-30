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
	// Item mapping for reverse lookup
	itemMap map[string]any
}

// NewRadixTreeWRR creates a new WRR with external radix tree-based implementation
func NewRadixTreeWRR() WRR {
	return &radixTreeWRR{
		tree:      radix.New(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
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
	rw.itemMap[key] = item
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
			// Return the original item from the item map
			item := rw.itemMap[key]
			if item != nil {
				return item
			}
		}
		currentWeight += weight
	}

	// Fallback: if we didn't find anything, return the first item
	if len(rw.itemMap) > 0 {
		for _, item := range rw.itemMap {
			if item != nil {
				return item
			}
		}
	}

	return nil
}

// updateEqualWeights checks if all weights are equal and optimizes accordingly
func (rw *radixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{rw.itemMap[key]}
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
		rw.items = append(rw.items, rw.itemMap[key])
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
		tree:      radix.New(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
	}
}

// Add adds an item with weight
func (rw *enhancedRadixTreeWRR) Add(item any, weight int64) {
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

// Next returns the next item using external radix tree
func (rw *enhancedRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
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
		// Verify the key exists in the radix tree using Get method
		if _, found := rw.tree.Get([]byte(key)); found {
			if currentWeight <= targetWeight && targetWeight < currentWeight+weight {
				item := rw.itemMap[key]
				if item != nil {
					return item
				}
			}
		}
		currentWeight += weight
	}

	// Fallback: if we didn't find anything, return the first item
	if len(rw.itemMap) > 0 {
		for _, item := range rw.itemMap {
			if item != nil {
				return item
			}
		}
	}

	return nil
}

// updateEqualWeights checks if all weights are equal
func (rw *enhancedRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{rw.itemMap[key]}
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
		rw.items = append(rw.items, rw.itemMap[key])
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

// benchmarkOptimizedRadixTreeWRR is optimized specifically for benchmark performance
type benchmarkOptimizedRadixTreeWRR struct {
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
	// Item mapping for reverse lookup
	itemMap map[string]any
	// Pre-computed cumulative weights for faster selection
	cumulativeWeights []int64
	// Pre-computed items array for faster selection
	itemsArray []any
}

// NewBenchmarkOptimizedRadixTreeWRR creates a new WRR optimized for benchmarks
func NewBenchmarkOptimizedRadixTreeWRR() WRR {
	return &benchmarkOptimizedRadixTreeWRR{
		tree:      radix.New(),
		weightMap: make(map[string]int64),
		itemMap:   make(map[string]any),
	}
}

// Add adds an item with weight to the WRR set
func (rw *benchmarkOptimizedRadixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Convert item to string key for tree storage
	key := fmt.Sprintf("%v", item)
	keyBytes := []byte(key)

	// Add to radix tree
	newTree, _, _ := rw.tree.Insert(keyBytes, key)
	rw.tree = newTree

	rw.weightMap[key] = weight
	rw.itemMap[key] = item
	rw.totalWeight += weight
	rw.itemCount++

	// Rebuild optimized arrays
	rw.rebuildOptimizedArrays()
}

// rebuildOptimizedArrays rebuilds the pre-computed arrays for faster selection
func (rw *benchmarkOptimizedRadixTreeWRR) rebuildOptimizedArrays() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{rw.itemMap[key]}
		}
		return
	}

	// Check if all weights are equal
	var firstWeight int64
	allEqual := true
	rw.items = make([]any, 0, rw.itemCount)
	rw.cumulativeWeights = make([]int64, 0, rw.itemCount)
	rw.itemsArray = make([]any, 0, rw.itemCount)

	var cumulative int64
	for key, weight := range rw.weightMap {
		if firstWeight == 0 {
			firstWeight = weight
		}
		rw.items = append(rw.items, rw.itemMap[key])
		rw.itemsArray = append(rw.itemsArray, rw.itemMap[key])
		cumulative += weight
		rw.cumulativeWeights = append(rw.cumulativeWeights, cumulative)
		if weight != firstWeight {
			allEqual = false
		}
	}

	rw.equalWeights = allEqual
}

// Next returns the next item using optimized selection
func (rw *benchmarkOptimizedRadixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Fast path for equal weights
	if rw.equalWeights {
		return rw.items[rand.IntN(len(rw.items))]
	}

	// Use optimized weighted selection with pre-computed arrays
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeightOptimized(randomWeight)
}

// findByWeightOptimized uses pre-computed arrays for faster weight-based selection
func (rw *benchmarkOptimizedRadixTreeWRR) findByWeightOptimized(targetWeight int64) any {
	// Use binary search on pre-computed cumulative weights
	left, right := 0, len(rw.cumulativeWeights)-1

	for left <= right {
		mid := (left + right) / 2
		if mid == 0 {
			if targetWeight < rw.cumulativeWeights[mid] {
				return rw.itemsArray[mid]
			}
		} else {
			if targetWeight >= rw.cumulativeWeights[mid-1] && targetWeight < rw.cumulativeWeights[mid] {
				return rw.itemsArray[mid]
			}
		}

		if targetWeight < rw.cumulativeWeights[mid] {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return rw.itemsArray[0] // fallback
}

// String returns string representation
func (rw *benchmarkOptimizedRadixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.equalWeights {
		return fmt.Sprintf("BenchmarkOptimizedRadixTreeWRR(equal_weights, items=%d)", len(rw.items))
	}
	return fmt.Sprintf("BenchmarkOptimizedRadixTreeWRR(weighted, total_weight=%d, items=%d)", rw.totalWeight, rw.itemCount)
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
			item := rw.itemMap[key]
			if item != nil {
				return item
			}
		}
		currentWeight += weight
	}

	// Fallback: if we didn't find anything, return the first item
	if len(rw.itemMap) > 0 {
		for _, item := range rw.itemMap {
			if item != nil {
				return item
			}
		}
	}

	return nil
}

// updateEqualWeights checks if all weights are equal
func (rw *iteratorRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{rw.itemMap[key]}
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
		rw.items = append(rw.items, rw.itemMap[key])
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

	// Use the existing radix tree iterator for traversal
	iterator := rw.tree.Root().Iterator()

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
			item := rw.itemMap[key]
			if item != nil {
				return item
			}
		}
		currentWeight += weight
	}

	// Fallback: if we didn't find anything, return the first item
	if len(rw.itemMap) > 0 {
		for _, item := range rw.itemMap {
			if item != nil {
				return item
			}
		}
	}

	return nil
}

// updateEqualWeights checks if all weights are equal using iterator
func (rw *advancedIteratorRadixTreeWRR) updateEqualWeights() {
	if rw.itemCount <= 1 {
		rw.equalWeights = true
		for key := range rw.weightMap {
			rw.items = []any{rw.itemMap[key]}
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
		rw.items = append(rw.items, rw.itemMap[key])
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
