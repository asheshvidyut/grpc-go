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
	"strconv"
	"sync"

	"github.com/asheshvidyut/prefix-search-optimized-radix/radix"
)

// radixTreeWRR uses a radix tree of radix trees for optimal performance
// Outer radix tree: weight (as bytes) -> inner radix tree
// Inner radix tree: item key (as bytes) -> item value
type radixTreeWRR struct {
	mu sync.RWMutex
	// Main radix tree: weight -> radix tree of items
	weightTree *radix.Tree
	// Total weight for normalization
	totalWeight int64
	// Number of items
	itemCount int
}

// NewRadixTreeWRR creates a new WRR using radix tree of radix trees
func NewRadixTreeWRR() WRR {
	return &radixTreeWRR{
		weightTree: radix.New(),
	}
}

// Add adds an item with weight to the WRR set
func (rw *radixTreeWRR) Add(item any, weight int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Convert weight to bytes for radix tree key
	weightKey := []byte(strconv.FormatInt(weight, 10))
	itemKey := []byte(fmt.Sprintf("%v", item))

	// Get or create inner radix tree for this weight
	var innerTree *radix.Tree
	if existingValue, found := rw.weightTree.Get(weightKey); found {
		innerTree = existingValue.(*radix.Tree)
	} else {
		innerTree = radix.New()
	}

	// Add item to inner radix tree
	newInnerTree, _, _ := innerTree.Insert(itemKey, item)

	// Update outer radix tree with new inner tree
	newWeightTree, _, _ := rw.weightTree.Insert(weightKey, newInnerTree)
	rw.weightTree = newWeightTree

	rw.totalWeight += weight
	rw.itemCount++
}

// Next returns the next item using radix tree selection
func (rw *radixTreeWRR) Next() any {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if rw.itemCount == 0 {
		return nil
	}

	// Use radix tree for weighted selection
	randomWeight := rand.Int64N(rw.totalWeight)
	return rw.findByWeight(randomWeight)
}

// findByWeight uses the radix tree structure for efficient weight-based selection
func (rw *radixTreeWRR) findByWeight(targetWeight int64) any {
	var currentWeight int64

	// Iterate through weights in sorted order (radix tree provides this naturally)
	iterator := rw.weightTree.Root().Iterator()

	for {
		weightBytes, value, found := iterator.Next()
		if !found {
			break
		}

		// Parse weight from bytes
		weight, err := strconv.ParseInt(string(weightBytes), 10, 64)
		if err != nil {
			continue
		}

		innerTree := value.(*radix.Tree)

		// Count items with this weight
		itemCount := int64(0)
		innerIterator := innerTree.Root().Iterator()
		for {
			_, _, found := innerIterator.Next()
			if !found {
				break
			}
			itemCount++
		}

		// Calculate total weight for this category
		categoryWeight := weight * itemCount

		// Check if target weight falls in this category
		if currentWeight <= targetWeight && targetWeight < currentWeight+categoryWeight {
			// Calculate which item to select within this weight category
			itemIndex := (targetWeight - currentWeight) / weight

			// Find the specific item
			innerIterator = innerTree.Root().Iterator()
			var count int64
			for {
				_, item, found := innerIterator.Next()
				if !found {
					break
				}

				if count == itemIndex {
					return item
				}
				count++
			}
		}

		currentWeight += categoryWeight
	}

	// Fallback: return first item
	iterator = rw.weightTree.Root().Iterator()
	if _, value, found := iterator.Next(); found {
		innerTree := value.(*radix.Tree)
		innerIterator := innerTree.Root().Iterator()
		if _, item, found := innerIterator.Next(); found {
			return item
		}
	}

	return nil
}

// String returns string representation
func (rw *radixTreeWRR) String() string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	return fmt.Sprintf("RadixTreeWRR(weighted, total_weight=%d, items=%d)",
		rw.totalWeight, rw.itemCount)
}
