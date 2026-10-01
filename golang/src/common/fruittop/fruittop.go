package fruittop

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"sort"
)

func Top(items []fruititem.FruitItem, n int) []fruititem.FruitItem {
	sorted := make([]fruititem.FruitItem, 0, len(items))
	sorted = append(sorted, items...)

	sort.SliceStable(sorted, func(i, j int) bool {
		return comesBefore(sorted[i], sorted[j])
	})

	return sorted[:min(n, len(sorted))]
}

func comesBefore(a, b fruititem.FruitItem) bool {
	if b.Less(a) {
		return true
	}
	if a.Less(b) {
		return false
	}
	return a.Fruit < b.Fruit
}
