package parallel

import (
	"golang.org/x/sync/errgroup"
)

// ForEachWithErrors iterates over elements of collection and invokes iteratee for each element in parallel.
// If any error occurs in one of the goroutines, it returns the first encountered error.
func ForEachWithErrors[T any](collection []T, iteratee func(item T, index int) error) error {
	var g errgroup.Group

	for i, item := range collection {
		_item := item
		_i := i

		g.Go(func() error {
			return iteratee(_item, _i)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
