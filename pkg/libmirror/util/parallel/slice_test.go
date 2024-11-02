package parallel

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Sample error to test with
var sampleError = errors.New("sample error")

// Test cases for ForEachWithErrors function
func TestForEachWithErrors(t *testing.T) {
	t.Run("All Success", func(t *testing.T) {
		collection := []int{1, 2, 3, 4}
		err := ForEachWithErrors(collection, func(item int, index int) error {
			// All items succeed
			return nil
		})
		assert.NoError(t, err, "Expected no error, but got one")
	})

	t.Run("Single Error", func(t *testing.T) {
		collection := []int{1, 2, 3, 4}
		err := ForEachWithErrors(collection, func(item int, index int) error {
			if item == 2 {
				return sampleError // Introduce an error on item 2
			}
			return nil
		})
		assert.Error(t, err, "Expected an error, but got nil")
		assert.Equal(t, sampleError, err, "Expected the returned error to be sampleError")
	})

	t.Run("Multiple Errors", func(t *testing.T) {
		collection := []int{1, 2, 3, 4}
		err := ForEachWithErrors(collection, func(item int, index int) error {
			if item%2 == 0 {
				return sampleError // Error on even items
			}
			return nil
		})
		assert.Error(t, err, "Expected an error, but got nil")
	})

	t.Run("Empty Collection", func(t *testing.T) {
		collection := []int{}
		err := ForEachWithErrors(collection, func(item int, index int) error {
			return nil // Should not be called
		})
		assert.NoError(t, err, "Expected no error for empty collection, but got one")
	})
}
