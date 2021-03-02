package amboy

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDuplicateError(t *testing.T) {
	t.Run("RegularErrorIsNotDuplicate", func(t *testing.T) {
		err := errors.New("err")
		assert.False(t, IsDuplicateJobError(err))
		assert.False(t, IsDuplicateJobScopeError(err))
	})
	t.Run("NilErrorIsNotDuplicate", func(t *testing.T) {
		assert.False(t, IsDuplicateJobError(nil))
		assert.False(t, IsDuplicateJobScopeError(nil))
	})
	t.Run("NewDuplicateJobError", func(t *testing.T) {
		err := NewDuplicateJobError("err")
		assert.True(t, IsDuplicateJobError(err))
		assert.False(t, IsDuplicateJobScopeError(err))
	})
	t.Run("NewDuplicateJobErrorf", func(t *testing.T) {
		err := NewDuplicateJobErrorf("err %s", "err")
		assert.True(t, IsDuplicateJobError(err))
		assert.False(t, IsDuplicateJobScopeError(err))
	})
	t.Run("MakeDuplicateJobError", func(t *testing.T) {
		err := MakeDuplicateJobError(errors.New("err"))
		assert.True(t, IsDuplicateJobError(err))
		assert.False(t, IsDuplicateJobScopeError(err))
	})
	t.Run("NewDuplicateJobScopeError", func(t *testing.T) {
		err := NewDuplicateJobScopeError("err")
		assert.True(t, IsDuplicateJobError(err))
		assert.True(t, IsDuplicateJobScopeError(err))
	})
	t.Run("NewDuplicateScopeJobErrorf", func(t *testing.T) {
		err := NewDuplicateJobScopeErrorf("err %s", "err")
		assert.True(t, IsDuplicateJobError(err))
		assert.True(t, IsDuplicateJobScopeError(err))
	})
	t.Run("MakeDuplicateScopeJobError", func(t *testing.T) {
		err := MakeDuplicateJobScopeError(errors.New("err"))
		assert.True(t, IsDuplicateJobError(err))
		assert.True(t, IsDuplicateJobScopeError(err))
	})
}
