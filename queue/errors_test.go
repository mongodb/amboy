package queue

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDuplicateError(t *testing.T) {
	assert.False(t, IsDuplicateJobError(errors.New("err")))
	assert.False(t, IsDuplicateJobError(nil))
	assert.True(t, IsDuplicateJobError(NewDuplicatJobError("err")))
	assert.True(t, IsDuplicateJobError(NewDuplicatJobErrorf("err")))
	assert.True(t, IsDuplicateJobError(NewDuplicatJobErrorf("err %s", "err")))
	assert.True(t, IsDuplicateJobError(MakeDuplicateJobError(errors.New("err"))))
}
