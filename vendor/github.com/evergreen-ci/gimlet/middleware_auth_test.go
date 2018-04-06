package gimlet

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMiddlewareValueAccessors(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	auth, ok := safeGetAuthenticator(ctx)
	assert.False(ok)
	assert.Nil(auth)
	assert.Panics(func() { auth = GetAuthenticator(ctx) })
	assert.Nil(auth)

	userm, ok := safeGetUserManager(ctx)
	assert.False(ok)
	assert.Nil(userm)
	assert.Panics(func() { userm = GetUserManager(ctx) })
	assert.Nil(userm)

	var idone, idtwo int
	idone = getNumber()
	assert.Equal(0, idtwo)
	assert.True(idone > 0)
	assert.NotPanics(func() { idtwo = GetRequestID(ctx) })
	assert.True(idone > idtwo)
	assert.Equal(-1, idtwo)
}
