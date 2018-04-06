package auth

import (
	"context"
	"net/http"
)

// User provides a common way of interacting with users from
// authentication systems.
//
// Note: this is the User interface from Evergreen with the addition
// of the Roles() method.
type User interface {
	DisplayName() string
	Email() string
	Username() string
	IsNil() bool
	GetAPIKey() string
	Roles() []string
}

type Provider interface {
	Reload(context.Context) error
	Open(context.Context) error
	Close() error
	Authenticator() Authenticator
	UserManager() UserManager
}

// Authenticator represents a service that answers specific
// authentication related questions, and is the public interface used for authentication workflows.
type Authenticator interface {
	CheckResourceAccess(User, string) bool
	CheckGroupAccess(User, string) bool
	CheckAuthenticated(User) bool
	GetUserFromRequest(UserManager, *http.Request) (User, error)
}

// UserManager sets and gets user tokens for implemented
// authentication mechanisms, and provides the data that is sent by
// the api and ui server after authenticating
//
// Note: this is the UserManager interface from Evergreen, without
// modification.
type UserManager interface {
	GetUserByToken(token string) (User, error)
	CreateUserToken(username, password string) (string, error)
	// GetLoginHandler returns the function that starts the login process for auth mechanisms
	// that redirect to a thirdparty site for authentication
	GetLoginHandler(url string) func(http.ResponseWriter, *http.Request)
	// GetLoginRedirectHandler returns the function that does login for the
	// user once it has been redirected from a thirdparty site.
	GetLoginCallbackHandler() func(http.ResponseWriter, *http.Request)
	// IsRedirect returns true if the user must be redirected to a thirdparty site to authenticate
	IsRedirect() bool
}
