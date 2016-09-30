package gimlet

//go:generate stringer -type=httpMethod
type httpMethod int

// Typed constants for specifying HTTP method types on routes.
const (
	GET httpMethod = iota
	PUT
	POST
	DELETE
	PATCH
)
