package contract

import "errors"

// ErrUnauthenticated is returned by AuthProvider.Authenticate when the
// credentials are not valid.
var ErrUnauthenticated = errors.New("unauthenticated")
