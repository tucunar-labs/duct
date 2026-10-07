package sharing

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Profile is the file a recipient hands to a Delta Sharing client (`.share`).
type Profile struct {
	ShareCredentialsVersion int    `json:"shareCredentialsVersion"`
	Endpoint                string `json:"endpoint"`
	BearerToken             string `json:"bearerToken"`
	ExpirationTime          string `json:"expirationTime,omitempty"`
}

// NewProfile builds the profile of an issued token for the REST endpoint.
func NewProfile(endpoint string, is Issued) Profile {
	p := Profile{ShareCredentialsVersion: 1, Endpoint: endpoint, BearerToken: is.Token}
	if !is.ExpiresAt.IsZero() {
		p.ExpirationTime = is.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return p
}

// JSON renders the profile.
func (p Profile) JSON() []byte {
	b, _ := json.MarshalIndent(p, "", "  ")
	return append(b, '\n')
}

// WriteFile writes the profile with mode 600, refusing to overwrite.
func (p Profile) WriteFile(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write profile: %w", err)
	}
	if _, err := f.Write(p.JSON()); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
