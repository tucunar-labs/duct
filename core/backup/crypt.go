package backup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"

	"filippo.io/age"
	"filippo.io/age/agessh"

	"github.com/alonsoburon/duct/core/project"
)

// EncryptedSuffix is appended to the key of every encrypted object of a backup.
const EncryptedSuffix = ".age"

// Crypto holds what a backup or a restore needs to encrypt or decrypt objects
// with age (https://age-encryption.org). A nil *Crypto means plaintext.
type Crypto struct {
	Recipients []age.Recipient
	// RecipientSpecs are the public keys as given (they go into the manifest).
	RecipientSpecs []string
	Identities     []age.Identity
}

// NewEncrypter parses recipients (age1... X25519, age1pq..., ssh-ed25519 or
// ssh-rsa public keys) into a Crypto that encrypts.
func NewEncrypter(specs []string) (*Crypto, error) {
	c := &Crypto{}
	for _, s := range specs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		var r age.Recipient
		var err error
		if strings.HasPrefix(s, "ssh-") {
			r, err = agessh.ParseRecipient(s)
		} else {
			var rs []age.Recipient
			if rs, err = age.ParseRecipients(strings.NewReader(s)); err == nil {
				if len(rs) != 1 {
					err = errors.New("expected one recipient")
				} else {
					r = rs[0]
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("backup: recipient %q: %w", shorten(s), err)
		}
		c.Recipients = append(c.Recipients, r)
		c.RecipientSpecs = append(c.RecipientSpecs, s)
	}
	if len(c.Recipients) == 0 {
		return nil, errors.New("backup: --encrypt needs a recipient (--recipient age1... or [backup] recipients in duct.toml)")
	}
	return c, nil
}

func shorten(s string) string {
	if len(s) > 24 {
		return s[:24] + "..."
	}
	return s
}

// LoadIdentities reads an age identity file (AGE-SECRET-KEY-... lines, as
// written by age-keygen) or an unencrypted OpenSSH private key.
func LoadIdentities(file string) ([]age.Identity, error) {
	raw, err := os.ReadFile(file) //nolint:gosec // path chosen by the operator
	if err != nil {
		return nil, fmt.Errorf("identity file: %w", err)
	}
	if bytes.Contains(raw, []byte("-----BEGIN ")) {
		id, err := agessh.ParseIdentity(raw)
		if err != nil {
			return nil, fmt.Errorf("identity file %s: %w", file, err)
		}
		return []age.Identity{id}, nil
	}
	ids, err := age.ParseIdentities(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("identity file %s: %w", file, err)
	}
	return ids, nil
}

type countHash struct {
	h hash.Hash
	n int64
}

func newCountHash() *countHash { return &countHash{h: sha256.New()} }
func (c *countHash) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return c.h.Write(p)
}
func (c *countHash) sum() string { return hex.EncodeToString(c.h.Sum(nil)) }

// encryptPut streams plaintext into dst/key encrypted for the recipients. The
// returned File has the size and SHA-256 of the plaintext and of the stored
// ciphertext.
func (c *Crypto) encryptPut(ctx context.Context, dst Store, key string, plain io.Reader) (File, error) {
	ph := newCountHash()
	pr, pw := io.Pipe()
	go func() {
		w, err := age.Encrypt(pw, c.Recipients...)
		if err == nil {
			_, err = io.Copy(w, io.TeeReader(plain, ph))
			if cerr := w.Close(); err == nil {
				err = cerr
			}
		}
		_ = pw.CloseWithError(err)
	}()
	csize, csha, err := dst.Put(ctx, key, pr)
	_ = pr.CloseWithError(err) // unblock the encrypting goroutine on failure
	if err != nil {
		return File{}, err
	}
	return File{Size: ph.n, SHA256: ph.sum(), Encrypted: true, CipherSize: csize, CipherSHA256: csha}, nil
}

// plainReader reads an encrypted object of a backup as plaintext; close (after
// the reader is drained) checks the ciphertext checksum, the plaintext size and
// the plaintext checksum against the manifest. The age stream is authenticated
// chunk by chunk, so a truncated or modified object fails while it is read.
type plainReader struct {
	io.Reader
	rc     io.ReadCloser
	cipher *countHash
	plain  *countHash
	f      File
}

func (c *Crypto) openPlain(ctx context.Context, src Store, f File) (*plainReader, error) {
	if c == nil || len(c.Identities) == 0 {
		return nil, errors.New("this backup is encrypted: pass --identity <age identity file> (or set " + project.EnvBackupIdentity + ")")
	}
	rc, _, err := src.Get(ctx, f.Path+EncryptedSuffix)
	if err != nil {
		return nil, err
	}
	pr := &plainReader{rc: rc, cipher: newCountHash(), plain: newCountHash(), f: f}
	dec, err := age.Decrypt(bufio.NewReader(io.TeeReader(rc, pr.cipher)), c.Identities...)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("decrypt %s: %w", f.Path, err)
	}
	pr.Reader = io.TeeReader(dec, pr.plain)
	return pr, nil
}

func (p *plainReader) close() error {
	_, _ = io.Copy(io.Discard, p.rc) // the checksum must see every ciphertext byte
	_ = p.rc.Close()
	if p.f.CipherSHA256 != "" && p.cipher.sum() != p.f.CipherSHA256 {
		return fmt.Errorf("%s is corrupted (ciphertext checksum mismatch)", p.f.Path)
	}
	if p.plain.n != p.f.Size || (p.f.SHA256 != "" && p.plain.sum() != p.f.SHA256) {
		return fmt.Errorf("%s does not match the manifest after decryption (size %d, want %d)", p.f.Path, p.plain.n, p.f.Size)
	}
	return nil
}

// decryptPut writes the plaintext of the encrypted object f to dst/dstKey.
func (c *Crypto) decryptPut(ctx context.Context, src Store, f File, dst Store, dstKey string) error {
	pr, err := c.openPlain(ctx, src, f)
	if err != nil {
		return err
	}
	if _, _, err := dst.Put(ctx, dstKey, pr); err != nil {
		_ = pr.rc.Close()
		return fmt.Errorf("decrypt %s: %w", f.Path, err)
	}
	return pr.close()
}

// fetchFile downloads backup object f to a local file (decrypting it if it is
// encrypted) and verifies it against the manifest.
func (c *Crypto) fetchFile(ctx context.Context, src Store, f File, local string) error {
	if !f.Encrypted {
		sum, err := getFile(ctx, src, f.Path, local)
		if err != nil {
			return err
		}
		if f.SHA256 != "" && sum != f.SHA256 {
			return fmt.Errorf("%s is corrupted (checksum mismatch)", f.Path)
		}
		return nil
	}
	pr, err := c.openPlain(ctx, src, f)
	if err != nil {
		return err
	}
	if err := writeLocal(local, pr); err != nil {
		_ = pr.rc.Close()
		return fmt.Errorf("decrypt %s: %w", f.Path, err)
	}
	return pr.close()
}

// FileSHA256 is the hex SHA-256 of a local file (tests and tools that check a
// backup object against its manifest entry).
func FileSHA256(file string) (string, error) {
	f, err := os.Open(file) //nolint:gosec // path chosen by the caller
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := newCountHash()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return h.sum(), nil
}
