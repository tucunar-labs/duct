package project

// BackupSettings is the [backup] table of duct.toml (ADR 0013):
//
//	[backup]
//	recipients = ["age1..."]   # public keys (age X25519 or ssh-ed25519/ssh-rsa); with any, `duct backup` encrypts
//
// The identity (private key) is never in duct.toml: restore reads it from
// --identity or DUCT_BACKUP_IDENTITY.
type BackupSettings struct {
	Recipients []string `toml:"recipients"`
}

// EnvBackupIdentity names the age identity file `duct restore` uses to read an
// encrypted backup (the --identity flag wins).
const EnvBackupIdentity = "DUCT_BACKUP_IDENTITY"
