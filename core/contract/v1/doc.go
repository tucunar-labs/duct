// Package contract defines the stable, versioned extension contracts of Duct
// (version 1).
//
// Modules under modules/ implement these interfaces and register themselves
// with core/registry. The core and the modules only meet here.
//
// # Versioning rules
//
//   - This package path (core/contract/v1) is frozen once a module depends on it.
//   - Allowed in v1: adding new types, new constants, and new OPTIONAL
//     interfaces that callers discover by type assertion.
//   - Breaking changes (removing or changing a method or field, tightening an
//     invariant) require a new package, core/contract/v2. v1 keeps working
//     and the core adapts it, so no module breaks.
//   - Every interface documents its invariants; the matching conformance
//     suite lives in core/contract/v1/contracttest and must pass for every
//     implementation.
package contract
