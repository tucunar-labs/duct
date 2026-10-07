// Package governance implements users, roles, grants, row filters, masks,
// tokens and the query audit log of `duct serve` (ADR 0007).
//
// Everything is stored in Duct-owned tables (duct.duct_users, duct_roles,
// duct_grants, ...) and reached through a contract.Engine. Grants are Duct's
// own concept: they are never DuckDB GRANTs. Instead, for every role Duct
// materialises a schema role_<name> in the lake with one view per granted
// relation; the view applies the role's row filter and column masks. Every
// protocol runs queries against those views only (see Handler).
package governance
