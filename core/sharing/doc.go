// Package sharing implements data sharing (ADR 0014): shares of lake tables
// granted to recipients, over the OpenSharing / Delta Sharing protocol.
//
// A recipient is an internal role "share_<name>" with a user of the same name
// and a bearer token (phase 1: it can already query its views over pg or
// http). Its row filters and masks are the ones of governance, so the
// materialized copy it downloads (phase 2) is exactly what its views show.
//
// The Store keeps the model in the Duct tables; the Service materializes one
// compacted Parquet copy per (share, recipient, table) on a Storage (a local
// folder or S3) and refreshes it when the lake snapshot moves. The REST layer
// lives in modules/sharing-server.
package sharing
