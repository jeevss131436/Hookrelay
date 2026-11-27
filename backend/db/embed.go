// Package db holds the hand-written SQL: goose migrations (migrations/) and
// sqlc query definitions (queries/). Migrations are embedded into the binary
// so `hookrelay migrate` needs no files on disk.
package db

import "embed"

// Migrations contains migrations/*.sql.
//
//go:embed migrations/*.sql
var Migrations embed.FS
