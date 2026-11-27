-- name: CreateAPIKey :one
INSERT INTO api_keys (project_id, name, prefix, key_hash)
VALUES (@project_id, @name, @prefix, @key_hash)
RETURNING id, project_id, name, prefix, created_at;

-- Authentication entry point: resolves a presented key's hash to its project.
-- This is the one deliberately unscoped lookup, because it is how the
-- request's project_id is determined. Revoked keys never match.
-- name: GetActiveAPIKeyByHash :one
SELECT id, project_id, name, prefix
FROM api_keys
WHERE key_hash = @key_hash
  AND revoked_at IS NULL;

-- name: ListAPIKeys :many
SELECT id, project_id, name, prefix, created_at, last_used_at, revoked_at
FROM api_keys
WHERE project_id = @project_id
ORDER BY created_at DESC, id DESC;

-- name: RevokeAPIKey :execrows
UPDATE api_keys
SET revoked_at = now()
WHERE project_id = @project_id
  AND id = @id
  AND revoked_at IS NULL;
