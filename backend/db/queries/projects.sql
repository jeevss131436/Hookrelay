-- name: CreateProject :one
INSERT INTO projects (name)
VALUES (@name)
RETURNING *;

-- A project is the tenant itself, so lookup by its own id is the scope.
-- name: GetProject :one
SELECT * FROM projects
WHERE id = @id;
