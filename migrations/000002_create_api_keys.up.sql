CREATE TABLE api_keys (
    id UUID PRIMARY KEY,
    client_name TEXT NOT NULL,
    hashed_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
