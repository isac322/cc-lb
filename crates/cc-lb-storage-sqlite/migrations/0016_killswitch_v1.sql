CREATE TABLE IF NOT EXISTS killswitch_v1 (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    reason TEXT
);

INSERT INTO killswitch_v1 (id, enabled, reason)
VALUES (
    1,
    COALESCE((SELECT CASE value WHEN 'true' THEN 1 ELSE 0 END FROM meta_v1 WHERE key = 'killswitch_enabled'), 0),
    NULL
)
ON CONFLICT(id) DO NOTHING;
