CREATE TABLE IF NOT EXISTS killswitch_v1 (
    id INT PRIMARY KEY CHECK (id = 1),
    enabled BOOL NOT NULL,
    reason TEXT
);
INSERT INTO killswitch_v1 (id, enabled, reason) VALUES (1, false, NULL) ON CONFLICT DO NOTHING;
