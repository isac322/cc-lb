#!/usr/bin/env bun
/** Execute fixture SQLite plans inside the project-pinned Bun container. */
import { Database, type SQLQueryBindings } from 'bun:sqlite';

const input = await new Response(Bun.stdin.stream()).text();
const request = JSON.parse(input) as {
  operation: 'seed' | 'snapshot';
  database: string;
  required_tables?: string[];
  statements?: Array<{ sql: string; params?: unknown[] }>;
  queries?: Array<{ name: string; sql: string; params?: unknown[] }>;
};

function decodeBinding(value: unknown): SQLQueryBindings {
  if (value === null) return null;
  if (typeof value === 'string' || typeof value === 'number') return value;
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const typed = value as { type?: unknown; base64?: unknown };
    if (typed.type === 'blob' && typeof typed.base64 === 'string') {
      return Uint8Array.from(Buffer.from(typed.base64, 'base64'));
    }
  }
  throw new Error(`unsupported SQLite binding type: ${typeof value}`);
}

function decodeBindings(values: unknown[]): SQLQueryBindings[] {
  return values.map(decodeBinding);
}

const db = new Database(request.database);
try {
  db.exec('PRAGMA busy_timeout=20000');
  if (request.operation === 'seed') {
    const missing = (request.required_tables ?? []).filter((table) => {
      return !db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table);
    });
    if (missing.length > 0) {
      throw new Error(`migrated SQLite schema missing: ${missing.join(', ')}`);
    }
    db.exec('BEGIN IMMEDIATE');
    try {
      for (const statement of request.statements ?? []) {
        const rawParams = statement.params ?? [];
        if (rawParams.length > 0 && Array.isArray(rawParams[0])) {
          for (const rawRow of rawParams) {
            if (!Array.isArray(rawRow)) throw new Error('SQLite bulk parameters must be arrays');
            db.query(statement.sql).run(...decodeBindings(rawRow));
          }
        } else {
          db.query(statement.sql).run(...decodeBindings(rawParams));
        }
      }
      db.exec('COMMIT');
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch { /* preserve the original error */ }
      throw error;
    }
    console.log(JSON.stringify({ ok: true }));
  } else {
    const rows: Record<string, unknown[]> = {};
    for (const query of request.queries ?? []) {
      rows[query.name] = db.query(query.sql).all(...decodeBindings(query.params ?? []));
    }
    console.log(JSON.stringify(rows));
  }
} finally {
  db.close();
}
