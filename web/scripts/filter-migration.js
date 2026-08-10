#!/usr/bin/env node
/**
 * Strip pipeline-owned DDL from a freshly generated migration.
 *
 * Why this exists: schema.ts declares the `edw.*` tables so the app can query
 * the warehouse, but those tables are built and owned by the Python ETL
 * (src/edw_schema/). drizzle-kit's `schemaFilter: ['app']` only constrains
 * `push` and `introspect` — `generate` renders every table in the schema file,
 * so it happily emits `CREATE TABLE "edw"."dim_manager"`. Running that against
 * production would at best fail and at worst clobber 20 years of league data.
 *
 * This removes every statement that is not against the `app` schema, then
 * asserts nothing pipeline-owned survived. Run automatically by `npm run
 * db:generate`.
 */

import fs from 'node:fs';
import path from 'node:path';

const MIGRATIONS_DIR = new URL('../drizzle/', import.meta.url);
const BREAKPOINT = '--> statement-breakpoint';

/**
 * Statements whose *target* is a pipeline-owned schema.
 *
 * Anchored on what the statement acts upon, not on any mention of the schema:
 * `ALTER TABLE "app"."league_member" ... REFERENCES "edw"."dim_manager"` is a
 * foreign key we want and must survive, while `CREATE TABLE "edw"."dim_manager"`
 * must not.
 */
const FOREIGN_TARGET =
	/^\s*(?:CREATE|ALTER|DROP)\s+(?:TABLE|INDEX|VIEW|MATERIALIZED\s+VIEW)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"(edw|public|meta_data)"/i;

const FOREIGN_SCHEMA_DDL = /^\s*(?:CREATE|DROP)\s+SCHEMA\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"(edw|public|meta_data)"/i;

function targetsForeignSchema(statement) {
	return FOREIGN_TARGET.test(statement) || FOREIGN_SCHEMA_DDL.test(statement);
}

const target = process.argv[2]
	? path.resolve(process.argv[2])
	: fs
			.readdirSync(MIGRATIONS_DIR)
			.filter((f) => f.endsWith('.sql'))
			.sort()
			.map((f) => path.join(MIGRATIONS_DIR.pathname, f))
			.pop();

if (!target || !fs.existsSync(target)) {
	console.error('No migration file found to filter.');
	process.exit(1);
}

const original = fs.readFileSync(target, 'utf8');
const statements = original.split(BREAKPOINT).map((s) => s.trim()).filter(Boolean);

const kept = [];
const dropped = [];

for (const statement of statements) {
	// `CREATE SCHEMA "edw"` and every edw/public table, index and view go.
	// Foreign keys declared on app tables that point at edw are kept.
	if (targetsForeignSchema(statement)) {
		dropped.push(statement.split('\n')[0]);
		continue;
	}
	kept.push(statement);
}

if (kept.length === 0) {
	console.error('Filtering removed every statement — refusing to write an empty migration.');
	process.exit(1);
}

const filtered = kept.join(`\n${BREAKPOINT}\n`) + '\n';

// Belt and braces: if anything pipeline-owned survived the filter, fail loudly
// rather than write a migration that could touch edw.
for (const statement of kept) {
	if (targetsForeignSchema(statement)) {
		console.error('A pipeline-owned statement survived filtering. Refusing to write.');
		process.exit(1);
	}
}

fs.writeFileSync(target, filtered);

console.log(`Filtered ${path.basename(target)}: kept ${kept.length}, dropped ${dropped.length}.`);
for (const line of dropped) console.log(`  dropped: ${line}`);
