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

const FOREIGN_SCHEMAS = String.raw`edw|public|meta_data`;

/**
 * The schema a statement acts *on*.
 *
 * Anchoring on the object type alone is not enough: drizzle emits the index
 * name before the table, so `CREATE INDEX "foo" ON "edw"."dim_manager"` has its
 * target after the ON, not after the object type. An earlier version of this
 * check missed exactly that, and — worse — the post-filter assertion reused the
 * same predicate, so a statement it could not see was also a statement it could
 * not catch. The verifier below is deliberately independent of this one.
 */
const TARGET_PATTERNS = [
	// CREATE/DROP SCHEMA "edw"
	new RegExp(String.raw`^\s*(?:CREATE|DROP)\s+SCHEMA\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"(${FOREIGN_SCHEMAS})"`, 'i'),
	// CREATE/ALTER/DROP TABLE|VIEW|MATERIALIZED VIEW|TYPE|SEQUENCE "edw"."x"
	new RegExp(String.raw`^\s*(?:CREATE|ALTER|DROP)\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW|TYPE|SEQUENCE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"(${FOREIGN_SCHEMAS})"`, 'i'),
	// CREATE [UNIQUE] INDEX "name" ON "edw"."x" — target follows ON
	new RegExp(String.raw`^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+[^;]*?\bON\s+"(${FOREIGN_SCHEMAS})"`, 'i'),
	// COMMENT ON ... "edw"."x"
	new RegExp(String.raw`^\s*COMMENT\s+ON\s+[^;]*?"(${FOREIGN_SCHEMAS})"`, 'i')
];

function targetsForeignSchema(statement) {
	return TARGET_PATTERNS.some((re) => re.test(statement));
}

/**
 * Independent verifier — deliberately NOT the filter's own predicate.
 *
 * A foreign schema may legitimately appear in exactly one place in a kept
 * statement: the REFERENCES clause of a foreign key declared on an app table
 * (app.user.manager_key -> edw.dim_manager). Anything else mentioning a
 * pipeline-owned schema is treated as suspect, whether or not the filter above
 * recognised its shape. This catches statement forms nobody anticipated.
 */
function mentionsForeignSchemaOutsideReferences(statement) {
	const withoutReferences = statement.replace(
		new RegExp(String.raw`REFERENCES\s+"(?:${FOREIGN_SCHEMAS})"\."[^"]+"\s*\([^)]*\)`, 'gi'),
		''
	);
	return new RegExp(String.raw`"(?:${FOREIGN_SCHEMAS})"`).test(withoutReferences);
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
	if (mentionsForeignSchemaOutsideReferences(statement)) {
		console.error('A pipeline-owned schema survived filtering, outside a REFERENCES clause:');
		console.error(`  ${statement.split('\n')[0]}`);
		console.error('Refusing to write. Widen TARGET_PATTERNS to cover this statement shape.');
		process.exit(1);
	}
}

fs.writeFileSync(target, filtered);

console.log(`Filtered ${path.basename(target)}: kept ${kept.length}, dropped ${dropped.length}.`);
for (const line of dropped) console.log(`  dropped: ${line}`);
