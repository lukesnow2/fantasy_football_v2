// Read-only production inspection. Answers the two questions the migration
// plan is gated on: is app.* empty (so a baseline reset is safe), and what are
// the real manager keys?
//
// Usage: node scripts/inspect-prod-state.js
// Reads DATABASE_URL from .env.production.local.

import fs from 'node:fs';
import postgres from 'postgres';

const raw = fs.readFileSync(new URL('../.env.production.local', import.meta.url), 'utf8');
const url = raw.match(/DATABASE_URL=(.*)/)?.[1]?.trim();
if (!url) throw new Error('DATABASE_URL not found in .env.production.local');

const sql = postgres(url, { ssl: 'require', max: 1, prepare: false });

try {
	const tables = await sql`
		select table_name from information_schema.tables
		where table_schema = 'app' order by 1`;

	console.log(`APP TABLES (${tables.length}):`);
	let totalRows = 0;
	for (const { table_name: name } of tables) {
		const [{ n }] = await sql.unsafe(`select count(*)::int n from app."${name}"`);
		totalRows += n;
		console.log(`  ${n === 0 ? ' ' : '!'} ${name} = ${n}`);
	}
	console.log(`\nTOTAL ROWS IN app.*: ${totalRows}`);

	const managers = await sql`
		select manager_key, manager_name from edw.dim_manager
		where is_current and is_active order by manager_key`;
	console.log(`\nACTIVE + CURRENT MANAGERS (${managers.length}):`);
	for (const m of managers) console.log(`  ${m.manager_key} = ${m.manager_name}`);

	const [mk1] = await sql`
		select manager_key, manager_name, is_current, is_active
		from edw.dim_manager where manager_key = 1`;
	console.log(`\nMANAGER_KEY 1: ${JSON.stringify(mk1 ?? null)}`);

	const cols = await sql`
		select table_name, column_name from information_schema.columns
		where table_schema = 'app'
		  and table_name in ('rule_proposal', 'rule_amendment')
		  and column_name in ('manager_key', 'submitted_by', 'approved_by')
		order by 1, 2`;
	console.log(
		`\nATTRIBUTION COLUMNS: ${cols.map((c) => `${c.table_name}.${c.column_name}`).join(', ') || '(none)'}`
	);
} finally {
	await sql.end();
}
