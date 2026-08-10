#!/usr/bin/env node
/**
 * Sync data/league-members.json into app.league_member.
 *
 * Idempotent and keyed on managerKey rather than email, so it can be run now
 * with blank emails and re-run later as addresses arrive, updating rows in place
 * instead of creating a second row per manager.
 *
 * Usage: DATABASE_URL=postgres://... node scripts/seed-league-members.js
 */

import fs from 'node:fs';
import postgres from 'postgres';

const url = process.env.DATABASE_URL;
if (!url) {
	console.error('DATABASE_URL is not set.');
	process.exit(1);
}

const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
const sql = postgres(url, { ssl: isLocal ? false : 'require', max: 1, prepare: false });

const { members } = JSON.parse(
	fs.readFileSync(new URL('../data/league-members.json', import.meta.url), 'utf8')
);

/** Mirrors slugifyManagerName in src/lib/server/members.ts. */
function slugify(name) {
	return name
		.toLowerCase()
		.normalize('NFKD')
		.replace(/[̀-ͯ]/g, '')
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

try {
	// Verify every key still points at the manager we think it does. dim_manager
	// is written by the Python ETL; if a run ever renumbers a key, every
	// historical vote silently repoints to the wrong person. Abort instead.
	const keys = members.map((m) => m.managerKey);
	const actual = await sql`
		select manager_key, manager_name from edw.dim_manager
		where manager_key in ${sql(keys)}`;

	const nameByKey = new Map(actual.map((r) => [r.manager_key, r.manager_name]));
	const mismatches = members.filter((m) => nameByKey.get(m.managerKey) !== m.managerName);

	if (mismatches.length > 0) {
		console.error('ABORTING — manager keys no longer match edw.dim_manager:');
		for (const m of mismatches) {
			console.error(
				`  key ${m.managerKey}: file says "${m.managerName}", database says "${nameByKey.get(m.managerKey) ?? '(missing)'}"`
			);
		}
		console.error('\nThe ETL may have renumbered. Fix data/league-members.json before re-running.');
		process.exit(1);
	}

	const withEmail = members.filter((m) => m.email?.trim());
	const withoutEmail = members.filter((m) => !m.email?.trim());

	for (const member of withEmail) {
		const email = member.email.toLowerCase().trim();
		await sql`
			insert into app.league_member (id, email, manager_key, role, active, display_name)
			values (${slugify(member.managerName)}, ${email}, ${member.managerKey},
			        ${member.role ?? 'member'}, true, ${member.managerName})
			on conflict (manager_key) do update set
				email = excluded.email,
				role = excluded.role,
				display_name = excluded.display_name,
				updated_at = now()`;
		console.log(`  ✓ ${member.managerName} <${email}> (${member.role ?? 'member'})`);
	}

	if (withoutEmail.length > 0) {
		console.log(`\nPending — no email yet, so not seeded (${withoutEmail.length}):`);
		for (const m of withoutEmail) console.log(`  · ${m.managerName} (key ${m.managerKey})`);
		console.log('\nThese managers cannot sign in until an address is filled in.');
	}

	const [{ count }] = await sql`select count(*)::int as count from app.league_member`;
	console.log(`\napp.league_member now holds ${count} of ${members.length} managers.`);
} finally {
	await sql.end();
}
