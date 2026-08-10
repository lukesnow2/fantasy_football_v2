#!/usr/bin/env node
/**
 * Prove the database refuses to attribute anything to a non-member.
 *
 * This is the test that fails if someone later drops the foreign keys on
 * rule_proposal.submitted_by / rule_vote.manager_key / chat_message.author_key.
 * Those FKs point at app.league_member — the ten people who can sign in — rather
 * than at edw.dim_manager, which holds every manager across 20 years including
 * manager_key 1, "-- hidden --", the value the old `|| 1` fallback wrote.
 *
 * Usage: DATABASE_URL=postgres://... node scripts/verify-attribution.js
 */

import postgres from 'postgres';

const url = process.env.DATABASE_URL;
if (!url) {
	console.error('DATABASE_URL is not set.');
	process.exit(1);
}

const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
const sql = postgres(url, { ssl: isLocal ? false : 'require', max: 1, prepare: false });

let failures = 0;

function check(name, passed, detail = '') {
	console.log(`  ${passed ? '✓' : '✗'} ${name}${detail ? ` — ${detail}` : ''}`);
	if (!passed) failures++;
}

async function rejects(label, fn) {
	try {
		await fn();
		check(label, false, 'the insert was ACCEPTED');
	} catch (error) {
		const isFk = /violates foreign key constraint/i.test(error.message);
		check(label, isFk, isFk ? '' : `rejected for the wrong reason: ${error.message}`);
	}
}

try {
	const [hidden] = await sql`
		select manager_key, manager_name, is_current from edw.dim_manager where manager_key = 1`;
	console.log(
		`\nmanager_key 1 in edw.dim_manager: "${hidden?.manager_name}" (is_current=${hidden?.is_current})`
	);

	const [inAllowlist] = await sql`
		select 1 from app.league_member where manager_key = 1`;
	check('manager_key 1 is NOT on the league allowlist', !inAllowlist);

	console.log('\nThe database must refuse every one of these:');

	await rejects('rule_vote attributed to manager_key 1', async () => {
		await sql`insert into app.rule_vote (proposal_key, manager_key, vote)
		          values (999999, 1, 'yes')`;
	});

	await rejects('rule_proposal submitted_by manager_key 1', async () => {
		await sql`insert into app.rule_proposal
			(proposal_id, title, description, proposal_type, category, proposed_language,
			 rationale, effective_season, submitted_by, required_votes, eligible_voters)
			values ('verify-attribution', 't', 'd', 'edit_clause', 'general', 'x', 'y',
			        2027, 1, 6, 10)`;
	});

	await rejects('chat_message authored by manager_key 1', async () => {
		await sql`insert into app.chat_message (message_id, content, author_key)
		          values ('verify-attribution', 'hello', 1)`;
	});

	// The FKs must be real constraints, not just a convention in the ORM.
	const fks = await sql`
		select tc.constraint_name, tc.table_name
		from information_schema.table_constraints tc
		join information_schema.constraint_column_usage ccu
		  on tc.constraint_name = ccu.constraint_name
		where tc.constraint_type = 'FOREIGN KEY'
		  and tc.table_schema = 'app'
		  and ccu.table_name = 'league_member'
		  and ccu.column_name = 'manager_key'
		order by tc.table_name`;

	console.log(`\nForeign keys pointing at league_member.manager_key (${fks.length}):`);
	for (const fk of fks) console.log(`  · ${fk.table_name}`);

	const expected = ['chat_message', 'rule_amendment', 'rule_proposal', 'rule_proposal', 'rule_vote'];
	check(
		'every attribution column is constrained',
		expected.every((t) => fks.some((fk) => fk.table_name === t)),
		`expected ${[...new Set(expected)].join(', ')}`
	);
} finally {
	await sql.end();
}

console.log(failures === 0 ? '\nAll attribution checks passed.' : `\n${failures} check(s) FAILED.`);
process.exit(failures === 0 ? 0 : 1);
