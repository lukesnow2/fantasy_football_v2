import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import postgres from 'postgres';

/**
 * The chat SQL, against a real Postgres.
 *
 * Skipped unless TEST_DATABASE_URL is set, so `npm run test:unit` never needs a
 * database. Run against a scratch copy of the schema:
 *
 *   TEST_DATABASE_URL=postgres://you@localhost:5432/the_league npx vitest run \
 *     --project=server src/lib/server/chat/queries.integration.test.ts
 *
 * These exercise the statements directly rather than through the query module,
 * because the module imports `$lib/server/db`, which binds to DATABASE_URL at
 * import time. The statements below are copies of the interesting shapes — if
 * one drifts from queries.ts, that drift is the thing worth catching.
 */

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;

describe.skipIf(!TEST_DATABASE_URL)('chat SQL', () => {
	let sql: postgres.Sql;
	let managerA = 0;
	let managerB = 0;
	const created: number[] = [];

	beforeAll(async () => {
		sql = postgres(TEST_DATABASE_URL!, { max: 1, onnotice: () => {} });
		const members = await sql<{ managerKey: number }[]>`
			SELECT manager_key AS "managerKey" FROM app.league_member ORDER BY manager_key LIMIT 2
		`;
		if (members.length < 1) throw new Error('Seed at least one league member first.');
		managerA = members[0].managerKey;
		managerB = members[1]?.managerKey ?? members[0].managerKey;
	});

	afterEach(async () => {
		// Reactions cascade from the message delete.
		if (created.length > 0) {
			await sql`DELETE FROM app.chat_message WHERE message_key = ANY(${created})`;
			created.length = 0;
		}
	});

	afterAll(async () => {
		await sql?.end();
	});

	async function insertMessage(content: string, parent: number | null = null): Promise<number> {
		const id = `m_${Math.random().toString(36).slice(2).padEnd(21, 'x').slice(0, 21)}`;
		const [row] = await sql<{ messageKey: number }[]>`
			INSERT INTO app.chat_message (message_id, content, author_key, channel_id, parent_message_key)
			VALUES (${id}, ${content}, ${managerA}, 'general', ${parent})
			RETURNING message_key AS "messageKey"
		`;
		created.push(row.messageKey);
		return row.messageKey;
	}

	async function reactionsFor(messageKey: number, me: number) {
		const [row] = await sql<{ reactions: unknown }[]>`
			SELECT COALESCE(rx.reactions, '[]'::json) AS reactions
			FROM app.chat_message m
			LEFT JOIN LATERAL (
				SELECT json_agg(
					json_build_object('emoji', g.emoji, 'emojiType', g.emoji_type,
					                  'count', g.cnt, 'mine', g.mine, 'users', g.users)
					ORDER BY g.first_at, g.emoji
				) AS reactions
				FROM (
					SELECT cr.emoji AS emoji, MIN(cr.emoji_type) AS emoji_type,
					       COUNT(*)::int AS cnt, MIN(cr.created_at) AS first_at,
					       COALESCE(BOOL_OR(cr.author_key = ${me}), false) AS mine,
					       json_agg(json_build_object('managerKey', cr.author_key,
					         'name', COALESCE(rm.display_name, rm.manager_name, 'Unknown'))
					         ORDER BY cr.created_at) AS users
					FROM app.chat_reaction cr
					LEFT JOIN edw.dim_manager rm ON rm.manager_key = cr.author_key
					WHERE cr.message_key = m.message_key
					GROUP BY cr.emoji
				) g
			) rx ON TRUE
			WHERE m.message_key = ${messageKey}
		`;
		return row.reactions as { emoji: string; count: number; mine: boolean; users: unknown[] }[];
	}

	async function toggle(messageKey: number, me: number, emoji: string) {
		const [row] = await sql<{ added: boolean }[]>`
			WITH target AS (
				SELECT message_key FROM app.chat_message
				WHERE message_key = ${messageKey} AND deleted_at IS NULL
			),
			del AS (
				DELETE FROM app.chat_reaction
				WHERE message_key = (SELECT message_key FROM target)
				  AND author_key = ${me} AND emoji = ${emoji}
				RETURNING 1
			),
			ins AS (
				INSERT INTO app.chat_reaction (message_key, author_key, emoji, emoji_type)
				SELECT (SELECT message_key FROM target), ${me}, ${emoji}, 'unicode'
				WHERE EXISTS (SELECT 1 FROM target) AND NOT EXISTS (SELECT 1 FROM del)
				ON CONFLICT (message_key, author_key, emoji) DO NOTHING
				RETURNING 1
			)
			SELECT (SELECT count(*) FROM ins)::int > 0 AS added
		`;
		return row.added;
	}

	it('returns an empty array, not null, when nothing has reacted', async () => {
		const key = await insertMessage('no reactions');
		expect(await reactionsFor(key, managerA)).toEqual([]);
	});

	it('groups one emoji across reactors and flags only the caller as mine', async () => {
		const key = await insertMessage('grouped');
		await toggle(key, managerA, '🔥');
		if (managerB !== managerA) await toggle(key, managerB, '🔥');

		const asA = await reactionsFor(key, managerA);
		expect(asA).toHaveLength(1);
		expect(asA[0].emoji).toBe('🔥');
		expect(asA[0].count).toBe(managerB === managerA ? 1 : 2);
		expect(asA[0].mine).toBe(true);
		expect(asA[0].users).toHaveLength(asA[0].count);

		if (managerB !== managerA) {
			// A manager who did not react must not see the pill as theirs.
			const asNobody = await reactionsFor(key, -1);
			expect(asNobody[0].mine).toBe(false);
		}
	});

	it('orders distinct emoji by when each was first used', async () => {
		const key = await insertMessage('ordered');
		await toggle(key, managerA, '🔥');
		await new Promise((r) => setTimeout(r, 5));
		await toggle(key, managerA, '👍');

		const groups = await reactionsFor(key, managerA);
		expect(groups.map((g) => g.emoji)).toEqual(['🔥', '👍']);
	});

	it('toggles off on a second call and leaves no row behind', async () => {
		const key = await insertMessage('toggling');
		expect(await toggle(key, managerA, '🔥')).toBe(true);
		expect(await toggle(key, managerA, '🔥')).toBe(false);

		const [{ count }] = await sql<{ count: number }[]>`
			SELECT count(*)::int AS count FROM app.chat_reaction WHERE message_key = ${key}
		`;
		expect(count).toBe(0);
	});

	it('hides a soft-deleted message from the root list but keeps the tombstone', async () => {
		const key = await insertMessage('doomed');
		await sql`UPDATE app.chat_message SET deleted_at = now(), updated_at = now() WHERE message_key = ${key}`;

		const roots = await sql<{ messageKey: number }[]>`
			SELECT message_key AS "messageKey" FROM app.chat_message
			WHERE channel_id = 'general' AND parent_message_key IS NULL AND deleted_at IS NULL
			  AND message_key = ${key}
		`;
		expect(roots).toHaveLength(0);

		const tombstone = await sql<{ deletedAt: Date | null }[]>`
			SELECT deleted_at AS "deletedAt" FROM app.chat_message WHERE message_key = ${key}
		`;
		expect(tombstone[0].deletedAt).not.toBeNull();
	});

	it('refuses a reply whose parent is itself a reply', async () => {
		const root = await insertMessage('root');
		const reply = await insertMessage('reply', root);

		// The guard in the real INSERT: parent must have no parent of its own.
		const rows = await sql`
			INSERT INTO app.chat_message (message_id, content, author_key, channel_id, parent_message_key)
			SELECT 'm_nestedxxxxxxxxxxxxxx', 'nested', ${managerA}, 'general', ${reply}
			WHERE EXISTS (
				SELECT 1 FROM app.chat_message p
				WHERE p.message_key = ${reply} AND p.channel_id = 'general'
				  AND p.parent_message_key IS NULL AND p.deleted_at IS NULL
			)
			RETURNING message_key
		`;
		expect(rows).toHaveLength(0);
	});

	it('cascades reactions when a message is really deleted', async () => {
		const key = await insertMessage('cascade');
		await toggle(key, managerA, '🔥');
		await sql`DELETE FROM app.chat_message WHERE message_key = ${key}`;
		created.pop();

		const [{ count }] = await sql<{ count: number }[]>`
			SELECT count(*)::int AS count FROM app.chat_reaction WHERE message_key = ${key}
		`;
		expect(count).toBe(0);
	});

	it('counts replies and reports the latest, ignoring deleted ones', async () => {
		const root = await insertMessage('has replies');
		await insertMessage('one', root);
		const second = await insertMessage('two', root);
		await sql`UPDATE app.chat_message SET deleted_at = now() WHERE message_key = ${second}`;

		const [row] = await sql<{ replyCount: number; lastReplyAt: string | null }[]>`
			SELECT rp.reply_count AS "replyCount", rp.last_reply_at AS "lastReplyAt"
			FROM app.chat_message m
			LEFT JOIN LATERAL (
				SELECT COUNT(*)::int AS reply_count,
				       (EXTRACT(EPOCH FROM MAX(c.created_at)) * 1000)::bigint AS last_reply_at
				FROM app.chat_message c
				WHERE c.parent_message_key = m.message_key AND c.deleted_at IS NULL
			) rp ON TRUE
			WHERE m.message_key = ${root}
		`;
		expect(Number(row.replyCount)).toBe(1);
		expect(row.lastReplyAt).not.toBeNull();
	});

	it('advances the read watermark but never rewinds it', async () => {
		await sql`
			INSERT INTO app.chat_read (manager_key, channel_id, last_read_message_key, last_read_at, updated_at)
			VALUES (${managerA}, 'general', 500, now(), now())
			ON CONFLICT (manager_key, channel_id) DO UPDATE
			SET last_read_message_key = GREATEST(app.chat_read.last_read_message_key, EXCLUDED.last_read_message_key)
		`;
		// A stale beacon from a background tab must not undo a foreground advance.
		await sql`
			INSERT INTO app.chat_read (manager_key, channel_id, last_read_message_key, last_read_at, updated_at)
			VALUES (${managerA}, 'general', 100, now(), now())
			ON CONFLICT (manager_key, channel_id) DO UPDATE
			SET last_read_message_key = GREATEST(app.chat_read.last_read_message_key, EXCLUDED.last_read_message_key)
		`;

		const [row] = await sql<{ key: number }[]>`
			SELECT last_read_message_key AS key FROM app.chat_read
			WHERE manager_key = ${managerA} AND channel_id = 'general'
		`;
		expect(row.key).toBe(500);
		await sql`DELETE FROM app.chat_read WHERE manager_key = ${managerA} AND channel_id = 'general'`;
	});
});

describe.skipIf(!TEST_DATABASE_URL)('power rankings SQL', () => {
	let sql: postgres.Sql;

	beforeAll(() => {
		sql = postgres(TEST_DATABASE_URL!, { max: 1, onnotice: () => {} });
	});

	afterAll(async () => {
		await sql?.end();
	});

	it('computes movement with LAG, because the mart column is always zero', async () => {
		const [stale] = await sql<{ total: number }[]>`
			SELECT COALESCE(SUM(ABS(rank_change)), 0)::int AS total FROM edw.mart_weekly_power_rankings
		`;
		// If this ever stops being zero the ETL started populating it, and the
		// LAG in the API becomes a duplicate worth removing.
		expect(stale.total).toBe(0);

		const rows = await sql<{ weekNumber: number; rankChange: number | null }[]>`
			WITH ranked AS (
				SELECT w.week_number AS "weekNumber", p.team_key,
				       p.power_rank,
				       LAG(p.power_rank) OVER (PARTITION BY p.team_key ORDER BY w.week_number) AS "prevRank"
				FROM edw.mart_weekly_power_rankings p
				JOIN edw.dim_week w   ON w.week_key = p.week_key
				JOIN edw.dim_league l ON l.league_key = p.league_key
				WHERE w.season_year = 2025 AND l.season_year = 2025
			)
			SELECT "weekNumber", ("prevRank" - power_rank) AS "rankChange" FROM ranked
			ORDER BY "weekNumber"
		`;

		expect(rows.length).toBeGreaterThan(0);
		// Week 1 has no previous week, so movement is unknown, not zero.
		expect(rows.filter((r) => r.weekNumber === 1).every((r) => r.rankChange === null)).toBe(true);
		// And later weeks actually move.
		expect(rows.some((r) => r.weekNumber > 1 && Number(r.rankChange) !== 0)).toBe(true);
	});

	it('resolves teams to managers by name, since dim_team.manager_key is empty', async () => {
		const [row] = await sql<{ teams: number; withKey: number; withName: number }[]>`
			SELECT count(*)::int AS teams,
			       count(manager_key)::int AS "withKey",
			       count(manager_name)::int AS "withName"
			FROM edw.dim_team WHERE league_key = (
				SELECT league_key FROM edw.dim_league WHERE season_year = 2025
			)
		`;
		expect(row.withKey).toBe(0);
		expect(row.withName).toBe(row.teams);
	});
});
