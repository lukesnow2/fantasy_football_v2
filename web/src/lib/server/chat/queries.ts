import { sql, type SQL } from 'drizzle-orm';
import { db } from '$lib/server/db';
import type {
	ChatCursor,
	ChatMessageDTO,
	ReactionGroup,
	RosterMember
} from '$lib/chat/types';

/**
 * Every chat SQL statement, in one place and individually callable so the
 * interesting ones can be tested against a real Postgres.
 *
 * Two conventions here that are load-bearing:
 *
 *  - Column aliases are quoted camelCase. The postgres.js client is configured
 *    with `transform: postgres.camel`, which rewrites *column names* but never
 *    the contents of a json value. Aliasing explicitly means the bootstrap and
 *    delta payloads have identical shapes whether a field arrives as a column
 *    or inside a json_agg, and nothing depends on knowing that rule.
 *  - Timestamps leave as epoch milliseconds. The client does arithmetic on them
 *    (grouping windows, relative times) and never has to parse or guess a zone.
 *
 * Statements are parameterized `sql` templates, never `sql.raw` with an
 * interpolated string.
 */

const MESSAGE_COLUMNS = sql`
	m.message_key                                        AS "messageKey",
	m.message_id                                         AS "messageId",
	m.content                                            AS "content",
	m.channel_id                                         AS "channelId",
	m.parent_message_key                                 AS "parentMessageKey",
	COALESCE(m.message_type, 'message')                  AS "messageType",
	m.author_key                                         AS "authorKey",
	dm.manager_name                                      AS "authorName",
	COALESCE(dm.display_name, dm.manager_name, lm.display_name, 'Unknown') AS "authorDisplayName",
	dm.profile_image_url                                 AS "authorProfileImage",
	m.mentions                                           AS "mentionsRaw",
	(EXTRACT(EPOCH FROM m.created_at) * 1000)::bigint    AS "createdAt",
	(EXTRACT(EPOCH FROM m.updated_at) * 1000)::bigint    AS "updatedAt",
	(EXTRACT(EPOCH FROM m.edited_at)  * 1000)::bigint    AS "editedAt",
	(EXTRACT(EPOCH FROM m.deleted_at) * 1000)::bigint    AS "deletedAt"
`;

const AUTHOR_JOINS = sql`
	LEFT JOIN edw.dim_manager dm  ON dm.manager_key = m.author_key
	LEFT JOIN app.league_member lm ON lm.manager_key = m.author_key
`;

/**
 * Grouped reactions for one message, as a single json column.
 *
 * A LATERAL subquery rather than a top-level GROUP BY: the unique index on
 * (message_key, author_key, emoji) makes the correlated lookup an index scan,
 * and it keeps message cardinality at exactly one row per message — a GROUP BY
 * join would force every selected message column into the grouping key.
 *
 * The json keys are written in their final camelCase form deliberately; see the
 * note at the top of the file.
 */
function reactionsLateral(me: number, alias: string): SQL {
	return sql`
		LEFT JOIN LATERAL (
			SELECT json_agg(
				json_build_object(
					'emoji',     g.emoji,
					'emojiType', g.emoji_type,
					'count',     g.cnt,
					'mine',      g.mine,
					'users',     g.users
				) ORDER BY g.first_at, g.emoji
			) AS reactions
			FROM (
				SELECT cr.emoji                                        AS emoji,
				       MIN(cr.emoji_type)                              AS emoji_type,
				       COUNT(*)::int                                   AS cnt,
				       MIN(cr.created_at)                              AS first_at,
				       COALESCE(BOOL_OR(cr.author_key = ${me}), false)  AS mine,
				       json_agg(
				         json_build_object(
				           'managerKey', cr.author_key,
				           'name', COALESCE(rm.display_name, rm.manager_name, 'Unknown')
				         ) ORDER BY cr.created_at
				       )                                               AS users
				FROM app.chat_reaction cr
				LEFT JOIN edw.dim_manager rm ON rm.manager_key = cr.author_key
				WHERE cr.message_key = m.message_key
				GROUP BY cr.emoji
			) g
		) ${sql.identifier(alias)} ON TRUE
	`;
}

/** Reply count and most recent reply for a root message. */
const REPLIES_LATERAL = sql`
	LEFT JOIN LATERAL (
		SELECT COUNT(*)::int                                           AS reply_count,
		       (EXTRACT(EPOCH FROM MAX(c.created_at)) * 1000)::bigint  AS last_reply_at
		FROM app.chat_message c
		WHERE c.parent_message_key = m.message_key AND c.deleted_at IS NULL
	) rp ON TRUE
`;

type RawRow = Record<string, unknown>;

function toNumber(value: unknown): number {
	return typeof value === 'number' ? value : Number(value ?? 0);
}

function toNullableNumber(value: unknown): number | null {
	if (value === null || value === undefined) return null;
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function parseMentions(raw: unknown): number[] {
	if (typeof raw !== 'string' || raw === '') return [];
	try {
		const parsed = JSON.parse(raw);
		return Array.isArray(parsed) ? parsed.map(Number).filter(Number.isFinite) : [];
	} catch {
		// A malformed blob is not worth failing a whole message render over.
		return [];
	}
}

function toDto(row: RawRow): ChatMessageDTO {
	return {
		messageKey: toNumber(row.messageKey),
		messageId: String(row.messageId),
		content: String(row.content ?? ''),
		channelId: String(row.channelId),
		parentMessageKey: toNullableNumber(row.parentMessageKey),
		messageType: String(row.messageType ?? 'message'),
		authorKey: toNumber(row.authorKey),
		authorName: row.authorName == null ? null : String(row.authorName),
		authorDisplayName: String(row.authorDisplayName ?? 'Unknown'),
		authorProfileImage: row.authorProfileImage == null ? null : String(row.authorProfileImage),
		mentions: parseMentions(row.mentionsRaw),
		createdAt: toNumber(row.createdAt),
		updatedAt: toNumber(row.updatedAt),
		editedAt: toNullableNumber(row.editedAt),
		deletedAt: toNullableNumber(row.deletedAt),
		reactions: (row.reactions as ReactionGroup[] | null) ?? [],
		replyCount: toNumber(row.replyCount),
		lastReplyAt: toNullableNumber(row.lastReplyAt)
	};
}

/**
 * The newest root messages in a channel, oldest-first.
 *
 * Reactions ride along in the same statement. They previously did not, which is
 * why every reaction vanished on reload and only reappeared once you personally
 * clicked one.
 */
export async function selectRootMessages(opts: {
	channelId: string;
	me: number;
	limit: number;
	beforeKey?: number | null;
}): Promise<{ messages: ChatMessageDTO[]; hasMore: boolean }> {
	const { channelId, me, limit, beforeKey } = opts;

	// One extra row, discarded, purely to answer "is there more history?" without
	// a second COUNT over the channel.
	const rows = Array.from(
		await db.execute(sql`
			SELECT ${MESSAGE_COLUMNS},
			       COALESCE(rx.reactions, '[]'::json) AS "reactions",
			       COALESCE(rp.reply_count, 0)        AS "replyCount",
			       rp.last_reply_at                   AS "lastReplyAt"
			FROM app.chat_message m
			${AUTHOR_JOINS}
			${reactionsLateral(me, 'rx')}
			${REPLIES_LATERAL}
			WHERE m.channel_id = ${channelId}
			  AND m.parent_message_key IS NULL
			  AND m.deleted_at IS NULL
			  ${beforeKey ? sql`AND m.message_key < ${beforeKey}` : sql``}
			ORDER BY m.message_key DESC
			LIMIT ${limit + 1}
		`)
	) as RawRow[];

	const hasMore = rows.length > limit;
	const page = hasMore ? rows.slice(0, limit) : rows;
	return { messages: page.map(toDto).reverse(), hasMore };
}

/** Replies to one root message, oldest-first. */
export async function selectReplies(opts: {
	channelId: string;
	me: number;
	parentMessageKey: number;
}): Promise<ChatMessageDTO[]> {
	const { channelId, me, parentMessageKey } = opts;

	const rows = Array.from(
		await db.execute(sql`
			SELECT ${MESSAGE_COLUMNS},
			       COALESCE(rx.reactions, '[]'::json) AS "reactions",
			       0                                  AS "replyCount",
			       NULL::bigint                       AS "lastReplyAt"
			FROM app.chat_message m
			${AUTHOR_JOINS}
			${reactionsLateral(me, 'rx')}
			WHERE m.channel_id = ${channelId}
			  AND m.parent_message_key = ${parentMessageKey}
			  AND m.deleted_at IS NULL
			ORDER BY m.message_key ASC
		`)
	) as RawRow[];

	return rows.map(toDto);
}

export interface DeltaResult {
	messages: ChatMessageDTO[];
	reactions: Record<string, ReactionGroup[]>;
	cursor: ChatCursor;
}

/**
 * Everything that changed since the caller's cursor.
 *
 * Matches on `message_key > sinceKey OR updated_at > sinceTs`, so edits and
 * soft-delete tombstones arrive alongside new messages rather than requiring a
 * reload.
 *
 * Reactions come back as *complete* state for the newest `window` root
 * messages, not as a delta. Removals are hard DELETEs with no tombstone, so no
 * cursor over chat_reaction could ever carry one; shipping full state makes
 * removals, edits and any dropped poll self-heal. At ten people and one channel
 * it is a few dozen mostly-empty arrays.
 *
 * `cursorTs` comes from now() inside the same statement, so it shares a snapshot
 * with the reads — that is what makes "no edit can slip between two polls" true.
 */
export async function selectDelta(opts: {
	channelId: string;
	me: number;
	sinceKey: number;
	sinceTs: number;
	window: number;
}): Promise<DeltaResult> {
	const { channelId, me, sinceKey, sinceTs, window } = opts;

	const changedRows = Array.from(
		await db.execute(sql`
			SELECT ${MESSAGE_COLUMNS},
			       '[]'::json                  AS "reactions",
			       COALESCE(rp.reply_count, 0) AS "replyCount",
			       rp.last_reply_at            AS "lastReplyAt"
			FROM app.chat_message m
			${AUTHOR_JOINS}
			${REPLIES_LATERAL}
			WHERE m.channel_id = ${channelId}
			  AND m.parent_message_key IS NULL
			  AND (
			    m.message_key > ${sinceKey}
			    OR m.updated_at > to_timestamp(${sinceTs}::double precision / 1000)
			  )
			ORDER BY m.message_key ASC
			LIMIT 200
		`)
	) as RawRow[];

	const reactionRows = Array.from(
		await db.execute(sql`
			WITH win AS (
				SELECT message_key
				FROM app.chat_message
				WHERE channel_id = ${channelId} AND parent_message_key IS NULL
				ORDER BY message_key DESC
				LIMIT ${window}
			)
			SELECT m.message_key                        AS "messageKey",
			       COALESCE(rx.reactions, '[]'::json)   AS "reactions"
			FROM app.chat_message m
			JOIN win ON win.message_key = m.message_key
			${reactionsLateral(me, 'rx')}
		`)
	) as RawRow[];

	const cursorRows = Array.from(
		await db.execute(sql`
			SELECT COALESCE(
			         (SELECT MAX(message_key) FROM app.chat_message WHERE channel_id = ${channelId}),
			         ${sinceKey}
			       )                                        AS "messageKey",
			       (EXTRACT(EPOCH FROM now()) * 1000)::bigint AS "ts"
		`)
	) as RawRow[];

	const reactions: Record<string, ReactionGroup[]> = {};
	for (const row of reactionRows) {
		reactions[String(toNumber(row.messageKey))] = (row.reactions as ReactionGroup[] | null) ?? [];
	}

	return {
		messages: changedRows.map(toDto),
		reactions,
		cursor: {
			messageKey: toNumber(cursorRows[0]?.messageKey ?? sinceKey),
			ts: toNumber(cursorRows[0]?.ts ?? Date.now())
		}
	};
}

/** One message by its public id, with reactions. Used after every write. */
export async function selectMessageById(opts: {
	channelId: string;
	me: number;
	messageId: string;
}): Promise<ChatMessageDTO | null> {
	const { channelId, me, messageId } = opts;

	const rows = Array.from(
		await db.execute(sql`
			SELECT ${MESSAGE_COLUMNS},
			       COALESCE(rx.reactions, '[]'::json) AS "reactions",
			       COALESCE(rp.reply_count, 0)        AS "replyCount",
			       rp.last_reply_at                   AS "lastReplyAt"
			FROM app.chat_message m
			${AUTHOR_JOINS}
			${reactionsLateral(me, 'rx')}
			${REPLIES_LATERAL}
			WHERE m.message_id = ${messageId} AND m.channel_id = ${channelId}
			LIMIT 1
		`)
	) as RawRow[];

	return rows[0] ? toDto(rows[0]) : null;
}

export async function selectReactionsForMessage(
	messageKey: number,
	me: number
): Promise<ReactionGroup[]> {
	const rows = Array.from(
		await db.execute(sql`
			SELECT COALESCE(rx.reactions, '[]'::json) AS "reactions"
			FROM app.chat_message m
			${reactionsLateral(me, 'rx')}
			WHERE m.message_key = ${messageKey}
			LIMIT 1
		`)
	) as RawRow[];

	return (rows[0]?.reactions as ReactionGroup[] | null) ?? [];
}

export type InsertOutcome =
	| { status: 'created'; messageId: string }
	| { status: 'duplicate'; messageId: string }
	| { status: 'bad-parent' };

/**
 * Insert a message the client has already named.
 *
 * `messageId` arrives from the client so the optimistic row and the row that
 * comes back from a poll share one identity — that is what makes the merge a
 * keyed replace instead of a heuristic. `message_id` is already UNIQUE, so a
 * retried send collides rather than double-posting, and 23505 is reported as a
 * duplicate rather than a failure.
 *
 * `parentMessageKey` must name a live root message in the same channel. The
 * INSERT ... SELECT ... WHERE EXISTS enforces one level of nesting in the
 * database, so a reply to a reply is impossible even if a client tries.
 */
export async function insertMessage(opts: {
	messageId: string;
	channelId: string;
	content: string;
	authorKey: number;
	parentMessageKey: number | null;
	mentions: number[];
}): Promise<InsertOutcome> {
	const { messageId, channelId, content, authorKey, parentMessageKey, mentions } = opts;
	const mentionsJson = mentions.length > 0 ? JSON.stringify(mentions) : null;

	try {
		const rows = Array.from(
			await db.execute(sql`
				INSERT INTO app.chat_message
					(message_id, content, author_key, channel_id, parent_message_key, message_type, mentions)
				SELECT ${messageId}, ${content}, ${authorKey}, ${channelId}, ${parentMessageKey}, 'message', ${mentionsJson}
				WHERE ${parentMessageKey === null
					? sql`TRUE`
					: sql`EXISTS (
						SELECT 1 FROM app.chat_message p
						WHERE p.message_key = ${parentMessageKey}
						  AND p.channel_id = ${channelId}
						  AND p.parent_message_key IS NULL
						  AND p.deleted_at IS NULL
					)`}
				RETURNING message_id AS "messageId"
			`)
		) as RawRow[];

		if (rows.length === 0) return { status: 'bad-parent' };

		// Touch the root so the reply is visible to everyone else's next poll.
		//
		// The delta matches root messages on `message_key > since OR updated_at >
		// sinceTs`, and a reply is not a root — without this bump, replyCount and
		// lastReplyAt only ever refreshed for the person who posted the reply, and
		// everyone else's thread footer stayed stale until a reload. edited_at is
		// deliberately untouched, so this does not make the root say "(edited)".
		if (parentMessageKey !== null) {
			await db.execute(sql`
				UPDATE app.chat_message SET updated_at = now() WHERE message_key = ${parentMessageKey}
			`);
		}

		return { status: 'created', messageId };
	} catch (err) {
		if ((err as { code?: string })?.code === '23505') {
			// The same client id already landed — a retry, or a double-tap.
			return { status: 'duplicate', messageId };
		}
		throw err;
	}
}

/** Edit. Author-only, enforced in the WHERE clause rather than a prior read. */
export async function updateMessage(opts: {
	messageId: string;
	channelId: string;
	authorKey: number;
	content: string;
	mentions: number[];
}): Promise<boolean> {
	const { messageId, channelId, authorKey, content, mentions } = opts;
	const mentionsJson = mentions.length > 0 ? JSON.stringify(mentions) : null;

	const rows = Array.from(
		await db.execute(sql`
			UPDATE app.chat_message
			SET content = ${content},
			    mentions = ${mentionsJson},
			    edited_at = now(),
			    updated_at = now()
			WHERE message_id = ${messageId}
			  AND channel_id = ${channelId}
			  AND author_key = ${authorKey}
			  AND deleted_at IS NULL
			RETURNING message_key
		`)
	);

	return rows.length > 0;
}

/**
 * Soft delete. The row survives as a tombstone so a poll can tell other clients
 * the message went away — a hard delete is invisible to a cursor.
 */
export async function softDeleteMessage(opts: {
	messageId: string;
	channelId: string;
	authorKey: number;
}): Promise<boolean> {
	const { messageId, channelId, authorKey } = opts;

	const rows = Array.from(
		await db.execute(sql`
			UPDATE app.chat_message
			SET deleted_at = now(), updated_at = now()
			WHERE message_id = ${messageId}
			  AND channel_id = ${channelId}
			  AND author_key = ${authorKey}
			  AND deleted_at IS NULL
			RETURNING message_key AS "messageKey", parent_message_key AS "parentMessageKey"
		`)
	) as RawRow[];

	if (rows.length === 0) return false;

	// Deleting a reply lowers the root's replyCount. Touch the root for the same
	// reason inserting one does — otherwise everyone else keeps seeing the old count.
	const parentMessageKey = toNullableNumber(rows[0].parentMessageKey);
	if (parentMessageKey !== null) {
		await db.execute(sql`
			UPDATE app.chat_message SET updated_at = now() WHERE message_key = ${parentMessageKey}
		`);
	}

	return true;
}

/**
 * Add the reaction if it isn't there, remove it if it is — one statement.
 *
 * The old handler read first and then wrote, and answered a repeated reaction
 * with a 400 that the client silently swallowed. Here `ins` references `del` in
 * its WHERE, which forces `del` to be evaluated first; both see the same
 * snapshot so they cannot both fire. ON CONFLICT DO NOTHING covers the genuine
 * race of one manager clicking the same pill in two tabs at once.
 */
export async function toggleReaction(opts: {
	channelId: string;
	messageId: string;
	me: number;
	emoji: string;
	emojiType: 'unicode' | 'custom';
}): Promise<{ messageKey: number; mine: boolean; reactions: ReactionGroup[] } | null> {
	const { channelId, messageId, me, emoji, emojiType } = opts;

	const rows = Array.from(
		await db.execute(sql`
			WITH target AS (
				SELECT message_key FROM app.chat_message
				WHERE message_id = ${messageId}
				  AND channel_id = ${channelId}
				  AND deleted_at IS NULL
			),
			del AS (
				DELETE FROM app.chat_reaction
				WHERE message_key = (SELECT message_key FROM target)
				  AND author_key = ${me}
				  AND emoji = ${emoji}
				RETURNING 1
			),
			ins AS (
				INSERT INTO app.chat_reaction (message_key, author_key, emoji, emoji_type)
				SELECT (SELECT message_key FROM target), ${me}, ${emoji}, ${emojiType}
				WHERE EXISTS (SELECT 1 FROM target)
				  AND NOT EXISTS (SELECT 1 FROM del)
				ON CONFLICT (message_key, author_key, emoji) DO NOTHING
				RETURNING 1
			)
			SELECT (SELECT message_key FROM target)      AS "messageKey",
			       (SELECT count(*) FROM ins)::int > 0   AS "added"
		`)
	) as RawRow[];

	const messageKey = toNullableNumber(rows[0]?.messageKey);
	if (messageKey === null) return null;

	return {
		messageKey,
		mine: rows[0]?.added === true,
		reactions: await selectReactionsForMessage(messageKey, me)
	};
}

/**
 * The roster, for @mention autocomplete and reaction attribution.
 *
 * Sourced from the allowlist rather than dim_manager: only people who can sign
 * in can be usefully mentioned, and dim_manager still holds two retired
 * '-- hidden --' rows from 2010.
 */
export async function selectRoster(): Promise<RosterMember[]> {
	const rows = Array.from(
		await db.execute(sql`
			SELECT lm.manager_key                                              AS "managerKey",
			       COALESCE(dm.manager_name, lm.display_name, 'Unknown')       AS "name",
			       COALESCE(dm.display_name, lm.display_name, dm.manager_name, 'Unknown') AS "displayName",
			       dm.profile_image_url                                        AS "profileImageUrl"
			FROM app.league_member lm
			LEFT JOIN edw.dim_manager dm ON dm.manager_key = lm.manager_key
			WHERE lm.active = true
			ORDER BY "displayName"
		`)
	) as RawRow[];

	return rows.map((row) => ({
		managerKey: toNumber(row.managerKey),
		name: String(row.name),
		displayName: String(row.displayName),
		profileImageUrl: row.profileImageUrl == null ? null : String(row.profileImageUrl)
	}));
}

export async function selectLastRead(opts: {
	managerKey: number;
	channelId: string;
}): Promise<number | null> {
	const rows = Array.from(
		await db.execute(sql`
			SELECT last_read_message_key AS "lastReadMessageKey"
			FROM app.chat_read
			WHERE manager_key = ${opts.managerKey} AND channel_id = ${opts.channelId}
			LIMIT 1
		`)
	) as RawRow[];

	return toNullableNumber(rows[0]?.lastReadMessageKey);
}

/**
 * Advance the read watermark.
 *
 * GREATEST, not assignment: a background tab flushing a stale beacon on pagehide
 * must not rewind read state that a foreground tab already advanced.
 */
export async function upsertRead(opts: {
	managerKey: number;
	channelId: string;
	lastReadMessageKey: number;
}): Promise<void> {
	const { managerKey, channelId, lastReadMessageKey } = opts;

	await db.execute(sql`
		INSERT INTO app.chat_read (manager_key, channel_id, last_read_message_key, last_read_at, updated_at)
		VALUES (${managerKey}, ${channelId}, ${lastReadMessageKey}, now(), now())
		ON CONFLICT (manager_key, channel_id) DO UPDATE
		SET last_read_message_key = GREATEST(
		      app.chat_read.last_read_message_key,
		      EXCLUDED.last_read_message_key
		    ),
		    last_read_at = now(),
		    updated_at = now()
	`);
}
