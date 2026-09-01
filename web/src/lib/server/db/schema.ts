import {
	pgTable,
	varchar,
	integer,
	decimal,
	boolean,
	timestamp,
	text,
	date,
	pgSchema,
	type AnyPgColumn,
	index,
	uniqueIndex,
	serial,
	smallint,
	bigint
} from 'drizzle-orm/pg-core';

// Define schemas
export const edwSchema = pgSchema('edw');
export const appSchema = pgSchema('app');

// EDW Dimension Tables
export const dimLeague = edwSchema.table('dim_league', {
	leagueKey: integer('league_key').primaryKey(),
	leagueId: varchar('league_id', { length: 50 }).notNull(),
	leagueName: varchar('league_name', { length: 255 }).notNull(),
	seasonYear: integer('season_year').notNull(),
	numTeams: integer('num_teams').notNull(),
	leagueType: varchar('league_type', { length: 50 }).notNull(),
	scoringType: varchar('scoring_type', { length: 50 }),
	draftType: varchar('draft_type', { length: 50 }),
	isActive: boolean('is_active').default(true),
	validFrom: timestamp('valid_from').defaultNow(),
	validTo: timestamp('valid_to'),
	createdAt: timestamp('created_at').defaultNow()
});

export const dimTeam = edwSchema.table('dim_team', {
	teamKey: integer('team_key').primaryKey(),
	teamId: varchar('team_id', { length: 50 }).notNull(),
	leagueKey: integer('league_key').notNull(),
	managerKey: integer('manager_key'),
	teamName: varchar('team_name', { length: 255 }).notNull(),
	managerName: varchar('manager_name', { length: 255 }),
	managerId: varchar('manager_id', { length: 100 }),
	teamLogoUrl: varchar('team_logo_url', { length: 500 }),
	isActive: boolean('is_active').default(true),
	validFrom: timestamp('valid_from').defaultNow(),
	validTo: timestamp('valid_to'),
	createdAt: timestamp('created_at').defaultNow()
});

export const dimManager = edwSchema.table('dim_manager', {
	managerKey: integer('manager_key').primaryKey(),
	managerName: varchar('manager_name', { length: 255 }).notNull(),
	managerId: varchar('manager_id', { length: 100 }),
	firstSeasonYear: integer('first_season_year'),
	lastSeasonYear: integer('last_season_year'),
	totalSeasons: integer('total_seasons').default(0),
	totalLeagues: integer('total_leagues').default(0),
	isCurrent: boolean('is_current').default(true),
	includeInAnalysis: boolean('include_in_analysis').default(true),
	email: varchar('email', { length: 255 }),
	displayName: varchar('display_name', { length: 255 }),
	profileImageUrl: varchar('profile_image_url', { length: 500 }),
	isActive: boolean('is_active').default(true),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
});

export const dimSeason = edwSchema.table('dim_season', {
	seasonKey: integer('season_key').primaryKey(),
	seasonYear: integer('season_year').notNull(),
	seasonStartDate: date('season_start_date'),
	seasonEndDate: date('season_end_date'),
	playoffStartWeek: integer('playoff_start_week'),
	championshipWeek: integer('championship_week'),
	totalWeeks: integer('total_weeks'),
	isCurrentSeason: boolean('is_current_season').default(false),
	seasonStatus: varchar('season_status', { length: 20 }).default('completed'),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
});

// EDW Fact Tables
export const factTeamPerformance = edwSchema.table('fact_team_performance', {
	performanceKey: integer('performance_key').primaryKey(),
	teamKey: integer('team_key').notNull(),
	managerKey: integer('manager_key').notNull(),
	leagueKey: integer('league_key').notNull(),
	weekKey: integer('week_key').notNull(),
	seasonYear: integer('season_year').notNull(),
	weekNumber: integer('week_number').notNull(),
	pointsFor: decimal('points_for', { precision: 8, scale: 2 }),
	pointsAgainst: decimal('points_against', { precision: 8, scale: 2 }),
	wins: integer('wins').default(0),
	losses: integer('losses').default(0),
	ties: integer('ties').default(0),
	weeklyRank: integer('weekly_rank'),
	seasonRank: integer('season_rank'),
	playoffSeed: integer('playoff_seed'),
	isPlayoffTeam: boolean('is_playoff_team').default(false),
	createdAt: timestamp('created_at').defaultNow()
});

export const factTransaction = edwSchema.table('fact_transaction', {
	transactionKey: integer('transaction_key').primaryKey(),
	leagueKey: integer('league_key').notNull(),
	playerKey: integer('player_key').notNull(),
	fromTeamKey: integer('from_team_key'),
	toTeamKey: integer('to_team_key'),
	fromManagerKey: integer('from_manager_key'),
	toManagerKey: integer('to_manager_key'),
	seasonYear: integer('season_year').notNull(),
	transactionWeek: integer('transaction_week'),
	transactionDate: date('transaction_date').notNull(),
	transactionType: varchar('transaction_type', { length: 30 }).notNull(),
	faabBid: decimal('faab_bid', { precision: 8, scale: 2 }),
	tradeGroupId: varchar('trade_group_id', { length: 100 }),
	isSuccessful: boolean('is_successful').default(true),
	createdAt: timestamp('created_at').defaultNow()
});

export const factMatchup = edwSchema.table('fact_matchup', {
	matchupKey: integer('matchup_key').primaryKey(),
	leagueKey: integer('league_key').notNull(),
	seasonYear: integer('season_year').notNull(),
	weekKey: integer('week_key').notNull(),
	weekNumber: integer('week_number').notNull(),
	team1Key: integer('team1_key').notNull(),
	team2Key: integer('team2_key').notNull(),
	manager1Key: integer('manager1_key').notNull(),
	manager2Key: integer('manager2_key').notNull(),
	team1Points: decimal('team1_points', { precision: 8, scale: 2 }),
	team2Points: decimal('team2_points', { precision: 8, scale: 2 }),
	winnerTeamKey: integer('winner_team_key'),
	winnerManagerKey: integer('winner_manager_key'),
	matchupType: varchar('matchup_type', { length: 20 }).default('regular'),
	isPlayoffs: boolean('is_playoffs').default(false),
	isChampionship: boolean('is_championship').default(false),
	isSemifinal: boolean('is_semifinal').default(false),
	isQuarterfinal: boolean('is_quarterfinal').default(false),
	isLastPlaceGame: boolean('is_last_place_game').default(false),
	marginOfVictory: decimal('margin_of_victory', { precision: 8, scale: 2 }),
	createdAt: timestamp('created_at').defaultNow()
});

// EDW Mart Tables for easy querying
// Columns aligned to the actual edw.mart_manager_performance table produced by the ETL.
export const martManagerPerformance = edwSchema.table('mart_manager_performance', {
	managerId: varchar('manager_id', { length: 100 }).primaryKey(),
	managerName: varchar('manager_name', { length: 255 }).notNull(),
	firstSeason: integer('first_season'),
	lastSeason: integer('last_season'),
	totalSeasons: integer('total_seasons'),
	totalLeagues: integer('total_leagues'),
	totalWins: integer('total_wins'),
	totalLosses: integer('total_losses'),
	totalTies: integer('total_ties'),
	careerWinPercentage: decimal('career_win_percentage', { precision: 8, scale: 4 }),
	totalPointsScored: decimal('total_points_scored', { precision: 15, scale: 2 }),
	avgPointsPerGame: decimal('avg_points_per_game', { precision: 10, scale: 2 }),
	avgPointsPerSeason: decimal('avg_points_per_season', { precision: 12, scale: 2 }),
	championshipsWon: integer('championships_won'),
	championshipAppearances: integer('championship_appearances'),
	playoffAppearances: integer('playoff_appearances'),
	playoffWinPercentage: decimal('playoff_win_percentage', { precision: 8, scale: 4 }),
	avgDraftGrade: decimal('avg_draft_grade', { precision: 4, scale: 2 }),
	bestDraftYear: integer('best_draft_year'),
	worstDraftYear: integer('worst_draft_year'),
	totalTransactions: integer('total_transactions'),
	avgTransactionsPerSeason: decimal('avg_transactions_per_season', { precision: 8, scale: 2 }),
	faabEfficiencyRating: decimal('faab_efficiency_rating', { precision: 8, scale: 4 }),
	seasonConsistencyScore: decimal('season_consistency_score', { precision: 8, scale: 4 }),
	bestSeasonRecord: varchar('best_season_record', { length: 10 }),
	worstSeasonRecord: varchar('worst_season_record', { length: 10 }),
	lastUpdated: timestamp('last_updated').defaultNow()
});

// EDW Views
export const vwCurrentSeasonDashboard = edwSchema.table('vw_current_season_dashboard', {
	leagueName: varchar('league_name', { length: 255 }),
	seasonYear: integer('season_year'),
	teamName: varchar('team_name', { length: 255 }),
	managerName: varchar('manager_name', { length: 255 }),
	wins: integer('wins'),
	losses: integer('losses'),
	ties: integer('ties'),
	pointsFor: decimal('points_for', { precision: 8, scale: 2 }),
	pointsAgainst: decimal('points_against', { precision: 8, scale: 2 }),
	pointDifferential: decimal('point_differential', { precision: 8, scale: 2 }),
	winPercentage: decimal('win_percentage', { precision: 5, scale: 4 }),
	seasonRank: integer('season_rank'),
	playoffProbability: decimal('playoff_probability', { precision: 5, scale: 4 }),
	isPlayoffTeam: boolean('is_playoff_team'),
	playoffSeed: integer('playoff_seed')
});

// Application Schema Tables (User management, UI features, etc.)
export const user = appSchema.table(
	'user',
	{
		id: text('id').primaryKey(),
		username: text('username').notNull().unique(),
		// Manager linking (references EDW schema)
		managerKey: integer('manager_key').references(() => dimManager.managerKey),
		// User profile
		email: varchar('email', { length: 255 }).unique(),
		displayName: varchar('display_name', { length: 255 }),
		// Account status for claiming placeholders
		accountStatus: varchar('account_status', { length: 20 }).default('active'),
		// User preferences stored as JSON
		notificationPreferences: text('notification_preferences'),
		profileSettings: text('profile_settings'),
		// Timestamps
		createdAt: timestamp('created_at').defaultNow(),
		updatedAt: timestamp('updated_at').defaultNow()
	},
	(table) => ({
		uniqueManagerKey: uniqueIndex('unique_user_manager_key').on(table.managerKey)
	})
);

export const session = appSchema.table('session', {
	id: text('id').primaryKey(),
	userId: text('user_id').notNull(),
	expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull(),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow(),
	lastUsedAt: timestamp('last_used_at', { withTimezone: true, mode: 'date' }).defaultNow(),
	userAgent: text('user_agent'),
	ipAddress: varchar('ip_address', { length: 45 }), // IPv6 compatible
	deviceType: varchar('device_type', { length: 50 })
});

/**
 * The league roster allowlist — the sole gate on who may sign in.
 *
 * Deliberately NOT edw.dim_manager: that table is written by the Python ETL and
 * holds every manager across 20 years, including departed and Yahoo-private
 * ones. This table holds exactly the people who may log in today, so it is also
 * the correct FK target for anything attributing an action to a manager, and the
 * correct denominator for a constitutional vote threshold.
 */
export const leagueMember = appSchema.table('league_member', {
	id: text('id').primaryKey(),
	email: varchar('email', { length: 255 }).notNull(),
	// Unique as a table constraint, not a separate index: every attribution
	// column in the app FKs to this, and Postgres needs the uniqueness to exist
	// before the referencing ALTER TABLE runs. A CREATE UNIQUE INDEX emitted
	// after the FKs is too late.
	managerKey: integer('manager_key')
		.notNull()
		.unique()
		.references(() => dimManager.managerKey),
	role: varchar('role', { length: 20 }).notNull().default('member'), // 'member' | 'commissioner'
	active: boolean('active').notNull().default(true),
	displayName: varchar('display_name', { length: 255 }),
	invitedAt: timestamp('invited_at', { withTimezone: true, mode: 'date' }),
	firstLoginAt: timestamp('first_login_at', { withTimezone: true, mode: 'date' }),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow(),
	updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
});
// NOTE: a case-insensitive unique index on lower(email) is also required and is
// hand-appended to the generated migration — drizzle-kit 0.30 cannot express an
// expression index. See the tail of drizzle/*_init.sql. Without it "Bob@x.com"
// and "bob@x.com" can both exist, and findMemberByEmail would have two rows to
// choose between when deciding who gets to log in.

/**
 * Single-use, short-lived magic-link tokens.
 *
 * Mirrors the session primitive in auth.ts: the raw token goes in the emailed
 * URL and only its sha256 is stored, so a database leak yields nothing usable.
 */
export const loginToken = appSchema.table(
	'login_token',
	{
		tokenHash: text('token_hash').primaryKey(),
		email: varchar('email', { length: 255 }).notNull(), // always stored lowercased
		purpose: varchar('purpose', { length: 20 }).notNull().default('login'), // 'login' | 'invite'
		redirectTo: text('redirect_to'), // validated same-origin path, or null
		expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull(),
		consumedAt: timestamp('consumed_at', { withTimezone: true, mode: 'date' }),
		requestIp: varchar('request_ip', { length: 45 }), // IPv6 compatible
		userAgent: text('user_agent'),
		createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
	},
	(table) => ({
		emailIdx: index('login_token_email_idx').on(table.email),
		expiresIdx: index('login_token_expires_idx').on(table.expiresAt)
	})
);

// Rule Proposal and Voting System Tables (Application features)
export const ruleProposal = appSchema.table('rule_proposal', {
	proposalKey: serial('proposal_key').primaryKey(),
	proposalId: varchar('proposal_id', { length: 50 }).notNull().unique(),
	title: varchar('title', { length: 255 }).notNull(),
	description: text('description').notNull(),
	proposalType: varchar('proposal_type', { length: 50 }).notNull(), // 'edit_clause' | 'add_clause' | 'delete_clause'
	/** Which Article 8 rule sets the bar. Chosen on the form, never inferred from the text. */
	category: varchar('category', { length: 40 }).notNull().default('general'),
	affectedSection: varchar('affected_section', { length: 100 }), // 'article1'…'appendix1'
	/**
	 * The clause this targets, by its stable uid rather than its position.
	 * A positional index breaks the moment any clause is inserted above it.
	 */
	targetClauseUid: text('target_clause_uid'),
	/** Only set for category 'manager_removal' — excluded from the denominator and barred from voting. */
	subjectManagerKey: integer('subject_manager_key').references(() => leagueMember.managerKey),
	currentLanguage: text('current_language'), // Clause text as it stood when proposed
	proposedLanguage: text('proposed_language').notNull(), // New clause text
	rationale: text('rationale').notNull(), // Why this change is needed
	effectiveSeason: integer('effective_season').notNull(), // Which season this takes effect
	/**
	 * FK to league_member, NOT to edw.dim_manager: dim_manager holds every manager
	 * across 20 years including retired and Yahoo-private ones, so it would happily
	 * accept manager_key 1 ('-- hidden --'). Pointing at the allowlist makes an
	 * unattributable proposal a constraint violation at insert time rather than a
	 * silently wrong row.
	 */
	submittedBy: integer('submitted_by')
		.notNull()
		.references(() => leagueMember.managerKey),
	submittedAt: timestamp('submitted_at').defaultNow(),
	votingStartDate: timestamp('voting_start_date'),
	votingEndDate: timestamp('voting_end_date'),
	status: varchar('status', { length: 20 }).notNull().default('draft'), // draft | active | passed | rejected | withdrawn | superseded
	requiredVotes: integer('required_votes').notNull(),
	eligibleVoters: integer('eligible_voters').notNull(),
	yesVotes: integer('yes_votes').notNull().default(0),
	noVotes: integer('no_votes').notNull().default(0),
	abstainVotes: integer('abstain_votes').notNull().default(0),
	/** Set once the proposal reaches a terminal state, by evaluate.ts and nothing else. */
	settledAt: timestamp('settled_at', { withTimezone: true, mode: 'date' }),
	appliedAt: timestamp('applied_at', { withTimezone: true, mode: 'date' }),
	amendmentKey: integer('amendment_key'),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
});

export const ruleVote = appSchema.table(
	'rule_vote',
	{
		voteKey: serial('vote_key').primaryKey(),
		proposalKey: integer('proposal_key')
			.notNull()
			.references(() => ruleProposal.proposalKey, { onDelete: 'cascade' }),
		/** The voter. FK to the allowlist for the same reason as rule_proposal.submitted_by. */
		managerKey: integer('manager_key')
			.notNull()
			.references(() => leagueMember.managerKey),
		vote: varchar('vote', { length: 10 }).notNull(), // 'yes', 'no', 'abstain'
		comment: text('comment'), // Optional comment on vote
		votedAt: timestamp('voted_at').defaultNow(),
		createdAt: timestamp('created_at').defaultNow()
	},
	(table) => ({
		// Ensure one vote per manager per proposal
		uniqueVote: uniqueIndex('unique_vote_per_manager').on(table.proposalKey, table.managerKey)
	})
);

export const ruleAmendment = appSchema.table('rule_amendment', {
	amendmentKey: serial('amendment_key').primaryKey(),
	proposalKey: integer('proposal_key')
		.notNull()
		.references(() => ruleProposal.proposalKey),
	amendmentYear: integer('amendment_year').notNull(),
	amendmentTitle: varchar('amendment_title', { length: 255 }).notNull(),
	amendmentDescription: text('amendment_description').notNull(),
	effectiveSeason: integer('effective_season').notNull(),
	voteResults: text('vote_results'), // JSON string of final vote counts
	/** The proposal's author. Null only for amendments recorded outside the vote flow. */
	approvedBy: integer('approved_by').references(() => leagueMember.managerKey),
	approvedAt: timestamp('approved_at'),
	createdAt: timestamp('created_at').defaultNow()
});

// ---------------------------------------------------------------------------
// Constitution (copy-on-write versioned document)
// ---------------------------------------------------------------------------
//
// Every passed amendment clones the whole document into a new version rather
// than mutating rows in place. At ~10 sections and ~70 clauses that costs
// nothing, and it buys real history: "the constitution as of 2019" is one
// WHERE clause, diffing two versions is a join, and "Last Updated" stops being
// a hardcoded string.

export const constitutionVersion = appSchema.table('constitution_version', {
	versionKey: serial('version_key').primaryKey(),
	versionNo: integer('version_no').notNull().unique(),
	/**
	 * When this version takes effect. Resolving "current" as the newest row with
	 * effective_at <= now() means a proposal passed today with effect from 2027
	 * simply is not current yet — no is_current flag to keep in sync.
	 */
	effectiveAt: timestamp('effective_at', { withTimezone: true, mode: 'date' })
		.notNull()
		.defaultNow(),
	amendmentKey: integer('amendment_key').references(() => ruleAmendment.amendmentKey), // null for v1
	note: text('note'),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
});

export const constitutionSection = appSchema.table(
	'constitution_section',
	{
		sectionKey: serial('section_key').primaryKey(),
		versionKey: integer('version_key')
			.notNull()
			.references(() => constitutionVersion.versionKey, { onDelete: 'cascade' }),
		sectionId: varchar('section_id', { length: 50 }).notNull(), // 'article1'…'article9', 'appendix1'
		title: varchar('title', { length: 255 }).notNull(),
		kind: varchar('kind', { length: 20 }).notNull(), // 'article' | 'appendix'
		icon: varchar('icon', { length: 50 }), // lucide icon name, resolved client-side
		sortOrder: integer('sort_order').notNull()
	},
	(table) => ({
		uniqueSection: uniqueIndex('constitution_section_version_id_idx').on(
			table.versionKey,
			table.sectionId
		)
	})
);

export const constitutionClause = appSchema.table(
	'constitution_clause',
	{
		clauseKey: serial('clause_key').primaryKey(),
		sectionKey: integer('section_key')
			.notNull()
			.references(() => constitutionSection.sectionKey, { onDelete: 'cascade' }),
		/**
		 * ON DELETE CASCADE is load-bearing, not decoration. apply.ts deletes a
		 * clause and relies on its children going with it; without the constraint
		 * they survive with a dangling parent_key, vanish from the rendered tree
		 * (loadVersionTree can neither attach nor root them), and then reappear as
		 * top-level clauses on the next amendment when cloneVersion remaps the
		 * missing parent to null.
		 */
		parentKey: integer('parent_key').references((): AnyPgColumn => constitutionClause.clauseKey, {
			onDelete: 'cascade'
		}),
		/**
		 * Stable across versions — this is what a proposal targets.
		 *
		 * clause_key changes on every clone, so it cannot identify "the clause this
		 * proposal edits" across an intervening amendment. clause_uid is copied
		 * verbatim by the clone and is the only durable handle.
		 */
		clauseUid: text('clause_uid').notNull(),
		depth: smallint('depth').notNull(), // 0 = I./II.  1 = a./b.  2 = i./ii.
		sortOrder: integer('sort_order').notNull(),
		label: varchar('label', { length: 16 }).notNull(), // 'I', 'a', 'iii' — regenerated on insert/delete
		body: text('body').notNull(),
		createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
	},
	(table) => ({
		uniqueClause: uniqueIndex('constitution_clause_section_uid_idx').on(
			table.sectionKey,
			table.clauseUid
		),
		sectionOrderIdx: index('constitution_clause_section_idx').on(table.sectionKey, table.sortOrder)
	})
);

// Chat System Tables
export const chatMessage = appSchema.table(
	'chat_message',
	{
		messageKey: serial('message_key').primaryKey(),
		messageId: varchar('message_id', { length: 100 }).notNull().unique(),
		content: text('content').notNull(),
		// FK to the allowlist, not dim_manager — see rule_proposal.submitted_by.
		authorKey: integer('author_key')
			.notNull()
			.references(() => leagueMember.managerKey),
		channelId: varchar('channel_id', { length: 100 }).notNull().default('general'), // Channel identifier
		// Self-referencing: a reply points at its root. One level only — the insert
		// path rejects a parent that is itself a reply.
		parentMessageKey: integer('parent_message_key').references(
			(): AnyPgColumn => chatMessage.messageKey,
			{
				onDelete: 'cascade'
			}
		),
		messageType: varchar('message_type', { length: 20 }).default('message'), // 'message', 'system', 'join', 'leave'
		// timestamptz throughout. As plain `timestamp` these were parsed in whatever
		// zone the server process happened to be in — right on Vercel, hours wrong in
		// local dev, with nothing to indicate which.
		editedAt: timestamp('edited_at', { withTimezone: true, mode: 'date' }),
		deletedAt: timestamp('deleted_at', { withTimezone: true, mode: 'date' }),
		attachments: text('attachments'), // JSON array of file attachments
		mentions: text('mentions'), // JSON array of mentioned manager keys, written server-side
		createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow(),
		updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).defaultNow()
	},
	(table) => ({
		// Plain indexes, not unique: these exist to make channel and thread queries
		// fast. Declared UNIQUE they would also forbid two messages in the same
		// channel sharing a created_at, which two quick sends collide on.
		channelIndex: index('chat_message_channel_created').on(table.channelId, table.createdAt),
		threadIndex: index('chat_message_thread').on(table.parentMessageKey, table.createdAt),
		// The root list pages on message_key DESC; the created_at index can't serve it.
		channelKeyIndex: index('idx_chat_message_channel_key').on(table.channelId, table.messageKey),
		// The delta poll's edit/tombstone arm.
		channelUpdatedIndex: index('idx_chat_message_channel_updated').on(
			table.channelId,
			table.updatedAt
		)
	})
);

// NOTE: `chat_thread` used to be declared here. Nothing ever read or wrote it,
// and its message_count / last_message_at columns duplicate what
// parent_message_key already tells us. Dropped in 0002_chat_hardening.sql.

export const chatReaction = appSchema.table(
	'chat_reaction',
	{
		reactionKey: serial('reaction_key').primaryKey(),
		messageKey: integer('message_key')
			.notNull()
			.references(() => chatMessage.messageKey, { onDelete: 'cascade' }),
		// Who reacted. FK to the allowlist, matching chat_message.author_key.
		authorKey: integer('author_key')
			.notNull()
			.references(() => leagueMember.managerKey),
		emoji: varchar('emoji', { length: 100 }).notNull(), // Unicode emoji or custom emoji name
		emojiType: varchar('emoji_type', { length: 20 }).default('unicode'), // 'unicode' or 'custom'
		createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow()
	},
	(table) => ({
		// One reaction per user per message per emoji. Also what makes the per-message
		// lateral lookup in the message query an index scan.
		uniqueReaction: uniqueIndex('unique_chat_reaction').on(
			table.messageKey,
			table.authorKey,
			table.emoji
		)
	})
);

export const chatRead = appSchema.table(
	'chat_read',
	{
		readKey: serial('read_key').primaryKey(),
		managerKey: integer('manager_key')
			.notNull()
			.references(() => leagueMember.managerKey, { onDelete: 'cascade' }),
		channelId: varchar('channel_id', { length: 100 }).notNull(),
		lastReadMessageKey: integer('last_read_message_key'),
		lastReadAt: timestamp('last_read_at', { withTimezone: true, mode: 'date' }).defaultNow(),
		updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).defaultNow()
	},
	(table) => ({
		// Ensure one read record per user per channel
		uniqueRead: uniqueIndex('unique_chat_read').on(table.managerKey, table.channelId)
	})
);

export const chatCustomEmoji = appSchema.table('chat_custom_emoji', {
	emojiKey: serial('emoji_key').primaryKey(),
	emojiId: varchar('emoji_id', { length: 100 }).notNull().unique(),
	name: varchar('name', { length: 50 }).notNull().unique(), // :my_emoji:
	imageUrl: varchar('image_url', { length: 500 }).notNull(),
	createdBy: integer('created_by').notNull(), // Manager who uploaded it
	category: varchar('category', { length: 50 }).default('custom'), // 'team', 'manager', 'general', etc.
	isActive: boolean('is_active').default(true),
	usageCount: integer('usage_count').default(0),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow(),
	updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).defaultNow()
});

// ---------------------------------------------------------------------------
// Side bets
// ---------------------------------------------------------------------------
//
// A record of what managers agreed to, not a book. Nothing here moves money and
// nothing here is enforceable — the site's only job is to remember the terms
// after everyone has forgotten them, and to hold the commissioner's ruling.

/**
 * One side bet between two managers.
 *
 * Two shapes in one table, distinguished by `counterpartyKey`:
 *  - null  → an open prop; any member other than the proposer may take it
 *  - set   → head-to-head; only that manager may accept
 *
 * Lifecycle: open → accepted → pending_resolution → settled | void, with
 * `declined` and `withdrawn` as dead ends before anyone agreed to anything.
 */
export const wager = appSchema.table(
	'wager',
	{
		wagerKey: serial('wager_key').primaryKey(),
		wagerId: varchar('wager_id', { length: 50 }).notNull().unique(),
		title: varchar('title', { length: 200 }).notNull(),
		/** What has to happen for the proposer to win. Free prose — this is the agreement of record. */
		terms: text('terms').notNull(),
		/** Free text on purpose: "$20", "loser buys wings", "2:1 on $50". No dollar ledger. */
		stake: varchar('stake', { length: 200 }).notNull(),
		seasonYear: integer('season_year'),
		/**
		 * FK to league_member, NOT edw.dim_manager — same reason as rule_proposal.submitted_by:
		 * dim_manager holds every manager across 20 years including manager_key 1,
		 * '-- hidden --'. Pointing at the allowlist makes an unattributable bet a
		 * constraint violation at insert time rather than a silently wrong row.
		 */
		proposedBy: integer('proposed_by')
			.notNull()
			.references(() => leagueMember.managerKey),
		/** Null = open prop any member may take. Set = head-to-head, only this manager may accept. */
		counterpartyKey: integer('counterparty_key').references(() => leagueMember.managerKey),
		/** Who actually took the other side. Equals counterpartyKey for a head-to-head. */
		acceptedBy: integer('accepted_by').references(() => leagueMember.managerKey),
		acceptedAt: timestamp('accepted_at', { withTimezone: true, mode: 'date' }),
		status: varchar('status', { length: 20 }).notNull().default('open'),
		// open | accepted | declined | withdrawn | pending_resolution | settled | void
		resolutionRequestedBy: integer('resolution_requested_by').references(
			() => leagueMember.managerKey
		),
		resolutionRequestedAt: timestamp('resolution_requested_at', {
			withTimezone: true,
			mode: 'date'
		}),
		/** What the flagging party says happened. Context for the commissioner, kept on the record. */
		resolutionNote: text('resolution_note'),
		outcome: varchar('outcome', { length: 20 }), // proposer | taker | push | void
		/** Denormalised from outcome so the W-L ledger is one pass. Null on a push or a void. */
		winnerKey: integer('winner_key').references(() => leagueMember.managerKey),
		rulingNote: text('ruling_note'),
		/** The commissioner who ruled. Nothing else writes this column. */
		resolvedBy: integer('resolved_by').references(() => leagueMember.managerKey),
		resolvedAt: timestamp('resolved_at', { withTimezone: true, mode: 'date' }),
		createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow(),
		updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
	},
	(table) => ({
		statusIdx: index('wager_status_idx').on(table.status),
		proposedByIdx: index('wager_proposed_by_idx').on(table.proposedBy)
	})
);

// Type exports
export type DimLeague = typeof dimLeague.$inferSelect;
export type DimTeam = typeof dimTeam.$inferSelect;
export type DimManager = typeof dimManager.$inferSelect;
export type DimSeason = typeof dimSeason.$inferSelect;
export type FactTeamPerformance = typeof factTeamPerformance.$inferSelect;
export type FactTransaction = typeof factTransaction.$inferSelect;
export type FactMatchup = typeof factMatchup.$inferSelect;
export type MartManagerPerformance = typeof martManagerPerformance.$inferSelect;
export type VwCurrentSeasonDashboard = typeof vwCurrentSeasonDashboard.$inferSelect;
export type RuleProposal = typeof ruleProposal.$inferSelect;
export type RuleVote = typeof ruleVote.$inferSelect;
export type RuleAmendment = typeof ruleAmendment.$inferSelect;
export type Session = typeof session.$inferSelect;
export type User = typeof user.$inferSelect;
export type LeagueMember = typeof leagueMember.$inferSelect;
export type LoginToken = typeof loginToken.$inferSelect;
export type ConstitutionVersion = typeof constitutionVersion.$inferSelect;
export type ConstitutionSection = typeof constitutionSection.$inferSelect;
export type ConstitutionClause = typeof constitutionClause.$inferSelect;

// Chat System Types
export type ChatMessage = typeof chatMessage.$inferSelect;
export type ChatReaction = typeof chatReaction.$inferSelect;
export type ChatRead = typeof chatRead.$inferSelect;
export type ChatCustomEmoji = typeof chatCustomEmoji.$inferSelect;

// Side Bet Types
export type Wager = typeof wager.$inferSelect;
