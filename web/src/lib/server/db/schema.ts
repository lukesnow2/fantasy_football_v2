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
export const user = appSchema.table('user', {
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
}, (table) => ({
	uniqueManagerKey: uniqueIndex('unique_user_manager_key').on(table.managerKey)
}));

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
	// Unique: one login per manager, and it makes this a valid FK target.
	managerKey: integer('manager_key')
		.notNull()
		.references(() => dimManager.managerKey),
	role: varchar('role', { length: 20 }).notNull().default('member'), // 'member' | 'commissioner'
	active: boolean('active').notNull().default(true),
	displayName: varchar('display_name', { length: 255 }),
	invitedAt: timestamp('invited_at', { withTimezone: true, mode: 'date' }),
	firstLoginAt: timestamp('first_login_at', { withTimezone: true, mode: 'date' }),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow(),
	updatedAt: timestamp('updated_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
}, (table) => ({
	uniqueManagerKey: uniqueIndex('league_member_manager_key_idx').on(table.managerKey)
	// NOTE: a case-insensitive unique index on lower(email) is also required, and
	// is hand-appended to the generated migration — drizzle-kit 0.30 cannot
	// express an expression index. See drizzle/0000_init.sql. Removing it would
	// let "Bob@x.com" and "bob@x.com" become two members of the same league.
}));

/**
 * Single-use, short-lived magic-link tokens.
 *
 * Mirrors the session primitive in auth.ts: the raw token goes in the emailed
 * URL and only its sha256 is stored, so a database leak yields nothing usable.
 */
export const loginToken = appSchema.table('login_token', {
	tokenHash: text('token_hash').primaryKey(),
	email: varchar('email', { length: 255 }).notNull(), // always stored lowercased
	purpose: varchar('purpose', { length: 20 }).notNull().default('login'), // 'login' | 'invite'
	redirectTo: text('redirect_to'), // validated same-origin path, or null
	expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull(),
	consumedAt: timestamp('consumed_at', { withTimezone: true, mode: 'date' }),
	requestIp: varchar('request_ip', { length: 45 }), // IPv6 compatible
	userAgent: text('user_agent'),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).notNull().defaultNow()
}, (table) => ({
	emailIdx: index('login_token_email_idx').on(table.email),
	expiresIdx: index('login_token_expires_idx').on(table.expiresAt)
}));

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

export const ruleVote = appSchema.table('rule_vote', {
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
}, (table) => ({
	// Ensure one vote per manager per proposal
	uniqueVote: uniqueIndex('unique_vote_per_manager').on(table.proposalKey, table.managerKey)
}));

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

export const constitutionSection = appSchema.table('constitution_section', {
	sectionKey: serial('section_key').primaryKey(),
	versionKey: integer('version_key')
		.notNull()
		.references(() => constitutionVersion.versionKey, { onDelete: 'cascade' }),
	sectionId: varchar('section_id', { length: 50 }).notNull(), // 'article1'…'article9', 'appendix1'
	title: varchar('title', { length: 255 }).notNull(),
	kind: varchar('kind', { length: 20 }).notNull(), // 'article' | 'appendix'
	icon: varchar('icon', { length: 50 }), // lucide icon name, resolved client-side
	sortOrder: integer('sort_order').notNull()
}, (table) => ({
	uniqueSection: uniqueIndex('constitution_section_version_id_idx').on(
		table.versionKey,
		table.sectionId
	)
}));

export const constitutionClause = appSchema.table('constitution_clause', {
	clauseKey: serial('clause_key').primaryKey(),
	sectionKey: integer('section_key')
		.notNull()
		.references(() => constitutionSection.sectionKey, { onDelete: 'cascade' }),
	parentKey: integer('parent_key'),
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
}, (table) => ({
	uniqueClause: uniqueIndex('constitution_clause_section_uid_idx').on(
		table.sectionKey,
		table.clauseUid
	),
	sectionOrderIdx: index('constitution_clause_section_idx').on(table.sectionKey, table.sortOrder)
}));

// Chat System Tables
export const chatMessage = appSchema.table('chat_message', {
	messageKey: serial('message_key').primaryKey(),
	messageId: varchar('message_id', { length: 100 }).notNull().unique(),
	content: text('content').notNull(),
	// FK to the allowlist, not dim_manager — see rule_proposal.submitted_by.
	authorKey: integer('author_key')
		.notNull()
		.references(() => leagueMember.managerKey),
	channelId: varchar('channel_id', { length: 100 }).notNull().default('general'), // Channel identifier
	parentMessageKey: integer('parent_message_key'), // For threaded replies
	messageType: varchar('message_type', { length: 20 }).default('message'), // 'message', 'system', 'join', 'leave'
	editedAt: timestamp('edited_at'),
	deletedAt: timestamp('deleted_at'),
	attachments: text('attachments'), // JSON array of file attachments
	mentions: text('mentions'), // JSON array of mentioned user keys
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
}, (table) => ({
	// Plain indexes, not unique: these exist to make channel and thread queries
	// fast. Declared UNIQUE they would also forbid two messages in the same
	// channel sharing a created_at, which two quick sends collide on.
	channelIndex: index('chat_message_channel_created').on(table.channelId, table.createdAt),
	threadIndex: index('chat_message_thread').on(table.parentMessageKey, table.createdAt)
}));

export const chatThread = appSchema.table('chat_thread', {
	threadKey: serial('thread_key').primaryKey(),
	threadId: varchar('thread_id', { length: 100 }).notNull().unique(),
	rootMessageKey: integer('root_message_key').notNull(), // Original message that started the thread
	channelId: varchar('channel_id', { length: 100 }).notNull(),
	title: varchar('title', { length: 255 }), // Optional thread title
	messageCount: integer('message_count').default(0),
	lastMessageAt: timestamp('last_message_at'),
	lastMessageKey: integer('last_message_key'),
	isLocked: boolean('is_locked').default(false),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
}, (table) => ({
	// Plain index — see the note on chat_message above.
	channelThreadIndex: index('chat_thread_channel_updated').on(table.channelId, table.updatedAt)
}));

export const chatReaction = appSchema.table('chat_reaction', {
	reactionKey: serial('reaction_key').primaryKey(),
	messageKey: integer('message_key').notNull(),
	authorKey: integer('author_key').notNull(), // Who reacted
	emoji: varchar('emoji', { length: 100 }).notNull(), // Unicode emoji or custom emoji name
	emojiType: varchar('emoji_type', { length: 20 }).default('unicode'), // 'unicode' or 'custom'
	createdAt: timestamp('created_at').defaultNow()
}, (table) => ({
	// Ensure one reaction per user per message per emoji
	uniqueReaction: uniqueIndex('unique_chat_reaction').on(table.messageKey, table.authorKey, table.emoji)
}));

export const chatRead = appSchema.table('chat_read', {
	readKey: serial('read_key').primaryKey(),
	managerKey: integer('manager_key').notNull(),
	channelId: varchar('channel_id', { length: 100 }).notNull(),
	lastReadMessageKey: integer('last_read_message_key'),
	lastReadAt: timestamp('last_read_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
}, (table) => ({
	// Ensure one read record per user per channel
	uniqueRead: uniqueIndex('unique_chat_read').on(table.managerKey, table.channelId)
}));

export const chatCustomEmoji = appSchema.table('chat_custom_emoji', {
	emojiKey: serial('emoji_key').primaryKey(),
	emojiId: varchar('emoji_id', { length: 100 }).notNull().unique(),
	name: varchar('name', { length: 50 }).notNull().unique(), // :my_emoji:
	imageUrl: varchar('image_url', { length: 500 }).notNull(),
	createdBy: integer('created_by').notNull(), // Manager who uploaded it
	category: varchar('category', { length: 50 }).default('custom'), // 'team', 'manager', 'general', etc.
	isActive: boolean('is_active').default(true),
	usageCount: integer('usage_count').default(0),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
});

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
export type ChatThread = typeof chatThread.$inferSelect;
export type ChatReaction = typeof chatReaction.$inferSelect;
export type ChatRead = typeof chatRead.$inferSelect;
export type ChatCustomEmoji = typeof chatCustomEmoji.$inferSelect;

