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
	uniqueIndex,
	serial,
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
export const martManagerPerformance = edwSchema.table('mart_manager_performance', {
	managerKey: integer('manager_key').primaryKey(),
	managerName: varchar('manager_name', { length: 255 }).notNull(),
	totalSeasons: integer('total_seasons'),
	totalWins: integer('total_wins'),
	totalLosses: integer('total_losses'),
	totalTies: integer('total_ties'),
	winPercentage: decimal('win_percentage', { precision: 5, scale: 4 }),
	avgPointsFor: decimal('avg_points_for', { precision: 8, scale: 2 }),
	avgPointsAgainst: decimal('avg_points_against', { precision: 8, scale: 2 }),
	totalChampionships: integer('total_championships'),
	totalPlayoffAppearances: integer('total_playoff_appearances'),
	bestSeason: varchar('best_season', { length: 4 }),
	worstSeason: varchar('worst_season', { length: 4 }),
	firstSeason: varchar('first_season', { length: 4 }),
	lastSeason: varchar('last_season', { length: 4 }),
	longestWinStreak: integer('longest_win_streak'),
	longestLossStreak: integer('longest_loss_streak'),
	totalTransactions: integer('total_transactions'),
	avgTransactionsPerSeason: decimal('avg_transactions_per_season', { precision: 5, scale: 2 }),
	updatedAt: timestamp('updated_at').defaultNow()
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
	age: integer('age'),
	username: text('username').notNull().unique(),
	passwordHash: text('password_hash').notNull(),
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
	// Passkey authentication fields
	passkeyEnabled: boolean('passkey_enabled').default(false),
	passkeyRegisteredAt: timestamp('passkey_registered_at'),
	// Timestamps
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
}, (table) => ({
	uniqueManagerKey: uniqueIndex('unique_user_manager_key').on(table.managerKey)
}));

export const session = appSchema.table('session', {
	id: text('id').primaryKey(),
	userId: text('user_id').notNull(),
	expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull()
});

export const passwordResetToken = appSchema.table('password_reset_token', {
	id: text('id').primaryKey(),
	userId: text('user_id').notNull(),
	email: text('email').notNull(),
	expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull(),
	createdAt: timestamp('created_at', { withTimezone: true, mode: 'date' }).defaultNow().notNull()
});

// WebAuthn Tables for Biometric Passkeys
export const webauthnCredentials = appSchema.table('webauthn_credentials', {
	id: text('id').primaryKey(),
	userId: text('user_id').notNull().references(() => user.id, { onDelete: 'cascade' }),
	credentialId: text('credential_id').notNull().unique(),
	publicKey: text('public_key').notNull(),
	signCount: bigint('sign_count', { mode: 'number' }).notNull().default(0),
	transports: text('transports').array(), // ['usb', 'nfc', 'ble', 'internal']
	backupEligible: boolean('backup_eligible').notNull().default(false),
	backupState: boolean('backup_state').notNull().default(false),
	createdAt: timestamp('created_at').defaultNow(),
	lastUsedAt: timestamp('last_used_at'),
	deviceType: varchar('device_type', { length: 50 }), // 'phone', 'laptop', 'desktop', 'tablet'
	authenticatorType: varchar('authenticator_type', { length: 50 }) // 'platform', 'cross-platform'
});

export const webauthnChallenges = appSchema.table('webauthn_challenges', {
	id: text('id').primaryKey(),
	challenge: text('challenge').notNull(),
	userId: text('user_id').references(() => user.id),
	type: varchar('type', { length: 20 }).notNull(), // 'registration', 'authentication'
	expiresAt: timestamp('expires_at').notNull(),
	createdAt: timestamp('created_at').defaultNow()
});

export const backupCodes = appSchema.table('backup_codes', {
	id: text('id').primaryKey(),
	userId: text('user_id').notNull().references(() => user.id, { onDelete: 'cascade' }),
	codeHash: text('code_hash').notNull(),
	used: boolean('used').default(false),
	usedAt: timestamp('used_at'),
	createdAt: timestamp('created_at').defaultNow()
});

// Rule Proposal and Voting System Tables (Application features)
export const ruleProposal = appSchema.table('rule_proposal', {
	proposalKey: serial('proposal_key').primaryKey(),
	proposalId: varchar('proposal_id', { length: 50 }).notNull().unique(),
	title: varchar('title', { length: 255 }).notNull(),
	description: text('description').notNull(),
	proposalType: varchar('proposal_type', { length: 50 }).notNull(), // 'add_clause', 'edit_language', 'tweak_rule'
	affectedSection: varchar('affected_section', { length: 100 }), // Which article/section this affects
	ruleIndex: integer('rule_index'), // Which specific rule within the section (0-based index)
	currentLanguage: text('current_language'), // Current rule text (for edits)
	proposedLanguage: text('proposed_language').notNull(), // New rule text
	rationale: text('rationale').notNull(), // Why this change is needed
	effectiveSeason: integer('effective_season').notNull(), // Which season this takes effect
	submittedBy: integer('manager_key').notNull(), // Who submitted the proposal
	submittedAt: timestamp('submitted_at').defaultNow(),
	votingStartDate: timestamp('voting_start_date'),
	votingEndDate: timestamp('voting_end_date'),
	status: varchar('status', { length: 20 }).default('draft'), // draft, active, passed, rejected, archived
	requiredVotes: integer('required_votes').default(7), // Super-majority default
	yesVotes: integer('yes_votes').default(0),
	noVotes: integer('no_votes').default(0),
	abstainVotes: integer('abstain_votes').default(0),
	createdAt: timestamp('created_at').defaultNow(),
	updatedAt: timestamp('updated_at').defaultNow()
});

export const ruleVote = appSchema.table('rule_vote', {
	voteKey: serial('vote_key').primaryKey(),
	proposalKey: integer('proposal_key').notNull(),
	managerKey: integer('manager_key').notNull(),
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
	proposalKey: integer('proposal_key').notNull(),
	amendmentYear: integer('amendment_year').notNull(),
	amendmentTitle: varchar('amendment_title', { length: 255 }).notNull(),
	amendmentDescription: text('amendment_description').notNull(),
	effectiveSeason: integer('effective_season').notNull(),
	voteResults: text('vote_results'), // JSON string of final vote counts
	approvedBy: integer('manager_key'), // Who approved the final amendment
	approvedAt: timestamp('approved_at'),
	createdAt: timestamp('created_at').defaultNow()
});

// Chat System Tables
export const chatMessage = appSchema.table('chat_message', {
	messageKey: serial('message_key').primaryKey(),
	messageId: varchar('message_id', { length: 100 }).notNull().unique(),
	content: text('content').notNull(),
	authorKey: integer('author_key').notNull(), // References dim_manager
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
	// Index for efficient channel queries
	channelIndex: uniqueIndex('chat_message_channel_created').on(table.channelId, table.createdAt),
	// Index for thread queries
	threadIndex: uniqueIndex('chat_message_thread').on(table.parentMessageKey, table.createdAt)
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
	// Index for efficient channel thread queries
	channelThreadIndex: uniqueIndex('chat_thread_channel_updated').on(table.channelId, table.updatedAt)
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
export type PasswordResetToken = typeof passwordResetToken.$inferSelect;
export type User = typeof user.$inferSelect;

// Chat System Types
export type ChatMessage = typeof chatMessage.$inferSelect;
export type ChatThread = typeof chatThread.$inferSelect;
export type ChatReaction = typeof chatReaction.$inferSelect;
export type ChatRead = typeof chatRead.$inferSelect;
export type ChatCustomEmoji = typeof chatCustomEmoji.$inferSelect;
