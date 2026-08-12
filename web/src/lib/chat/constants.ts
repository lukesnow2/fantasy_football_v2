/** Values shared by the chat server handlers and the chat UI. */

/**
 * The one channel that exists. Kept as a list rather than a string constant so
 * `channelId` validation is an allowlist from day one — the column is
 * varchar(100) and would otherwise accept anything a client sent.
 */
export const CHANNELS = ['general'] as const;
export type ChannelId = (typeof CHANNELS)[number];

export const DEFAULT_CHANNEL: ChannelId = 'general';

/** Measured in graphemes, not UTF-16 units. See `messageLength` in the server validator. */
export const MAX_MESSAGE_LENGTH = 2000;

/** chat_reaction.emoji is varchar(100); this is the byte budget, not a character count. */
export const MAX_EMOJI_BYTES = 100;

/** How many root messages the first load fetches, and how wide the delta's reaction window is. */
export const PAGE_SIZE = 50;

/** Consecutive messages from one author inside this window render as a single block. */
export const GROUPING_WINDOW_MS = 5 * 60 * 1000;

/** Offered in the hover toolbar until the viewer has a personal history to draw on. */
export const QUICK_EMOJI = ['👍', '🔥', '😂'];

export const RECENT_EMOJI_STORAGE_KEY = 'chat:recent-emoji';
export const MAX_RECENT_EMOJI = 12;

/**
 * Where emoji-picker-element loads its dataset from.
 *
 * Served from our own /static rather than the element's jsDelivr default: that
 * default is a ~440kB third-party fetch on first open, fails offline, and is the
 * kind of thing a content security policy exists to block. The file is copied
 * out of emoji-picker-element-data by scripts/sync-emoji-data.js, which runs on
 * every build so it cannot drift from the installed package.
 */
export const EMOJI_DATA_SOURCE = '/emoji/data.json';

/**
 * Poll cadence.
 *
 * Vercel serverless rules out WebSockets, and SSE would pin an invocation per
 * open tab while still polling Postgres underneath (Neon's pooled endpoint has
 * no LISTEN/NOTIFY). For ten people in one channel, a cursor poll that usually
 * returns zero rows is both cheaper and far less machinery.
 */
export const POLL_ACTIVE_MS = 3_000;
export const POLL_IDLE_MS = 10_000;
export const POLL_DORMANT_MS = 30_000;

/** Activity younger than this counts as "someone is talking right now". */
export const ACTIVE_WINDOW_MS = 2 * 60 * 1000;
export const IDLE_WINDOW_MS = 10 * 60 * 1000;

export const POLL_ERROR_BASE_MS = 2_000;
export const POLL_ERROR_MAX_MS = 60_000;
/** Consecutive failures before the UI admits it is disconnected. */
export const FAILURES_BEFORE_RECONNECTING = 3;

/** A local mutation schedules a near-immediate poll: if you are typing, so is someone else. */
export const POLL_AFTER_MUTATION_MS = 400;

/** Safety valve for an optimistic reaction whose confirming poll never arrives. */
export const PENDING_REACTION_TTL_MS = 30_000;

/** Debounce on the last-read watermark write. */
export const READ_DEBOUNCE_MS = 1_500;

/** How close to the bottom still counts as "at the bottom" for autoscroll. */
export const BOTTOM_THRESHOLD_PX = 48;
