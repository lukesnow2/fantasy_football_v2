/**
 * The chat wire format, shared by the API handlers and the client.
 *
 * Every timestamp is epoch milliseconds, not an ISO string. The chat tables store
 * `timestamptz` and the server converts on the way out, so the client never has
 * to guess a zone and `a.createdAt - b.createdAt` is always meaningful.
 */

export type EmojiType = 'unicode' | 'custom';

export interface ReactionUser {
	managerKey: number;
	name: string;
}

export interface ReactionGroup {
	emoji: string;
	emojiType: EmojiType;
	count: number;
	/** Whether the requesting manager is one of the reactors. Computed server-side. */
	mine: boolean;
	users: ReactionUser[];
}

export interface ChatMessageDTO {
	messageKey: number;
	messageId: string;
	content: string;
	channelId: string;
	parentMessageKey: number | null;
	messageType: string;
	authorKey: number;
	authorName: string | null;
	authorDisplayName: string;
	authorProfileImage: string | null;
	/** Manager keys mentioned in the content, resolved server-side. */
	mentions: number[];
	createdAt: number;
	updatedAt: number;
	editedAt: number | null;
	deletedAt: number | null;
	reactions: ReactionGroup[];
	replyCount: number;
	lastReplyAt: number | null;
}

/** How a locally-created message is faring on its way to the server. */
export type SendState = 'pending' | 'sent' | 'failed';

export interface ChatMessageView extends Omit<ChatMessageDTO, 'messageKey'> {
	/** Null until the server has confirmed the insert. */
	messageKey: number | null;
	sendState: SendState;
	failureReason?: string;
}

export interface RosterMember {
	managerKey: number;
	name: string;
	displayName: string;
	profileImageUrl: string | null;
}

export interface ChatCursor {
	messageKey: number;
	/** Server clock, epoch ms. Echoed back verbatim as `sinceTs` — never recomputed. */
	ts: number;
}

export interface ChatBootstrapResponse {
	mode: 'bootstrap';
	channelId: string;
	messages: ChatMessageDTO[];
	hasMore: boolean;
	roster: RosterMember[];
	lastReadMessageKey: number | null;
	cursor: ChatCursor;
	serverTime: number;
	me: { managerKey: number; displayName: string };
}

export interface ChatDeltaResponse {
	mode: 'delta';
	channelId: string;
	messages: ChatMessageDTO[];
	/** Complete reaction state for the newest window of root messages, keyed by messageKey. */
	reactions: Record<string, ReactionGroup[]>;
	cursor: ChatCursor;
	serverTime: number;
}

export interface ChatHistoryResponse {
	mode: 'history';
	channelId: string;
	messages: ChatMessageDTO[];
	hasMore: boolean;
}

export interface ChatThreadResponse {
	mode: 'thread';
	channelId: string;
	parentMessageKey: number;
	messages: ChatMessageDTO[];
}

export interface ReactionToggleResponse {
	messageKey: number;
	/** True when the caller now holds this reaction, false when it was removed. */
	mine: boolean;
	reactions: ReactionGroup[];
}
