// Client-side types for chat components
export interface ChatMessage {
	messageId: string;
	messageKey: number; // Add for compatibility
	content: string;
	messageType: 'message' | 'system' | 'announcement';
	channelId: string;
	authorManagerKey: number;
	authorKey: number; // Add for compatibility - same as authorManagerKey
	parentMessageKey?: number;
	mentions?: string[];
	attachments?: string[];
	createdAt: string; // Use string for dates on client side
	editedAt?: string;
	deletedAt?: string;
	
	// Extended properties from joins
	authorName?: string;
	authorDisplayName?: string;
	authorProfileImage?: string;
	reactions?: ChatReaction[];
	replyCount?: number;
}

export interface ChatReaction {
	reactionId: string;
	messageId: string;
	emoji: string;
	emojiType: 'unicode' | 'custom';
	reactorManagerKey: number;
	createdAt: string; // Use string for dates on client side
	
	// Extended properties
	reactorName?: string;
	users?: Array<{
		managerKey: number;
		name: string;
	}>;
	count?: number;
}

export interface ChatThread {
	threadId: string;
	rootMessageKey: number;
	channelId: string;
	title?: string;
	isLocked: boolean;
	participantCount: number;
	lastActivityAt: string;
	createdAt: string;
}

export interface ChatCustomEmoji {
	emojiId: string;
	name: string;
	imageUrl: string;
	category?: string;
	creatorManagerKey: number;
	isActive: boolean;
	usageCount: number;
	createdAt: string;
} 