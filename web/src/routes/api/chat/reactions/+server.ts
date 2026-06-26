import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { chatReaction, chatMessage, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, sql } from 'drizzle-orm';
import { getCurrentManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';

// Resolve the authenticated user's manager key, or null if not authenticated.
// Callers must return 401 on null — never fall back to a hardcoded key.
async function resolveManagerKey(locals: App.Locals): Promise<number | null> {
	if (!locals.user) return null;
	try {
		return (await getCurrentManagerKey(locals.user as any)) ?? null;
	} catch {
		return null;
	}
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const messageId = url.searchParams.get('messageId');
		
		if (!messageId) {
			return json({ error: 'Message ID is required' }, { status: 400 });
		}
		
		// Get the message key first
		const message = await db
			.select({ messageKey: chatMessage.messageKey })
			.from(chatMessage)
			.where(eq(chatMessage.messageId, messageId))
			.limit(1);
			
		if (message.length === 0) {
			return json({ error: 'Message not found' }, { status: 404 });
		}
		
		// Get all reactions for this message
		const reactions = await db
			.select({
				reactionKey: chatReaction.reactionKey,
				emoji: chatReaction.emoji,
				emojiType: chatReaction.emojiType,
				createdAt: chatReaction.createdAt,
				// Author information
				authorKey: chatReaction.authorKey,
				authorName: dimManager.managerName,
				authorDisplayName: dimManager.displayName,
				authorProfileImage: dimManager.profileImageUrl
			})
			.from(chatReaction)
			.leftJoin(dimManager, eq(chatReaction.authorKey, dimManager.managerKey))
			.where(eq(chatReaction.messageKey, message[0].messageKey))
			.orderBy(desc(chatReaction.createdAt));
		
		// Group reactions by emoji
		const reactionGroups = reactions.reduce((acc, reaction) => {
			const key = reaction.emoji;
			if (!acc[key]) {
				acc[key] = {
					emoji: reaction.emoji,
					emojiType: reaction.emojiType,
					count: 0,
					users: []
				};
			}
			acc[key].count++;
			acc[key].users.push({
				authorKey: reaction.authorKey,
				authorName: reaction.authorName,
				authorDisplayName: reaction.authorDisplayName,
				authorProfileImage: reaction.authorProfileImage
			});
			return acc;
		}, {} as Record<string, any>);
		
		return json({ 
			reactions: Object.values(reactionGroups),
			totalCount: reactions.length
		});
	} catch (error) {
		console.error('Error fetching reactions:', error);
		return json({ error: 'Failed to fetch reactions' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	try {
		// Require an authenticated, manager-linked user.
		const managerKey = await resolveManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'Unauthorized' }, { status: 401 });
		}

		const body = await request.json();
		const { messageId, emoji, emojiType = 'unicode' } = body;
		
		// Validate required fields
		if (!messageId || !emoji) {
			return json({ error: 'Message ID and emoji are required' }, { status: 400 });
		}
		
		// Get the message key first
		const message = await db
			.select({ messageKey: chatMessage.messageKey })
			.from(chatMessage)
			.where(and(
				eq(chatMessage.messageId, messageId),
				sql`${chatMessage.deletedAt} IS NULL`
			))
			.limit(1);
			
		if (message.length === 0) {
			return json({ error: 'Message not found' }, { status: 404 });
		}
		
		// Check if user already reacted with this emoji
		const existingReaction = await db
			.select()
			.from(chatReaction)
			.where(and(
				eq(chatReaction.messageKey, message[0].messageKey),
				eq(chatReaction.authorKey, managerKey),
				eq(chatReaction.emoji, emoji)
			))
			.limit(1);
		
		if (existingReaction.length > 0) {
			return json({ error: 'You have already reacted with this emoji' }, { status: 400 });
		}
		
		// Add the reaction
		const [newReaction] = await db
			.insert(chatReaction)
			.values({
				messageKey: message[0].messageKey,
				authorKey: managerKey,
				emoji,
				emojiType
			})
			.returning();
		
		// Get the reaction with author info
		const reactionWithAuthor = await db
			.select({
				reactionKey: chatReaction.reactionKey,
				emoji: chatReaction.emoji,
				emojiType: chatReaction.emojiType,
				createdAt: chatReaction.createdAt,
				// Author information
				authorKey: chatReaction.authorKey,
				authorName: dimManager.managerName,
				authorDisplayName: dimManager.displayName,
				authorProfileImage: dimManager.profileImageUrl
			})
			.from(chatReaction)
			.leftJoin(dimManager, eq(chatReaction.authorKey, dimManager.managerKey))
			.where(eq(chatReaction.reactionKey, newReaction.reactionKey))
			.limit(1);
		
		return json({ reaction: reactionWithAuthor[0] });
	} catch (error) {
		console.error('Error adding reaction:', error);
		return json({ error: 'Failed to add reaction' }, { status: 500 });
	}
};

export const DELETE: RequestHandler = async ({ request, locals }) => {
	try {
		// Require an authenticated, manager-linked user.
		const managerKey = await resolveManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'Unauthorized' }, { status: 401 });
		}

		const body = await request.json();
		const { messageId, emoji } = body;
		
		// Validate required fields
		if (!messageId || !emoji) {
			return json({ error: 'Message ID and emoji are required' }, { status: 400 });
		}
		
		// Get the message key first
		const message = await db
			.select({ messageKey: chatMessage.messageKey })
			.from(chatMessage)
			.where(eq(chatMessage.messageId, messageId))
			.limit(1);
			
		if (message.length === 0) {
			return json({ error: 'Message not found' }, { status: 404 });
		}
		
		// Check if user has reacted with this emoji
		const existingReaction = await db
			.select()
			.from(chatReaction)
			.where(and(
				eq(chatReaction.messageKey, message[0].messageKey),
				eq(chatReaction.authorKey, managerKey),
				eq(chatReaction.emoji, emoji)
			))
			.limit(1);
		
		if (existingReaction.length === 0) {
			return json({ error: 'Reaction not found' }, { status: 404 });
		}
		
		// Remove the reaction
		await db
			.delete(chatReaction)
			.where(eq(chatReaction.reactionKey, existingReaction[0].reactionKey));
		
		return json({ message: 'Reaction removed successfully' });
	} catch (error) {
		console.error('Error removing reaction:', error);
		return json({ error: 'Failed to remove reaction' }, { status: 500 });
	}
}; 