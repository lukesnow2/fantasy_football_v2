import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { chatMessage, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, asc, sql } from 'drizzle-orm';
import { getCurrentManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';
import { nanoid } from 'nanoid';

// Resolve the authenticated user's manager key, or null if not authenticated /
// not linked to a manager. Callers must return 401 on null — never fall back to a
// hardcoded key (which previously let any unauthenticated user post as manager 1).
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
		const channelId = url.searchParams.get('channelId') || 'general';
		const limit = parseInt(url.searchParams.get('limit') || '50');
		const offset = parseInt(url.searchParams.get('offset') || '0');
		const threadId = url.searchParams.get('threadId');
		
		let whereConditions = [
			eq(chatMessage.channelId, channelId),
			sql`${chatMessage.deletedAt} IS NULL`
		];
		
		if (threadId) {
			// Get messages in a specific thread
			whereConditions.push(eq(chatMessage.parentMessageKey, parseInt(threadId)));
		} else {
			// Get root messages (not replies)
			whereConditions.push(sql`${chatMessage.parentMessageKey} IS NULL`);
		}
		
		const messages = await db
			.select({
				messageKey: chatMessage.messageKey,
				messageId: chatMessage.messageId,
				content: chatMessage.content,
				channelId: chatMessage.channelId,
				parentMessageKey: chatMessage.parentMessageKey,
				messageType: chatMessage.messageType,
				attachments: chatMessage.attachments,
				mentions: chatMessage.mentions,
				editedAt: chatMessage.editedAt,
				createdAt: chatMessage.createdAt,
				updatedAt: chatMessage.updatedAt,
				// Author information
				authorKey: chatMessage.authorKey,
				authorName: dimManager.managerName,
				authorDisplayName: dimManager.displayName,
				authorProfileImage: dimManager.profileImageUrl
			})
			.from(chatMessage)
			.leftJoin(dimManager, eq(chatMessage.authorKey, dimManager.managerKey))
			.where(and(...whereConditions))
			.orderBy(desc(chatMessage.createdAt))
			.limit(limit)
			.offset(offset);
		
		return json({ 
			messages: messages.reverse(), // Reverse to get chronological order
			hasMore: messages.length === limit
		});
	} catch (error) {
		console.error('Error fetching chat messages:', error);
		return json({ error: 'Failed to fetch messages' }, { status: 500 });
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
		const { content, channelId = 'general', parentMessageKey, messageType = 'message', attachments, mentions } = body;
		
		// Validate required fields
		if (!content || content.trim() === '') {
			return json({ error: 'Message content is required' }, { status: 400 });
		}
		
		// Generate unique message ID
		const messageId = `msg-${Date.now()}-${nanoid(8)}`;
		
		// Create the message
		const [newMessage] = await db
			.insert(chatMessage)
			.values({
				messageId,
				content: content.trim(),
				authorKey: managerKey,
				channelId,
				parentMessageKey,
				messageType,
				attachments: attachments ? JSON.stringify(attachments) : null,
				mentions: mentions ? JSON.stringify(mentions) : null
			})
			.returning();
		
		// Get the complete message with author info
		const messageWithAuthor = await db
			.select({
				messageKey: chatMessage.messageKey,
				messageId: chatMessage.messageId,
				content: chatMessage.content,
				channelId: chatMessage.channelId,
				parentMessageKey: chatMessage.parentMessageKey,
				messageType: chatMessage.messageType,
				attachments: chatMessage.attachments,
				mentions: chatMessage.mentions,
				editedAt: chatMessage.editedAt,
				createdAt: chatMessage.createdAt,
				updatedAt: chatMessage.updatedAt,
				// Author information
				authorKey: chatMessage.authorKey,
				authorName: dimManager.managerName,
				authorDisplayName: dimManager.displayName,
				authorProfileImage: dimManager.profileImageUrl
			})
			.from(chatMessage)
			.leftJoin(dimManager, eq(chatMessage.authorKey, dimManager.managerKey))
			.where(eq(chatMessage.messageKey, newMessage.messageKey))
			.limit(1);
		
		return json({ message: messageWithAuthor[0] });
	} catch (error) {
		console.error('Error creating chat message:', error);
		return json({ error: 'Failed to create message' }, { status: 500 });
	}
};

export const PUT: RequestHandler = async ({ request, locals }) => {
	try {
		// Require an authenticated, manager-linked user.
		const managerKey = await resolveManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'Unauthorized' }, { status: 401 });
		}

		const body = await request.json();
		const { messageId, content } = body;
		
		// Validate required fields
		if (!messageId || !content || content.trim() === '') {
			return json({ error: 'Message ID and content are required' }, { status: 400 });
		}
		
		// Check if message exists and user owns it
		const existingMessage = await db
			.select()
			.from(chatMessage)
			.where(and(
				eq(chatMessage.messageId, messageId),
				eq(chatMessage.authorKey, managerKey),
				sql`${chatMessage.deletedAt} IS NULL`
			))
			.limit(1);
			
		if (existingMessage.length === 0) {
			return json({ error: 'Message not found or not authorized to edit' }, { status: 404 });
		}
		
		// Update the message
		const [updatedMessage] = await db
			.update(chatMessage)
			.set({
				content: content.trim(),
				editedAt: new Date(),
				updatedAt: new Date()
			})
			.where(eq(chatMessage.messageId, messageId))
			.returning();
		
		return json({ message: updatedMessage });
	} catch (error) {
		console.error('Error updating chat message:', error);
		return json({ error: 'Failed to update message' }, { status: 500 });
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
		const { messageId } = body;
		
		// Validate required fields
		if (!messageId) {
			return json({ error: 'Message ID is required' }, { status: 400 });
		}
		
		// Check if message exists and user owns it
		const existingMessage = await db
			.select()
			.from(chatMessage)
			.where(and(
				eq(chatMessage.messageId, messageId),
				eq(chatMessage.authorKey, managerKey),
				sql`${chatMessage.deletedAt} IS NULL`
			))
			.limit(1);
			
		if (existingMessage.length === 0) {
			return json({ error: 'Message not found or not authorized to delete' }, { status: 404 });
		}
		
		// Soft delete the message
		const [deletedMessage] = await db
			.update(chatMessage)
			.set({
				deletedAt: new Date(),
				updatedAt: new Date()
			})
			.where(eq(chatMessage.messageId, messageId))
			.returning();
		
		return json({ message: 'Message deleted successfully' });
	} catch (error) {
		console.error('Error deleting chat message:', error);
		return json({ error: 'Failed to delete message' }, { status: 500 });
	}
}; 