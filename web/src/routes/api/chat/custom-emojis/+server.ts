import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { chatCustomEmoji, dimManager } from '$lib/server/db/schema';
import { eq, and, desc, sql } from 'drizzle-orm';
import { requireManagerKey } from '$lib/server/auth-manager';
import type { RequestHandler } from './$types';
import { nanoid } from 'nanoid';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const category = url.searchParams.get('category');
		const limit = parseInt(url.searchParams.get('limit') || '100');
		const offset = parseInt(url.searchParams.get('offset') || '0');
		
		let whereConditions = [
			eq(chatCustomEmoji.isActive, true)
		];
		
		if (category) {
			whereConditions.push(eq(chatCustomEmoji.category, category));
		}
		
		const customEmojis = await db
			.select({
				emojiKey: chatCustomEmoji.emojiKey,
				emojiId: chatCustomEmoji.emojiId,
				name: chatCustomEmoji.name,
				imageUrl: chatCustomEmoji.imageUrl,
				category: chatCustomEmoji.category,
				usageCount: chatCustomEmoji.usageCount,
				createdAt: chatCustomEmoji.createdAt,
				// Creator information
				createdBy: chatCustomEmoji.createdBy,
				creatorName: dimManager.managerName,
				creatorDisplayName: dimManager.displayName,
				creatorProfileImage: dimManager.profileImageUrl
			})
			.from(chatCustomEmoji)
			.leftJoin(dimManager, eq(chatCustomEmoji.createdBy, dimManager.managerKey))
			.where(and(...whereConditions))
			.orderBy(desc(chatCustomEmoji.usageCount), desc(chatCustomEmoji.createdAt))
			.limit(limit)
			.offset(offset);
		
		return json({ 
			emojis: customEmojis,
			hasMore: customEmojis.length === limit
		});
	} catch (error) {
		console.error('Error fetching custom emojis:', error);
		return json({ error: 'Failed to fetch custom emojis' }, { status: 500 });
	}
};

export const POST: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to create custom emojis' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = requireManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const body = await request.json();
		const { name, imageUrl, category = 'custom' } = body;
		
		// Validate required fields
		if (!name || !imageUrl) {
			return json({ error: 'Name and image URL are required' }, { status: 400 });
		}
		
		// Validate emoji name format (should be alphanumeric with underscores)
		if (!/^[a-zA-Z0-9_]+$/.test(name)) {
			return json({ error: 'Emoji name can only contain letters, numbers, and underscores' }, { status: 400 });
		}
		
		// Check if emoji name already exists
		const existingEmoji = await db
			.select()
			.from(chatCustomEmoji)
			.where(and(
				eq(chatCustomEmoji.name, name),
				eq(chatCustomEmoji.isActive, true)
			))
			.limit(1);
		
		if (existingEmoji.length > 0) {
			return json({ error: 'An emoji with this name already exists' }, { status: 400 });
		}
		
		// Generate unique emoji ID
		const emojiId = `emoji-${Date.now()}-${nanoid(8)}`;
		
		// Create the custom emoji
		const [newEmoji] = await db
			.insert(chatCustomEmoji)
			.values({
				emojiId,
				name,
				imageUrl,
				createdBy: managerKey,
				category
			})
			.returning();
		
		// Get the emoji with creator info
		const emojiWithCreator = await db
			.select({
				emojiKey: chatCustomEmoji.emojiKey,
				emojiId: chatCustomEmoji.emojiId,
				name: chatCustomEmoji.name,
				imageUrl: chatCustomEmoji.imageUrl,
				category: chatCustomEmoji.category,
				usageCount: chatCustomEmoji.usageCount,
				createdAt: chatCustomEmoji.createdAt,
				// Creator information
				createdBy: chatCustomEmoji.createdBy,
				creatorName: dimManager.managerName,
				creatorDisplayName: dimManager.displayName,
				creatorProfileImage: dimManager.profileImageUrl
			})
			.from(chatCustomEmoji)
			.leftJoin(dimManager, eq(chatCustomEmoji.createdBy, dimManager.managerKey))
			.where(eq(chatCustomEmoji.emojiKey, newEmoji.emojiKey))
			.limit(1);
		
		return json({ emoji: emojiWithCreator[0] });
	} catch (error) {
		console.error('Error creating custom emoji:', error);
		return json({ error: 'Failed to create custom emoji' }, { status: 500 });
	}
};

export const PUT: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to update custom emojis' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = requireManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const body = await request.json();
		const { emojiId, name, imageUrl, category, isActive } = body;
		
		// Validate required fields
		if (!emojiId) {
			return json({ error: 'Emoji ID is required' }, { status: 400 });
		}
		
		// Check if emoji exists and user owns it (or is admin)
		const existingEmoji = await db
			.select()
			.from(chatCustomEmoji)
			.where(and(
				eq(chatCustomEmoji.emojiId, emojiId),
				eq(chatCustomEmoji.createdBy, managerKey) // Only creator can edit for now
			))
			.limit(1);
			
		if (existingEmoji.length === 0) {
			return json({ error: 'Emoji not found or not authorized to edit' }, { status: 404 });
		}
		
		// Prepare update data
		const updateData: any = {
			updatedAt: new Date()
		};
		
		if (name !== undefined) {
			// Validate emoji name format
			if (!/^[a-zA-Z0-9_]+$/.test(name)) {
				return json({ error: 'Emoji name can only contain letters, numbers, and underscores' }, { status: 400 });
			}
			updateData.name = name;
		}
		
		if (imageUrl !== undefined) {
			updateData.imageUrl = imageUrl;
		}
		
		if (category !== undefined) {
			updateData.category = category;
		}
		
		if (isActive !== undefined) {
			updateData.isActive = isActive;
		}
		
		// Update the emoji
		const [updatedEmoji] = await db
			.update(chatCustomEmoji)
			.set(updateData)
			.where(eq(chatCustomEmoji.emojiId, emojiId))
			.returning();
		
		return json({ emoji: updatedEmoji });
	} catch (error) {
		console.error('Error updating custom emoji:', error);
		return json({ error: 'Failed to update custom emoji' }, { status: 500 });
	}
};

export const DELETE: RequestHandler = async ({ request, locals }) => {
	try {
		// Check if user is authenticated
		if (!locals.user) {
			return json({ error: 'Authentication required to delete custom emojis' }, { status: 401 });
		}

		// Get the authenticated manager key
		const managerKey = requireManagerKey(locals);
		if (!managerKey) {
			return json({ error: 'No manager profile linked to your account' }, { status: 400 });
		}

		const body = await request.json();
		const { emojiId } = body;
		
		// Validate required fields
		if (!emojiId) {
			return json({ error: 'Emoji ID is required' }, { status: 400 });
		}
		
		// Check if emoji exists and user owns it (or is admin)
		const existingEmoji = await db
			.select()
			.from(chatCustomEmoji)
			.where(and(
				eq(chatCustomEmoji.emojiId, emojiId),
				eq(chatCustomEmoji.createdBy, managerKey) // Only creator can delete for now
			))
			.limit(1);
			
		if (existingEmoji.length === 0) {
			return json({ error: 'Emoji not found or not authorized to delete' }, { status: 404 });
		}
		
		// Soft delete the emoji (set isActive to false)
		await db
			.update(chatCustomEmoji)
			.set({
				isActive: false,
				updatedAt: new Date()
			})
			.where(eq(chatCustomEmoji.emojiId, emojiId));
		
		return json({ message: 'Custom emoji deleted successfully' });
	} catch (error) {
		console.error('Error deleting custom emoji:', error);
		return json({ error: 'Failed to delete custom emoji' }, { status: 500 });
	}
}; 