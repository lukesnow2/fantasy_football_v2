import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

export interface RateLimitConfig {
	// Authentication attempts
	maxAuthAttemptsPerMinute: number;
	maxAuthAttemptsPerHour: number;
	maxAuthAttemptsPerDay: number;
	
	// Registration attempts
	maxRegistrationAttemptsPerHour: number;
	maxRegistrationAttemptsPerDay: number;
	
	// Challenge generation
	maxChallengesPerMinute: number;
	maxChallengesPerHour: number;
	
	// IP-based limits
	maxAttemptsPerIpPerMinute: number;
	maxAttemptsPerIpPerHour: number;
	
	// User-based limits
	maxAttemptsPerUserPerMinute: number;
	maxAttemptsPerUserPerHour: number;
	
	// Blocking durations
	blockDurationMinutes: number;
	extendedBlockDurationHours: number;
}

export const defaultRateLimitConfig: RateLimitConfig = {
	maxAuthAttemptsPerMinute: 5,
	maxAuthAttemptsPerHour: 20,
	maxAuthAttemptsPerDay: 100,
	maxRegistrationAttemptsPerHour: 3,
	maxRegistrationAttemptsPerDay: 10,
	maxChallengesPerMinute: 10,
	maxChallengesPerHour: 50,
	maxAttemptsPerIpPerMinute: 10,
	maxAttemptsPerIpPerHour: 100,
	maxAttemptsPerUserPerMinute: 3,
	maxAttemptsPerUserPerHour: 15,
	blockDurationMinutes: 15,
	extendedBlockDurationHours: 24
};

export interface RateLimitResult {
	allowed: boolean;
	remaining: number;
	resetTime: Date;
	retryAfter?: number; // seconds
	blocked?: boolean;
	blockExpiry?: Date;
}

export interface RateLimitEntry {
	id: string;
	key: string; // IP address or user ID
	type: 'ip' | 'user';
	action: 'auth' | 'registration' | 'challenge';
	count: number;
	windowStart: Date;
	windowEnd: Date;
	blocked: boolean;
	blockExpiry?: Date;
	createdAt: Date;
	updatedAt: Date;
}

// In-memory rate limit store (in production, use Redis or database)
const rateLimitStore = new Map<string, RateLimitEntry>();

export async function checkRateLimit(
	key: string,
	type: 'ip' | 'user',
	action: 'auth' | 'registration' | 'challenge',
	config: RateLimitConfig = defaultRateLimitConfig
): Promise<RateLimitResult> {
	const now = new Date();
	const storeKey = `${type}:${key}:${action}`;
	
	// Get or create rate limit entry
	let entry = rateLimitStore.get(storeKey);
	
	if (!entry) {
		entry = {
			id: crypto.randomUUID(),
			key,
			type,
			action,
			count: 0,
			windowStart: now,
			windowEnd: new Date(now.getTime() + 60 * 1000), // 1 minute window
			blocked: false,
			createdAt: now,
			updatedAt: now
		};
		rateLimitStore.set(storeKey, entry);
	}

	// Check if currently blocked
	if (entry.blocked && entry.blockExpiry && now < entry.blockExpiry) {
		const retryAfter = Math.ceil((entry.blockExpiry.getTime() - now.getTime()) / 1000);
		
		await logAuditEvent(
			AuditEventType.RATE_LIMIT_TRIGGERED,
			AuditSeverity.WARNING,
			{
				reason: 'rate_limit_blocked',
				key,
				type,
				action,
				blockExpiry: entry.blockExpiry,
				retryAfter
			},
			{ ipAddress: type === 'ip' ? key : undefined, userId: type === 'user' ? key : undefined }
		);

		return {
			allowed: false,
			remaining: 0,
			resetTime: entry.blockExpiry,
			retryAfter,
			blocked: true,
			blockExpiry: entry.blockExpiry
		};
	}

	// Check if window has expired
	if (now >= entry.windowEnd) {
		// Reset for new window
		entry.count = 0;
		entry.windowStart = now;
		entry.windowEnd = new Date(now.getTime() + 60 * 1000);
		entry.blocked = false;
		entry.blockExpiry = undefined;
	}

	// Get limits for this action
	const limits = getLimitsForAction(action, config);
	const currentLimit = getCurrentLimit(entry, limits, now);
	
	// Check if limit exceeded
	if (entry.count >= currentLimit.limit) {
		// Determine block duration
		const blockDuration = entry.count >= currentLimit.limit * 2 ? 
			config.extendedBlockDurationHours * 60 * 60 * 1000 : 
			config.blockDurationMinutes * 60 * 1000;
		
		entry.blocked = true;
		entry.blockExpiry = new Date(now.getTime() + blockDuration);
		
		await logAuditEvent(
			AuditEventType.RATE_LIMIT_TRIGGERED,
			AuditSeverity.WARNING,
			{
				reason: 'rate_limit_exceeded',
				key,
				type,
				action,
				count: entry.count,
				limit: currentLimit.limit,
				window: currentLimit.window,
				blockDuration: blockDuration / 1000
			},
			{ ipAddress: type === 'ip' ? key : undefined, userId: type === 'user' ? key : undefined }
		);

		return {
			allowed: false,
			remaining: 0,
			resetTime: entry.blockExpiry,
			retryAfter: Math.ceil(blockDuration / 1000),
			blocked: true,
			blockExpiry: entry.blockExpiry
		};
	}

	// Increment count
	entry.count++;
	entry.updatedAt = now;
	rateLimitStore.set(storeKey, entry);

	return {
		allowed: true,
		remaining: currentLimit.limit - entry.count,
		resetTime: entry.windowEnd
	};
}

function getLimitsForAction(action: 'auth' | 'registration' | 'challenge', config: RateLimitConfig) {
	switch (action) {
		case 'auth':
			return {
				perMinute: config.maxAuthAttemptsPerMinute,
				perHour: config.maxAuthAttemptsPerHour,
				perDay: config.maxAuthAttemptsPerDay
			};
		case 'registration':
			return {
				perMinute: config.maxRegistrationAttemptsPerHour / 60, // Convert to per minute
				perHour: config.maxRegistrationAttemptsPerHour,
				perDay: config.maxRegistrationAttemptsPerDay
			};
		case 'challenge':
			return {
				perMinute: config.maxChallengesPerMinute,
				perHour: config.maxChallengesPerHour,
				perDay: config.maxChallengesPerHour * 24 // Estimate daily limit
			};
	}
}

function getCurrentLimit(entry: RateLimitEntry, limits: any, now: Date) {
	const windowAge = now.getTime() - entry.windowStart.getTime();
	const windowMinutes = windowAge / (60 * 1000);
	const windowHours = windowAge / (60 * 60 * 1000);
	const windowDays = windowAge / (24 * 60 * 60 * 1000);

	// Determine which limit to apply based on window size
	if (windowMinutes < 1) {
		return { limit: limits.perMinute, window: '1 minute' };
	} else if (windowHours < 1) {
		return { limit: limits.perHour, window: '1 hour' };
	} else if (windowDays < 1) {
		return { limit: limits.perDay, window: '1 day' };
	} else {
		return { limit: limits.perDay, window: '1 day' };
	}
}

export async function checkMultiLevelRateLimit(
	ipAddress: string,
	userId?: string,
	action: 'auth' | 'registration' | 'challenge' = 'auth',
	config: RateLimitConfig = defaultRateLimitConfig
): Promise<RateLimitResult> {
	// Check IP-based rate limit
	const ipResult = await checkRateLimit(ipAddress, 'ip', action, config);
	if (!ipResult.allowed) {
		return ipResult;
	}

	// Check user-based rate limit if user ID provided
	if (userId) {
		const userResult = await checkRateLimit(userId, 'user', action, config);
		if (!userResult.allowed) {
			return userResult;
		}

		// Return the more restrictive result
		return {
			allowed: true,
			remaining: Math.min(ipResult.remaining, userResult.remaining),
			resetTime: ipResult.resetTime < userResult.resetTime ? ipResult.resetTime : userResult.resetTime
		};
	}

	return ipResult;
}

export function getRateLimitHeaders(result: RateLimitResult): Record<string, string> {
	const headers: Record<string, string> = {
		'X-RateLimit-Remaining': result.remaining.toString(),
		'X-RateLimit-Reset': result.resetTime.toISOString()
	};

	if (result.retryAfter) {
		headers['Retry-After'] = result.retryAfter.toString();
	}

	if (result.blocked) {
		headers['X-RateLimit-Blocked'] = 'true';
		if (result.blockExpiry) {
			headers['X-RateLimit-Block-Expiry'] = result.blockExpiry.toISOString();
		}
	}

	return headers;
}

export async function cleanupExpiredRateLimits(): Promise<number> {
	const now = new Date();
	let cleanedCount = 0;

	for (const [key, entry] of rateLimitStore.entries()) {
		// Remove entries older than 24 hours
		if (now.getTime() - entry.updatedAt.getTime() > 24 * 60 * 60 * 1000) {
			rateLimitStore.delete(key);
			cleanedCount++;
		}
	}

	return cleanedCount;
}

export function getRateLimitStats(): {
	totalEntries: number;
	blockedEntries: number;
	ipEntries: number;
	userEntries: number;
} {
	let blockedCount = 0;
	let ipCount = 0;
	let userCount = 0;

	for (const entry of rateLimitStore.values()) {
		if (entry.blocked) blockedCount++;
		if (entry.type === 'ip') ipCount++;
		if (entry.type === 'user') userCount++;
	}

	return {
		totalEntries: rateLimitStore.size,
		blockedEntries: blockedCount,
		ipEntries: ipCount,
		userEntries: userCount
	};
} 