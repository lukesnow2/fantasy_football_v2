import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

export interface TimeoutConfig {
	// Challenge timeouts
	challengeTimeoutMs: number;
	challengeCleanupIntervalMs: number;
	
	// Authentication timeouts
	authTimeoutMs: number;
	authCleanupIntervalMs: number;
	
	// Registration timeouts
	registrationTimeoutMs: number;
	registrationCleanupIntervalMs: number;
	
	// Session timeouts
	sessionTimeoutMs: number;
	sessionCleanupIntervalMs: number;
	
	// General timeouts
	requestTimeoutMs: number;
	responseTimeoutMs: number;
	
	// Clock skew tolerance
	clockSkewToleranceMs: number;
}

export const defaultTimeoutConfig: TimeoutConfig = {
	challengeTimeoutMs: 1000 * 60 * 5, // 5 minutes
	challengeCleanupIntervalMs: 1000 * 60 * 10, // 10 minutes
	authTimeoutMs: 1000 * 60 * 2, // 2 minutes
	authCleanupIntervalMs: 1000 * 60 * 5, // 5 minutes
	registrationTimeoutMs: 1000 * 60 * 10, // 10 minutes
	registrationCleanupIntervalMs: 1000 * 60 * 15, // 15 minutes
	sessionTimeoutMs: 1000 * 60 * 60 * 24 * 30, // 30 days
	sessionCleanupIntervalMs: 1000 * 60 * 60, // 1 hour
	requestTimeoutMs: 1000 * 30, // 30 seconds
	responseTimeoutMs: 1000 * 60, // 60 seconds
	clockSkewToleranceMs: 1000 * 60 * 5 // 5 minutes
};

export interface TimeoutResult {
	timedOut: boolean;
	elapsedMs: number;
	timeoutMs: number;
	error?: string;
}

export class TimeoutManager {
	private config: TimeoutConfig;
	private cleanupIntervals: Map<string, NodeJS.Timeout> = new Map();

	constructor(config: TimeoutConfig = defaultTimeoutConfig) {
		this.config = config;
		this.startCleanupIntervals();
	}

	private startCleanupIntervals(): void {
		// Challenge cleanup
		this.cleanupIntervals.set('challenge', setInterval(
			() => this.cleanupExpiredChallenges(),
			this.config.challengeCleanupIntervalMs
		));

		// Session cleanup
		this.cleanupIntervals.set('session', setInterval(
			() => this.cleanupExpiredSessions(),
			this.config.sessionCleanupIntervalMs
		));
	}

	async checkChallengeTimeout(
		challengeId: string,
		createdAt: Date,
		context?: { userId?: string; ipAddress?: string }
	): Promise<TimeoutResult> {
		const now = new Date();
		const elapsedMs = now.getTime() - createdAt.getTime();
		const timedOut = elapsedMs > this.config.challengeTimeoutMs;

		if (timedOut) {
			await logAuditEvent(
				AuditEventType.CHALLENGE_EXPIRED,
				AuditSeverity.WARNING,
				{
					reason: 'timeout',
					challengeId,
					elapsedMs,
					timeoutMs: this.config.challengeTimeoutMs
				},
				context
			);
		}

		return {
			timedOut,
			elapsedMs,
			timeoutMs: this.config.challengeTimeoutMs,
			error: timedOut ? 'Challenge has timed out' : undefined
		};
	}

	async checkAuthTimeout(
		authId: string,
		startedAt: Date,
		context?: { userId?: string; ipAddress?: string }
	): Promise<TimeoutResult> {
		const now = new Date();
		const elapsedMs = now.getTime() - startedAt.getTime();
		const timedOut = elapsedMs > this.config.authTimeoutMs;

		if (timedOut) {
			await logAuditEvent(
				AuditEventType.AUTHENTICATION_FAILED,
				AuditSeverity.WARNING,
				{
					reason: 'timeout',
					authId,
					elapsedMs,
					timeoutMs: this.config.authTimeoutMs
				},
				context
			);
		}

		return {
			timedOut,
			elapsedMs,
			timeoutMs: this.config.authTimeoutMs,
			error: timedOut ? 'Authentication has timed out' : undefined
		};
	}

	async checkRegistrationTimeout(
		registrationId: string,
		startedAt: Date,
		context?: { userId?: string; ipAddress?: string }
	): Promise<TimeoutResult> {
		const now = new Date();
		const elapsedMs = now.getTime() - startedAt.getTime();
		const timedOut = elapsedMs > this.config.registrationTimeoutMs;

		if (timedOut) {
			await logAuditEvent(
				AuditEventType.REGISTRATION_FAILED,
				AuditSeverity.WARNING,
				{
					reason: 'timeout',
					registrationId,
					elapsedMs,
					timeoutMs: this.config.registrationTimeoutMs
				},
				context
			);
		}

		return {
			timedOut,
			elapsedMs,
			timeoutMs: this.config.registrationTimeoutMs,
			error: timedOut ? 'Registration has timed out' : undefined
		};
	}

	async checkSessionTimeout(
		sessionId: string,
		createdAt: Date,
		context?: { userId?: string; ipAddress?: string }
	): Promise<TimeoutResult> {
		const now = new Date();
		const elapsedMs = now.getTime() - createdAt.getTime();
		const timedOut = elapsedMs > this.config.sessionTimeoutMs;

		if (timedOut) {
			await logAuditEvent(
				AuditEventType.SESSION_EXPIRED,
				AuditSeverity.INFO,
				{
					reason: 'timeout',
					sessionId,
					elapsedMs,
					timeoutMs: this.config.sessionTimeoutMs
				},
				context
			);
		}

		return {
			timedOut,
			elapsedMs,
			timeoutMs: this.config.sessionTimeoutMs,
			error: timedOut ? 'Session has expired' : undefined
		};
	}

	async validateClockSkew(
		clientTime: Date,
		context?: { userId?: string; ipAddress?: string }
	): Promise<{ valid: boolean; skewMs: number; error?: string }> {
		const serverTime = new Date();
		const skewMs = Math.abs(serverTime.getTime() - clientTime.getTime());
		const valid = skewMs <= this.config.clockSkewToleranceMs;

		if (!valid) {
			await logAuditEvent(
				AuditEventType.SECURITY_VIOLATION,
				AuditSeverity.WARNING,
				{
					reason: 'clock_skew',
					clientTime: clientTime.toISOString(),
					serverTime: serverTime.toISOString(),
					skewMs,
					toleranceMs: this.config.clockSkewToleranceMs
				},
				context
			);
		}

		return {
			valid,
			skewMs,
			error: valid ? undefined : `Clock skew too large: ${skewMs}ms`
		};
	}

	async withTimeout<T>(
		promise: Promise<T>,
		timeoutMs: number,
		operation: string,
		context?: { userId?: string; ipAddress?: string }
	): Promise<T> {
		const timeoutPromise = new Promise<never>((_, reject) => {
			setTimeout(() => {
				reject(new Error(`${operation} timed out after ${timeoutMs}ms`));
			}, timeoutMs);
		});

		try {
			const result = await Promise.race([promise, timeoutPromise]);
			return result;
		} catch (error) {
			await logAuditEvent(
				AuditEventType.SYSTEM_ERROR,
				AuditSeverity.ERROR,
				{
					reason: 'operation_timeout',
					operation,
					timeoutMs,
					error: error instanceof Error ? error.message : String(error)
				},
				context
			);
			throw error;
		}
	}

	private async cleanupExpiredChallenges(): Promise<void> {
		// This would clean up expired challenges from the database
		// Implementation depends on your database schema
		console.log('Cleaning up expired challenges...');
	}

	private async cleanupExpiredSessions(): Promise<void> {
		// This would clean up expired sessions from the database
		// Implementation depends on your database schema
		console.log('Cleaning up expired sessions...');
	}

	dispose(): void {
		// Clear all cleanup intervals
		for (const interval of this.cleanupIntervals.values()) {
			clearInterval(interval);
		}
		this.cleanupIntervals.clear();
	}
}

// Global timeout manager instance
export const timeoutManager = new TimeoutManager();

// Convenience functions
export async function checkTimeout(
	startTime: Date,
	timeoutMs: number,
	operation: string,
	context?: { userId?: string; ipAddress?: string }
): Promise<TimeoutResult> {
	const now = new Date();
	const elapsedMs = now.getTime() - startTime.getTime();
	const timedOut = elapsedMs > timeoutMs;

	if (timedOut) {
		await logAuditEvent(
			AuditEventType.SYSTEM_ERROR,
			AuditSeverity.WARNING,
			{
				reason: 'operation_timeout',
				operation,
				elapsedMs,
				timeoutMs
			},
			context
		);
	}

	return {
		timedOut,
		elapsedMs,
		timeoutMs,
		error: timedOut ? `${operation} has timed out` : undefined
	};
}

export function createTimeoutPromise<T>(
	promise: Promise<T>,
	timeoutMs: number,
	errorMessage: string
): Promise<T> {
	return Promise.race([
		promise,
		new Promise<never>((_, reject) => {
			setTimeout(() => reject(new Error(errorMessage)), timeoutMs);
		})
	]);
} 