import { db } from '$lib/server/db';
import { webauthnAuditLog } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export enum AuditEventType {
	// Registration events
	REGISTRATION_STARTED = 'REGISTRATION_STARTED',
	REGISTRATION_COMPLETED = 'REGISTRATION_COMPLETED',
	REGISTRATION_FAILED = 'REGISTRATION_FAILED',
	
	// Authentication events
	AUTHENTICATION_STARTED = 'AUTHENTICATION_STARTED',
	AUTHENTICATION_COMPLETED = 'AUTHENTICATION_COMPLETED',
	AUTHENTICATION_FAILED = 'AUTHENTICATION_FAILED',
	
	// Credential management events
	CREDENTIAL_DELETED = 'CREDENTIAL_DELETED',
	CREDENTIAL_ROTATED = 'CREDENTIAL_ROTATED',
	CREDENTIAL_LISTED = 'CREDENTIAL_LISTED',
	
	// Session events
	SESSION_CREATED = 'SESSION_CREATED',
	SESSION_VALIDATED = 'SESSION_VALIDATED',
	SESSION_EXPIRED = 'SESSION_EXPIRED',
	SESSION_INVALIDATED = 'SESSION_INVALIDATED',
	
	// Security events
	CSRF_TOKEN_CREATED = 'CSRF_TOKEN_CREATED',
	CSRF_TOKEN_VALIDATED = 'CSRF_TOKEN_VALIDATED',
	CSRF_TOKEN_INVALID = 'CSRF_TOKEN_INVALID',
	RATE_LIMIT_TRIGGERED = 'RATE_LIMIT_TRIGGERED',
	
	// Challenge events
	CHALLENGE_CREATED = 'CHALLENGE_CREATED',
	CHALLENGE_VALIDATED = 'CHALLENGE_VALIDATED',
	CHALLENGE_EXPIRED = 'CHALLENGE_EXPIRED',
	
	// Backup code events
	BACKUP_CODES_GENERATED = 'BACKUP_CODES_GENERATED',
	BACKUP_CODE_USED = 'BACKUP_CODE_USED',
	BACKUP_CODES_REGENERATED = 'BACKUP_CODES_REGENERATED',
	
	// Migration events
	USER_MIGRATED = 'USER_MIGRATED',
	MIGRATION_STARTED = 'MIGRATION_STARTED',
	MIGRATION_COMPLETED = 'MIGRATION_COMPLETED',
	
	// Error events
	SECURITY_VIOLATION = 'SECURITY_VIOLATION',
	SYSTEM_ERROR = 'SYSTEM_ERROR',
	CONFIGURATION_ERROR = 'CONFIGURATION_ERROR'
}

export enum AuditSeverity {
	INFO = 'INFO',
	WARNING = 'WARNING',
	ERROR = 'ERROR',
	CRITICAL = 'CRITICAL'
}

export interface AuditEvent {
	id: string;
	eventType: AuditEventType;
	severity: AuditSeverity;
	userId?: string | null;
	sessionId?: string | null;
	ipAddress?: string | null;
	userAgent?: string | null;
	deviceType?: string | null;
	details: Record<string, any>;
	timestamp: Date;
	requestId?: string | null;
	errorCode?: string | null;
	errorMessage?: string | null;
}

export interface AuditContext {
	userId?: string;
	sessionId?: string;
	ipAddress?: string;
	userAgent?: string;
	deviceType?: string;
	requestId?: string;
}

export async function logAuditEvent(
	eventType: AuditEventType,
	severity: AuditSeverity,
	details: Record<string, any>,
	context?: AuditContext,
	error?: Error
): Promise<void> {
	try {
		const now = new Date();
		const eventId = crypto.randomUUID();

		const auditEvent: AuditEvent = {
			id: eventId,
			eventType,
			severity,
			userId: context?.userId,
			sessionId: context?.sessionId,
			ipAddress: context?.ipAddress,
			userAgent: context?.userAgent,
			deviceType: context?.deviceType,
			details,
			timestamp: now,
			requestId: context?.requestId,
			errorCode: error?.name,
			errorMessage: error?.message
		};

		// Insert audit event into database
		await db.insert(webauthnAuditLog).values({
			id: auditEvent.id,
			eventType: auditEvent.eventType,
			severity: auditEvent.severity,
			userId: auditEvent.userId,
			sessionId: auditEvent.sessionId,
			ipAddress: auditEvent.ipAddress,
			userAgent: auditEvent.userAgent,
			deviceType: auditEvent.deviceType,
			details: JSON.stringify(auditEvent.details),
			timestamp: auditEvent.timestamp,
			requestId: auditEvent.requestId,
			errorCode: auditEvent.errorCode,
			errorMessage: auditEvent.errorMessage
		});

		console.log(`📊 Audit event logged: ${eventType} (${severity})`);
	} catch (error) {
		// Log the error but don't fail the main operation
		console.error('Failed to log audit event:', error);
		console.error('Audit event data:', {
			eventType,
			severity,
			details,
			context,
			error: error instanceof Error ? error.message : 'Unknown error'
		});
	}
}

// Convenience functions for common audit events
export async function logRegistrationEvent(
	userId: string,
	success: boolean,
	context?: AuditContext,
	error?: Error
): Promise<void> {
	const eventType = success ? AuditEventType.REGISTRATION_COMPLETED : AuditEventType.REGISTRATION_FAILED;
	const severity = success ? AuditSeverity.INFO : AuditSeverity.ERROR;
	
	await logAuditEvent(eventType, severity, {
		userId,
		success,
		credentialType: 'passkey'
	}, context, error);
}

export async function logAuthenticationEvent(
	userId: string,
	success: boolean,
	context?: AuditContext,
	error?: Error
): Promise<void> {
	const eventType = success ? AuditEventType.AUTHENTICATION_COMPLETED : AuditEventType.AUTHENTICATION_FAILED;
	const severity = success ? AuditSeverity.INFO : AuditSeverity.WARNING;
	
	await logAuditEvent(eventType, severity, {
		userId,
		success,
		authMethod: 'passkey'
	}, context, error);
}

export async function logSecurityViolation(
	violationType: string,
	details: Record<string, any>,
	context?: AuditContext
): Promise<void> {
	await logAuditEvent(
		AuditEventType.SECURITY_VIOLATION,
		AuditSeverity.CRITICAL,
		{
			violationType,
			...details
		},
		context
	);
}

export async function logRateLimitEvent(
	userId: string,
	limitType: string,
	context?: AuditContext
): Promise<void> {
	await logAuditEvent(
		AuditEventType.RATE_LIMIT_TRIGGERED,
		AuditSeverity.WARNING,
		{
			userId,
			limitType,
			action: 'blocked'
		},
		context
	);
}

// Audit query functions for monitoring
export async function getAuditEventsForUser(
	userId: string,
	limit: number = 100
): Promise<AuditEvent[]> {
	const events = await db
		.select()
		.from(webauthnAuditLog)
		.where(eq(webauthnAuditLog.userId, userId))
		.orderBy(webauthnAuditLog.timestamp)
		.limit(limit);

	return events.map(event => ({
		id: event.id,
		eventType: event.eventType as AuditEventType,
		severity: event.severity as AuditSeverity,
		userId: event.userId,
		sessionId: event.sessionId,
		ipAddress: event.ipAddress,
		userAgent: event.userAgent,
		deviceType: event.deviceType,
		details: event.details ? JSON.parse(event.details) : {},
		timestamp: event.timestamp || new Date(),
		requestId: event.requestId,
		errorCode: event.errorCode,
		errorMessage: event.errorMessage
	}));
}

export async function getSecurityViolations(
	hours: number = 24
): Promise<AuditEvent[]> {
	const cutoffTime = new Date(Date.now() - hours * 60 * 60 * 1000);
	
	const events = await db
		.select()
		.from(webauthnAuditLog)
		.where(eq(webauthnAuditLog.severity, AuditSeverity.CRITICAL))
		.orderBy(webauthnAuditLog.timestamp);

	return events
		.filter(event => event.timestamp && event.timestamp >= cutoffTime)
		.map(event => ({
			id: event.id,
			eventType: event.eventType as AuditEventType,
			severity: event.severity as AuditSeverity,
			userId: event.userId,
			sessionId: event.sessionId,
			ipAddress: event.ipAddress,
			userAgent: event.userAgent,
			deviceType: event.deviceType,
			details: event.details ? JSON.parse(event.details) : {},
			timestamp: event.timestamp || new Date(),
			requestId: event.requestId,
			errorCode: event.errorCode,
			errorMessage: event.errorMessage
		}));
} 