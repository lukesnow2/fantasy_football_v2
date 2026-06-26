import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

export interface VerificationAttempt {
	id: string;
	userId?: string;
	ipAddress?: string;
	userAgent?: string;
	deviceType?: string;
	attemptType: 'registration' | 'authentication' | 'challenge' | 'attestation';
	success: boolean;
	timestamp: Date;
	errorCode?: string;
	errorMessage?: string;
	details?: Record<string, any>;
}

export interface FraudIndicator {
	type: 'multiple_failures' | 'unusual_location' | 'suspicious_timing' | 'invalid_credentials';
	severity: 'low' | 'medium' | 'high' | 'critical';
	details: Record<string, any>;
	timestamp: Date;
}

export interface MonitoringConfig {
	maxFailuresPerHour: number;
	maxFailuresPerDay: number;
	suspiciousIpThreshold: number;
	unusualTimingThreshold: number; // milliseconds
	enableFraudDetection: boolean;
	alertThresholds: {
		critical: number;
		high: number;
		medium: number;
		low: number;
	};
}

export const defaultMonitoringConfig: MonitoringConfig = {
	maxFailuresPerHour: 5,
	maxFailuresPerDay: 20,
	suspiciousIpThreshold: 10,
	unusualTimingThreshold: 1000, // 1 second
	enableFraudDetection: true,
	alertThresholds: {
		critical: 10,
		high: 5,
		medium: 3,
		low: 1
	}
};

export async function logVerificationAttempt(
	attempt: Omit<VerificationAttempt, 'id' | 'timestamp'>,
	config: MonitoringConfig = defaultMonitoringConfig
): Promise<void> {
	const verificationAttempt: VerificationAttempt = {
		...attempt,
		id: crypto.randomUUID(),
		timestamp: new Date()
	};

	// Log the attempt
	await logAuditEvent(
		attempt.success ? AuditEventType.AUTHENTICATION_COMPLETED : AuditEventType.AUTHENTICATION_FAILED,
		attempt.success ? AuditSeverity.INFO : AuditSeverity.WARNING,
		{
			attemptType: attempt.attemptType,
			success: attempt.success,
			errorCode: attempt.errorCode,
			errorMessage: attempt.errorMessage,
			details: attempt.details
		},
		{
			userId: attempt.userId,
			ipAddress: attempt.ipAddress,
			deviceType: attempt.deviceType
		}
	);

	// Check for fraud indicators if enabled
	if (config.enableFraudDetection && !attempt.success) {
		await checkForFraudIndicators(verificationAttempt, config);
	}
}

export async function checkForFraudIndicators(
	attempt: VerificationAttempt,
	config: MonitoringConfig
): Promise<FraudIndicator[]> {
	const indicators: FraudIndicator[] = [];
	const now = new Date();
	const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
	const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

	// Check for multiple failures from same IP
	if (attempt.ipAddress) {
		// This would typically query a database for recent failures
		// For now, we'll simulate the check
		const recentFailures = await getRecentFailuresByIp(attempt.ipAddress, oneHourAgo);
		
		if (recentFailures.length >= config.maxFailuresPerHour) {
			indicators.push({
				type: 'multiple_failures',
				severity: recentFailures.length >= config.alertThresholds.critical ? 'critical' : 
						 recentFailures.length >= config.alertThresholds.high ? 'high' : 
						 recentFailures.length >= config.alertThresholds.medium ? 'medium' : 'low',
				details: {
					ipAddress: attempt.ipAddress,
					failureCount: recentFailures.length,
					timeWindow: '1 hour',
					threshold: config.maxFailuresPerHour
				},
				timestamp: now
			});
		}
	}

	// Check for unusual timing patterns
	if (attempt.userId) {
		const recentAttempts = await getRecentAttemptsByUser(attempt.userId, oneHourAgo);
		
		if (recentAttempts.length > 1) {
			const lastAttempt = recentAttempts[recentAttempts.length - 2];
			const timeDiff = now.getTime() - lastAttempt.timestamp.getTime();
			
			if (timeDiff < config.unusualTimingThreshold) {
				indicators.push({
					type: 'suspicious_timing',
					severity: 'medium',
					details: {
						userId: attempt.userId,
						timeDiff,
						threshold: config.unusualTimingThreshold,
						attemptCount: recentAttempts.length
					},
					timestamp: now
				});
			}
		}
	}

	// Log fraud indicators
	for (const indicator of indicators) {
		await logAuditEvent(
			AuditEventType.SECURITY_VIOLATION,
			indicator.severity === 'critical' ? AuditSeverity.CRITICAL :
			indicator.severity === 'high' ? AuditSeverity.ERROR :
			indicator.severity === 'medium' ? AuditSeverity.WARNING : AuditSeverity.INFO,
			{
				fraudType: indicator.type,
				severity: indicator.severity,
				details: indicator.details
			},
			{
				userId: attempt.userId,
				ipAddress: attempt.ipAddress
			}
		);
	}

	return indicators;
}

// Mock functions for demonstration - in production these would query the database
async function getRecentFailuresByIp(ipAddress: string, since: Date): Promise<VerificationAttempt[]> {
	// This would query the audit log for recent failures from this IP
	// For now, return empty array
	return [];
}

async function getRecentAttemptsByUser(userId: string, since: Date): Promise<VerificationAttempt[]> {
	// This would query the audit log for recent attempts by this user
	// For now, return empty array
	return [];
}

export async function logFailedVerification(
	attemptType: 'registration' | 'authentication' | 'challenge' | 'attestation',
	errorCode: string,
	errorMessage: string,
	context: {
		userId?: string;
		ipAddress?: string;
		userAgent?: string;
		deviceType?: string;
		details?: Record<string, any>;
	}
): Promise<void> {
	await logVerificationAttempt({
		attemptType,
		success: false,
		errorCode,
		errorMessage,
		...context
	});
}

export async function logSuccessfulVerification(
	attemptType: 'registration' | 'authentication' | 'challenge' | 'attestation',
	context: {
		userId?: string;
		ipAddress?: string;
		userAgent?: string;
		deviceType?: string;
		details?: Record<string, any>;
	}
): Promise<void> {
	await logVerificationAttempt({
		attemptType,
		success: true,
		...context
	});
}

export function generateMonitoringReport(
	startDate: Date,
	endDate: Date
): Promise<{
	totalAttempts: number;
	successfulAttempts: number;
	failedAttempts: number;
	successRate: number;
	fraudIndicators: number;
	topFailureReasons: Array<{ reason: string; count: number }>;
	topIpAddresses: Array<{ ip: string; attempts: number; failures: number }>;
}> {
	// This would generate a comprehensive monitoring report
	// For now, return a mock report
	return Promise.resolve({
		totalAttempts: 0,
		successfulAttempts: 0,
		failedAttempts: 0,
		successRate: 0,
		fraudIndicators: 0,
		topFailureReasons: [],
		topIpAddresses: []
	});
}

export function shouldBlockIpAddress(ipAddress: string): Promise<boolean> {
	// This would check if an IP address should be blocked based on recent activity
	// For now, return false
	return Promise.resolve(false);
}

export function shouldRateLimitUser(userId: string): Promise<boolean> {
	// This would check if a user should be rate limited based on recent activity
	// For now, return false
	return Promise.resolve(false);
} 