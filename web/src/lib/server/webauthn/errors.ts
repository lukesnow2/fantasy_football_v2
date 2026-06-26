import { dev } from '$app/environment';

export enum WebAuthnErrorCode {
	// Registration errors
	REGISTRATION_FAILED = 'REGISTRATION_FAILED',
	INVALID_REGISTRATION_RESPONSE = 'INVALID_REGISTRATION_RESPONSE',
	CREDENTIAL_ALREADY_EXISTS = 'CREDENTIAL_ALREADY_EXISTS',
	
	// Authentication errors
	AUTHENTICATION_FAILED = 'AUTHENTICATION_FAILED',
	INVALID_AUTHENTICATION_RESPONSE = 'INVALID_AUTHENTICATION_RESPONSE',
	CREDENTIAL_NOT_FOUND = 'CREDENTIAL_NOT_FOUND',
	
	// Challenge errors
	CHALLENGE_EXPIRED = 'CHALLENGE_EXPIRED',
	INVALID_CHALLENGE = 'INVALID_CHALLENGE',
	CHALLENGE_ALREADY_USED = 'CHALLENGE_ALREADY_USED',
	
	// Security errors
	CSRF_TOKEN_INVALID = 'CSRF_TOKEN_INVALID',
	ORIGIN_NOT_ALLOWED = 'ORIGIN_NOT_ALLOWED',
	RATE_LIMIT_EXCEEDED = 'RATE_LIMIT_EXCEEDED',
	
	// Session errors
	SESSION_INVALID = 'SESSION_INVALID',
	SESSION_EXPIRED = 'SESSION_EXPIRED',
	
	// Validation errors
	INVALID_USER_ID = 'INVALID_USER_ID',
	INVALID_CREDENTIAL_ID = 'INVALID_CREDENTIAL_ID',
	INVALID_PUBLIC_KEY = 'INVALID_PUBLIC_KEY',
	
	// Server errors
	INTERNAL_ERROR = 'INTERNAL_ERROR',
	DATABASE_ERROR = 'DATABASE_ERROR',
	CONFIGURATION_ERROR = 'CONFIGURATION_ERROR'
}

export interface WebAuthnError {
	code: WebAuthnErrorCode;
	message: string;
	details?: string;
	timestamp: Date;
	requestId?: string;
}

export class SecureWebAuthnError extends Error {
	public readonly code: WebAuthnErrorCode;
	public readonly timestamp: Date;
	public readonly requestId?: string;
	private readonly internalDetails?: string;

	constructor(
		code: WebAuthnErrorCode,
		userMessage: string,
		internalDetails?: string,
		requestId?: string
	) {
		super(userMessage);
		this.name = 'SecureWebAuthnError';
		this.code = code;
		this.timestamp = new Date();
		this.requestId = requestId;
		this.internalDetails = internalDetails;
	}

	public toJSON(): WebAuthnError {
		return {
			code: this.code,
			message: this.message,
			details: dev ? this.internalDetails : undefined,
			timestamp: this.timestamp,
			requestId: this.requestId
		};
	}

	public getInternalDetails(): string | undefined {
		return dev ? this.internalDetails : undefined;
	}
}

export function createWebAuthnError(
	code: WebAuthnErrorCode,
	userMessage: string,
	internalDetails?: string,
	requestId?: string
): SecureWebAuthnError {
	return new SecureWebAuthnError(code, userMessage, internalDetails, requestId);
}

export function sanitizeError(error: unknown): { message: string; code?: string } {
	if (error instanceof SecureWebAuthnError) {
		return {
			message: error.message,
			code: error.code
		};
	}

	if (error instanceof Error) {
		// In development, show more details
		if (dev) {
			return {
				message: error.message,
				code: 'UNKNOWN_ERROR'
			};
		}
		
		// In production, show generic message
		return {
			message: 'An unexpected error occurred',
			code: 'UNKNOWN_ERROR'
		};
	}

	return {
		message: 'An unexpected error occurred',
		code: 'UNKNOWN_ERROR'
	};
}

export function logWebAuthnError(error: SecureWebAuthnError, context?: Record<string, any>): void {
	const logData = {
		error: error.toJSON(),
		context,
		stack: dev ? error.stack : undefined
	};

	// In development, log to console
	if (dev) {
		console.error('WebAuthn Error:', logData);
		return;
	}

	// In production, log to structured logging system
	// This would typically go to a logging service like DataDog, LogRocket, etc.
	console.error('WebAuthn Error:', {
		code: error.code,
		message: error.message,
		timestamp: error.timestamp,
		requestId: error.requestId,
		context: context ? Object.keys(context) : undefined
	});
}

export function handleWebAuthnError(error: unknown, requestId?: string): SecureWebAuthnError {
	if (error instanceof SecureWebAuthnError) {
		logWebAuthnError(error);
		return error;
	}

	// Convert unknown errors to secure errors
	const secureError = createWebAuthnError(
		WebAuthnErrorCode.INTERNAL_ERROR,
		'An unexpected error occurred',
		error instanceof Error ? error.message : String(error),
		requestId
	);

	logWebAuthnError(secureError);
	return secureError;
}

// Predefined error messages for common scenarios
export const WebAuthnErrorMessages = {
	REGISTRATION_FAILED: 'Passkey registration was cancelled or failed. Please try again.',
	AUTHENTICATION_FAILED: 'Passkey authentication was cancelled or failed. Please try again.',
	CHALLENGE_EXPIRED: 'The authentication request has expired. Please try again.',
	CSRF_TOKEN_INVALID: 'Security validation failed. Please refresh the page and try again.',
	RATE_LIMIT_EXCEEDED: 'Too many attempts. Please wait a moment and try again.',
	SESSION_EXPIRED: 'Your session has expired. Please sign in again.',
	CREDENTIAL_NOT_FOUND: 'No passkey found for this account. Please register a passkey.',
	INTERNAL_ERROR: 'An unexpected error occurred. Please try again later.'
} as const; 