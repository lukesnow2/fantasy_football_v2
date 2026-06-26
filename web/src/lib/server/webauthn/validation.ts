import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

export interface ValidationConfig {
	allowedOrigins: string[];
	allowedRpIds: string[];
	allowedSubdomains: string[];
	requireHttps: boolean;
	allowLocalhost: boolean;
}

export interface ValidationResult {
	valid: boolean;
	error?: string;
	details?: Record<string, any>;
}

// Default validation configuration
export const defaultValidationConfig: ValidationConfig = {
	allowedOrigins: [
		'https://your-domain.com',
		'https://www.your-domain.com',
		'https://app.your-domain.com'
	],
	allowedRpIds: [
		'your-domain.com',
		'www.your-domain.com',
		'app.your-domain.com'
	],
	allowedSubdomains: [
		'app',
		'api',
		'admin'
	],
	requireHttps: true,
	allowLocalhost: process.env.NODE_ENV === 'development'
};

export function validateOrigin(origin: string, config: ValidationConfig = defaultValidationConfig): ValidationResult {
	// Check if origin is provided
	if (!origin) {
		return {
			valid: false,
			error: 'Origin header is required',
			details: { missing: 'origin' }
		};
	}

	// Parse the origin URL
	let originUrl: URL;
	try {
		originUrl = new URL(origin);
	} catch (error) {
		return {
			valid: false,
			error: 'Invalid origin format',
			details: { origin, error: error instanceof Error ? error.message : 'URL parsing failed' }
		};
	}

	// Check protocol requirements
	if (config.requireHttps && originUrl.protocol !== 'https:') {
		// Allow localhost in development
		if (!(config.allowLocalhost && originUrl.hostname === 'localhost')) {
			return {
				valid: false,
				error: 'HTTPS is required',
				details: { protocol: originUrl.protocol, hostname: originUrl.hostname }
			};
		}
	}

	// Check against allowed origins
	if (config.allowedOrigins.includes(origin)) {
		return { valid: true };
	}

	// Check subdomain validation
	const hostname = originUrl.hostname;
	const domainParts = hostname.split('.');
	
	if (domainParts.length >= 2) {
		const subdomain = domainParts[0];
		const domain = domainParts.slice(1).join('.');
		
		// Check if it's a valid subdomain
		if (config.allowedSubdomains.includes(subdomain)) {
			const baseDomain = `${subdomain}.${domain}`;
			if (config.allowedRpIds.includes(domain)) {
				return { valid: true };
			}
		}
		
		// Check if it's the base domain
		if (config.allowedRpIds.includes(hostname)) {
			return { valid: true };
		}
	}

	// Allow localhost in development
	if (config.allowLocalhost && hostname === 'localhost') {
		return { valid: true };
	}

	return {
		valid: false,
		error: 'Origin not allowed',
		details: { 
			origin, 
			hostname: originUrl.hostname,
			allowedOrigins: config.allowedOrigins,
			allowedRpIds: config.allowedRpIds
		}
	};
}

export function validateRelyingPartyId(rpId: string, config: ValidationConfig = defaultValidationConfig): ValidationResult {
	// Check if rpId is provided
	if (!rpId) {
		return {
			valid: false,
			error: 'Relying party ID is required',
			details: { missing: 'rpId' }
		};
	}

	// Check against allowed RP IDs
	if (config.allowedRpIds.includes(rpId)) {
		return { valid: true };
	}

	// Check subdomain validation
	const domainParts = rpId.split('.');
	
	if (domainParts.length >= 2) {
		const subdomain = domainParts[0];
		const domain = domainParts.slice(1).join('.');
		
		// Check if it's a valid subdomain
		if (config.allowedSubdomains.includes(subdomain)) {
			if (config.allowedRpIds.includes(domain)) {
				return { valid: true };
			}
		}
	}

	// Allow localhost in development
	if (config.allowLocalhost && rpId === 'localhost') {
		return { valid: true };
	}

	return {
		valid: false,
		error: 'Relying party ID not allowed',
		details: { 
			rpId, 
			allowedRpIds: config.allowedRpIds 
		}
	};
}

export function validateClientData(clientData: any, expectedChallenge: string, expectedOrigin: string): ValidationResult {
	if (!clientData) {
		return {
			valid: false,
			error: 'Client data is required',
			details: { missing: 'clientData' }
		};
	}

	// Validate challenge
	if (clientData.challenge !== expectedChallenge) {
		return {
			valid: false,
			error: 'Challenge mismatch',
			details: { 
				expected: expectedChallenge.substring(0, 8) + '...',
				received: clientData.challenge?.substring(0, 8) + '...'
			}
		};
	}

	// Validate origin
	if (clientData.origin !== expectedOrigin) {
		return {
			valid: false,
			error: 'Origin mismatch',
			details: { 
				expected: expectedOrigin,
				received: clientData.origin 
			}
		};
	}

	// Validate type
	if (!clientData.type || !['webauthn.create', 'webauthn.get'].includes(clientData.type)) {
		return {
			valid: false,
			error: 'Invalid client data type',
			details: { 
				type: clientData.type,
				expected: ['webauthn.create', 'webauthn.get']
			}
		};
	}

	return { valid: true };
}

export async function validateWebAuthnRequest(
	request: Request,
	expectedChallenge: string,
	expectedOrigin: string,
	context?: { userId?: string; ipAddress?: string }
): Promise<ValidationResult> {
	const origin = request.headers.get('origin');
	const referer = request.headers.get('referer');

	// Validate origin
	if (origin) {
		const originValidation = validateOrigin(origin);
		if (!originValidation.valid) {
			await logAuditEvent(
				AuditEventType.SECURITY_VIOLATION,
				AuditSeverity.CRITICAL,
				{
					reason: 'invalid_origin',
					origin,
					details: originValidation.details
				},
				context
			);
			return originValidation;
		}
	}

	// Validate referer if present
	if (referer) {
		try {
			const refererUrl = new URL(referer);
			const refererValidation = validateOrigin(refererUrl.origin);
			if (!refererValidation.valid) {
				await logAuditEvent(
					AuditEventType.SECURITY_VIOLATION,
					AuditSeverity.CRITICAL,
					{
						reason: 'invalid_referer',
						referer: refererUrl.origin,
						details: refererValidation.details
					},
					context
				);
				return refererValidation;
			}
		} catch (error) {
			await logAuditEvent(
				AuditEventType.SECURITY_VIOLATION,
				AuditSeverity.WARNING,
				{
					reason: 'invalid_referer_format',
					referer,
					error: error instanceof Error ? error.message : 'URL parsing failed'
				},
				context
			);
			return {
				valid: false,
				error: 'Invalid referer format',
				details: { referer }
			};
		}
	}

	// For same-origin requests, additional validation may not be needed
	if (!origin && !referer) {
		return { valid: true };
	}

	return { valid: true };
}

export function extractRelyingPartyId(origin: string): string {
	try {
		const url = new URL(origin);
		return url.hostname;
	} catch {
		return origin;
	}
}

export function isSubdomain(hostname: string, domain: string): boolean {
	return hostname.endsWith(`.${domain}`) || hostname === domain;
}

export function getValidationConfigForEnvironment(): ValidationConfig {
	const isDevelopment = process.env.NODE_ENV === 'development';
	const domain = process.env.DOMAIN || 'your-domain.com';
	
	return {
		allowedOrigins: [
			`https://${domain}`,
			`https://www.${domain}`,
			`https://app.${domain}`,
			...(isDevelopment ? ['http://localhost:5173', 'http://localhost:3000'] : [])
		],
		allowedRpIds: [
			domain,
			`www.${domain}`,
			`app.${domain}`,
			...(isDevelopment ? ['localhost'] : [])
		],
		allowedSubdomains: ['app', 'api', 'admin'],
		requireHttps: !isDevelopment,
		allowLocalhost: isDevelopment
	};
} 