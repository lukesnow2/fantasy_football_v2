import type { RequestEvent } from '@sveltejs/kit';
import { json } from '@sveltejs/kit';
import { validateCSRFToken, getCSRFTokenFromRequest, validateCSRFHeaders } from './csrf';
import { validateWebAuthnSession } from './session';

export interface WebAuthnMiddlewareOptions {
	requireCSRF?: boolean;
	requireSession?: boolean;
	requireOrigin?: boolean;
}

export async function validateWebAuthnRequest(
	event: RequestEvent,
	options: WebAuthnMiddlewareOptions = {}
): Promise<{ valid: boolean; error?: string; status?: number }> {
	const { requireCSRF = true, requireSession = false, requireOrigin = true } = options;

	// Validate origin/referer headers
	if (requireOrigin && !validateCSRFHeaders(event.request)) {
		return {
			valid: false,
			error: 'Invalid origin or referer header',
			status: 403
		};
	}

	// Validate CSRF token if required
	if (requireCSRF) {
		const csrfToken = getCSRFTokenFromRequest(event.request);
		if (!csrfToken) {
			return {
				valid: false,
				error: 'CSRF token missing',
				status: 403
			};
		}

		const isValidCSRF = await validateCSRFToken(csrfToken);
		if (!isValidCSRF) {
			return {
				valid: false,
				error: 'Invalid or expired CSRF token',
				status: 403
			};
		}
	}

	// Validate session if required
	if (requireSession) {
		const sessionToken = event.cookies.get('webauthn-session');
		if (!sessionToken) {
			return {
				valid: false,
				error: 'Session token missing',
				status: 401
			};
		}

		const { session, user } = await validateWebAuthnSession(sessionToken);
		if (!session || !user) {
			return {
				valid: false,
				error: 'Invalid or expired session',
				status: 401
			};
		}
	}

	return { valid: true };
}

export function createWebAuthnMiddleware(options: WebAuthnMiddlewareOptions = {}) {
	return async (event: RequestEvent) => {
		const validation = await validateWebAuthnRequest(event, options);
		
		if (!validation.valid) {
			return json(
				{ 
					success: false, 
					error: validation.error 
				}, 
				{ status: validation.status || 400 }
			);
		}
	};
}

export function withWebAuthnValidation(options: WebAuthnMiddlewareOptions = {}) {
	return function(target: any, propertyKey: string, descriptor: PropertyDescriptor) {
		const originalMethod = descriptor.value;
		
		descriptor.value = async function(event: RequestEvent, ...args: any[]) {
			const validation = await validateWebAuthnRequest(event, options);
			
			if (!validation.valid) {
				return json(
					{ 
						success: false, 
						error: validation.error 
					}, 
					{ status: validation.status || 400 }
				);
			}
			
			return originalMethod.apply(this, [event, ...args]);
		};
		
		return descriptor;
	};
} 