import { env } from '$env/dynamic/private';
import { error } from '@sveltejs/kit';

// Validate required environment variables
function validateEnv() {
	const required = [
		'DATABASE_URL',
		'AUTH_SECRET',
		'SESSION_SECRET'
	];

	const missing = required.filter(key => !env[key]);
	
	if (missing.length > 0) {
		throw error(500, `Missing required environment variables: ${missing.join(', ')}`);
	}

	// Warn about missing optional but recommended variables
	const recommended = ['SENDGRID_API_KEY', 'EMAIL_FROM'];
	const missingRecommended = recommended.filter(key => !env[key]);
	
	if (missingRecommended.length > 0) {
		console.warn(`⚠️  Recommended environment variables missing: ${missingRecommended.join(', ')}`);
		console.warn('   Email functionality will use console mode instead of actual email delivery.');
	}
}

// Call validation on module load
validateEnv();

// Database Configuration
export const DATABASE_URL = env.DATABASE_URL!;

// Authentication Secrets
export const AUTH_SECRET = env.AUTH_SECRET!;
export const SESSION_SECRET = env.SESSION_SECRET!;

// Email Configuration
export const EMAIL_PROVIDER = env.EMAIL_PROVIDER || 'console'; // 'sendgrid', 'ses', 'console'
export const SENDGRID_API_KEY = env.SENDGRID_API_KEY;
export const EMAIL_FROM = env.EMAIL_FROM || 'noreply@fantasyleague.com';
export const EMAIL_FROM_NAME = env.EMAIL_FROM_NAME || 'Fantasy League';

// Application Configuration
export const NODE_ENV = env.NODE_ENV || 'development';
// In production ORIGIN must be set explicitly — a wrong/localhost origin silently
// breaks WebAuthn (passkey) origin validation and CSRF checks.
if (NODE_ENV === 'production' && !env.ORIGIN) {
	throw error(500, 'ORIGIN environment variable must be set in production');
}
export const ORIGIN = env.ORIGIN || 'http://localhost:5173';
export const PORT = parseInt(env.PORT || '3000');

// Security Configuration
export const BCRYPT_ROUNDS = parseInt(env.BCRYPT_ROUNDS || '12');
export const SESSION_LIFETIME_HOURS = parseInt(env.SESSION_LIFETIME_HOURS || '24');

// Rate Limiting
export const RATE_LIMIT_REQUESTS_PER_MINUTE = parseInt(env.RATE_LIMIT_REQUESTS_PER_MINUTE || '60');
export const RATE_LIMIT_WINDOW_MS = parseInt(env.RATE_LIMIT_WINDOW_MS || '60000');

// Optional Features
export const SENTRY_DSN = env.SENTRY_DSN;
export const ENABLE_ANALYTICS = env.ENABLE_ANALYTICS === 'true';

// Development helpers
export const isDevelopment = NODE_ENV === 'development';
export const isProduction = NODE_ENV === 'production';

console.log(`🌍 Environment: ${NODE_ENV}`);
console.log(`🔗 Origin: ${ORIGIN}`);
console.log(`📧 Email Provider: ${EMAIL_PROVIDER}`); 