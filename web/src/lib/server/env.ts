import { env } from '$env/dynamic/private';

// Application Configuration
export const NODE_ENV = env.NODE_ENV || 'development';
export const isDevelopment = NODE_ENV === 'development';
export const isProduction = NODE_ENV === 'production';

// Email Configuration
export const EMAIL_PROVIDER = env.EMAIL_PROVIDER || 'console'; // 'resend' | 'sendgrid' | 'console'
export const RESEND_API_KEY = env.RESEND_API_KEY;
export const SENDGRID_API_KEY = env.SENDGRID_API_KEY;
export const EMAIL_FROM = env.EMAIL_FROM;
export const EMAIL_FROM_NAME = env.EMAIL_FROM_NAME || 'The League';

/**
 * Fail fast on missing configuration.
 *
 * Throws a plain Error rather than SvelteKit's `error()` helper: this runs at
 * module load, outside any request, where `error()` produces a confusing
 * "500" object instead of a stack trace pointing at the real problem.
 */
function validateEnv() {
	const missing = ['DATABASE_URL'].filter((key) => !env[key]);
	if (missing.length > 0) {
		throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
	}

	// In production ORIGIN must be set explicitly. Magic-link URLs are built from
	// it, so a wrong or defaulted value emails everyone a link to localhost.
	if (isProduction && !env.ORIGIN) {
		throw new Error('ORIGIN environment variable must be set in production');
	}

	// A mail provider selected but not configured is worse than no provider: the
	// service falls back to console mode, sends land in a serverless log, and
	// nobody can sign in while everything reports success.
	if (EMAIL_PROVIDER === 'resend' && !RESEND_API_KEY) {
		console.warn('⚠️  EMAIL_PROVIDER=resend but RESEND_API_KEY is unset — falling back to console mode.');
	}
	if (EMAIL_PROVIDER === 'sendgrid' && !SENDGRID_API_KEY) {
		console.warn('⚠️  EMAIL_PROVIDER=sendgrid but SENDGRID_API_KEY is unset — falling back to console mode.');
	}
	if (EMAIL_PROVIDER !== 'console' && !EMAIL_FROM) {
		console.warn('⚠️  EMAIL_FROM is unset — sign-in emails will be rejected by the provider.');
	}
	if (isProduction && EMAIL_PROVIDER === 'console') {
		console.warn('⚠️  EMAIL_PROVIDER=console in production — nobody can sign in; links go to the log.');
	}
}

validateEnv();

// Database Configuration
export const DATABASE_URL = env.DATABASE_URL!;

export const ORIGIN = env.ORIGIN || 'http://localhost:5173';
export const PORT = parseInt(env.PORT || '3000');

// Optional Features
export const SENTRY_DSN = env.SENTRY_DSN;
export const ENABLE_ANALYTICS = env.ENABLE_ANALYTICS === 'true';

console.log(`🌍 Environment: ${NODE_ENV}`);
console.log(`🔗 Origin: ${ORIGIN}`);
console.log(`📧 Email Provider: ${EMAIL_PROVIDER}`);
