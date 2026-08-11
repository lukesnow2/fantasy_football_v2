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
 * Report missing configuration — warn here, fail where it matters.
 *
 * Deliberately does not throw. This module is evaluated during `vite build`
 * while SvelteKit analyses routes, so throwing here fails the *build* on a
 * machine that has no reason to hold production secrets. Compiling the app
 * should not require a database URL or a mail key.
 *
 * The hard failures live at the point of use instead: db/index.ts throws on the
 * first query, and requireOrigin() throws before a sign-in link is built.
 */
function validateEnv() {
	const missing = ['DATABASE_URL'].filter((key) => !env[key]);
	if (missing.length > 0) {
		console.warn(`⚠️  Missing environment variables: ${missing.join(', ')}`);
	}

	// Magic-link URLs are built from ORIGIN, so a wrong or defaulted value emails
	// everyone a link to localhost. Enforced by requireOrigin() at send time.
	if (isProduction && !env.ORIGIN) {
		console.warn('⚠️  ORIGIN is unset in production — sign-in links cannot be built.');
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

/**
 * The origin to build a sign-in link from, or an exception.
 *
 * Called on the send path rather than at module load. A production deployment
 * that fell back to the localhost default would mail every manager a link they
 * cannot open, and because sends are logged rather than thrown, nothing else
 * would notice. Better to refuse to send.
 */
export function requireOrigin(): string {
	if (isProduction && !env.ORIGIN) {
		throw new Error('ORIGIN must be set in production before sign-in links can be sent.');
	}
	return ORIGIN;
}

/**
 * The address to send from, or an exception.
 *
 * Gated on the provider rather than on isProduction, unlike requireOrigin().
 * ORIGIN has a *correct* default in development, so it only becomes wrong in
 * production. EMAIL_FROM has no default at all — EMAIL_FROM_NAME defaults to
 * 'The League' but the address does not — so an unset value built the header
 * "The League <undefined>" and every provider rejected it. There is no
 * environment in which that is right, and the person who most needs the loud
 * failure is a developer pointing EMAIL_PROVIDER=resend at a real key locally.
 *
 * The console provider never reads `from`, so `npm run dev` and `vite build`
 * are unaffected.
 *
 * NOTE: transports call this from inside their own try/catch, because the
 * EmailService contract forbids throwing — so in practice this degrades to a
 * `return false` with a named message rather than surfacing to a user. That is
 * why describeMailConfig() exists: the log is not a surface anyone watches.
 */
export function requireEmailFrom(): string {
	if (EMAIL_PROVIDER !== 'console' && !env.EMAIL_FROM) {
		throw new Error(
			`EMAIL_FROM must be set when EMAIL_PROVIDER=${EMAIL_PROVIDER}. ` +
				`Without it every message is sent from "${EMAIL_FROM_NAME} <undefined>" and is rejected.`
		);
	}
	return env.EMAIL_FROM ?? '';
}

/**
 * Mail configuration problems, described rather than thrown.
 *
 * Every failure mode here is currently invisible: a missing key falls back to
 * console mode, a missing EMAIL_FROM is refused by the provider, and both are
 * reported only to a serverless log. Rendered on /admin/members because the
 * commissioner is the one person who both looks at that page and can set the
 * variable.
 */
export function describeMailConfig(): { ok: boolean; problems: string[] } {
	const problems: string[] = [];

	if (EMAIL_PROVIDER === 'console') {
		if (isProduction) {
			problems.push(
				'EMAIL_PROVIDER is "console", so sign-in links are written to the server log instead of being emailed. Nobody can sign in.'
			);
		}
	} else {
		if (!env.EMAIL_FROM) {
			problems.push(
				`EMAIL_FROM is not set. Messages would be sent from "${EMAIL_FROM_NAME} <undefined>" and the provider rejects them.`
			);
		}
		if (EMAIL_PROVIDER === 'resend' && !RESEND_API_KEY) {
			problems.push(
				'EMAIL_PROVIDER is "resend" but RESEND_API_KEY is not set, so mail silently falls back to the server log.'
			);
		}
		if (EMAIL_PROVIDER === 'sendgrid' && !SENDGRID_API_KEY) {
			problems.push(
				'EMAIL_PROVIDER is "sendgrid" but SENDGRID_API_KEY is not set, so mail silently falls back to the server log.'
			);
		}
	}

	if (isProduction && !env.ORIGIN) {
		problems.push('ORIGIN is not set, so sign-in links cannot be built.');
	}

	return { ok: problems.length === 0, problems };
}

// Optional Features
export const SENTRY_DSN = env.SENTRY_DSN;
export const ENABLE_ANALYTICS = env.ENABLE_ANALYTICS === 'true';

console.log(`🌍 Environment: ${NODE_ENV}`);
console.log(`🔗 Origin: ${ORIGIN}`);
console.log(`📧 Email Provider: ${EMAIL_PROVIDER}`);
