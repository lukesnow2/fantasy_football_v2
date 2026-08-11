import {
	EMAIL_PROVIDER,
	RESEND_API_KEY,
	SENDGRID_API_KEY,
	EMAIL_FROM_NAME,
	requireEmailFrom
} from './env';
import { escapeHtml } from './html';

export interface EmailOptions {
	to: string;
	subject: string;
	html: string;
	text?: string;
}

/** Split rather than pre-formatted: SendGrid wants {email, name}, Resend wants one header string. */
export interface FromAddress {
	address: string;
	name: string | null;
}

/**
 * Resolved per send and ALLOWED TO THROW — transports must call it inside their
 * own try/catch. See the EmailService contract below.
 */
export type ResolveFrom = () => FromAddress;

/** Positional: `sent[i]` describes `messages[i]`, so a caller can name who missed. */
export interface BatchResult {
	sent: boolean[];
	succeeded: number;
	failed: number;
}

export interface EmailService {
	/** Resolves false on failure. Implementations must never throw — see below. */
	sendEmail(options: EmailOptions): Promise<boolean>;
	/** Resolves a per-message verdict. Must never throw and never reject. */
	sendBatch(messages: EmailOptions[]): Promise<BatchResult>;
}

/** Everything a transport talks to that a test needs to replace. */
export interface TransportOptions {
	fetchImpl?: typeof fetch;
	sleep?: (ms: number) => Promise<void>;
	newIdempotencyKey?: () => string;
	timeoutMs?: number;
	log?: (message: string) => void;
}

const RESEND_ENDPOINT = 'https://api.resend.com/emails';
const RESEND_BATCH_ENDPOINT = 'https://api.resend.com/emails/batch';

/** Resend accepts at most 100 messages per batch request. */
export const MAX_BATCH = 100;
/** Longer than this and we skip the retry rather than stall a form action. */
export const MAX_RETRY_WAIT_MS = 1000;
export const DEFAULT_RETRY_WAIT_MS = 250;
export const DEFAULT_TIMEOUT_MS = 8000;

// ---------------------------------------------------------------------------
// Shared transport helpers
// ---------------------------------------------------------------------------

export function formatFromHeader(from: FromAddress): string {
	if (!from.name) return from.address;
	// A display name containing a comma parses as two addresses unless it is
	// quoted, and the provider rejects the whole send rather than the name.
	const name = /["(),:;<>@[\\\]]/.test(from.name)
		? `"${from.name.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
		: from.name;
	return `${name} <${from.address}>`;
}

function isRetryable(status: number): boolean {
	return status === 429 || status >= 500;
}

/** Milliseconds to wait before the single retry, or null meaning "do not retry". */
function retryDelayMs(response: Response): number | null {
	const header = response.headers.get('retry-after');
	if (header === null) return DEFAULT_RETRY_WAIT_MS;

	const seconds = Number(header);
	// Retry-After is also allowed to be an HTTP-date, which Number() turns into
	// NaN. setTimeout(NaN) fires immediately, so treating it as a delay would
	// turn a paced retry into an instant re-flood of a provider that just asked
	// us to slow down. Fall back to our own schedule instead.
	if (!Number.isFinite(seconds) || seconds < 0) return DEFAULT_RETRY_WAIT_MS;

	const ms = seconds * 1000;
	// Told to wait longer than our budget: skip the retry rather than sleep into
	// a certain second refusal. This runs inside a form action, and a manager
	// watching a spinner is a worse outcome than one logged failure.
	return ms > MAX_RETRY_WAIT_MS ? null : ms;
}

function describeFailure(status: number, body: unknown): string {
	// Resend reports errors as {statusCode, name, message} at the TOP level. The
	// previous code read only body.error.message, which never matches, so the
	// most common misconfiguration of all — an unverified sending domain —
	// surfaced as a bare "HTTP 403" with the sentence naming the fix discarded.
	// Both shapes are read because swapping one guess for another is not a fix.
	const record = (body ?? {}) as Record<string, unknown>;
	const nested = record.error;
	if (typeof nested === 'string' && nested) return nested;
	if (nested && typeof nested === 'object') {
		const message = (nested as Record<string, unknown>).message;
		if (typeof message === 'string' && message) return message;
	}
	if (typeof record.message === 'string' && record.message) return record.message;
	return `HTTP ${status}`;
}

function tally(sent: boolean[]): BatchResult {
	const succeeded = sent.filter(Boolean).length;
	return { sent, succeeded, failed: sent.length - succeeded };
}

/**
 * Default fan-out: one message at a time.
 *
 * Sequential rather than Promise.all deliberately. Unbounded concurrency is the
 * bug this batching work exists to remove, and a transport that inherits this
 * should inherit the pacing too. Resend overrides it with a real batch call.
 */
abstract class BaseEmailService implements EmailService {
	abstract sendEmail(options: EmailOptions): Promise<boolean>;

	async sendBatch(messages: EmailOptions[]): Promise<BatchResult> {
		const sent: boolean[] = [];
		for (const message of messages) {
			sent.push(await this.sendEmail(message));
		}
		return tally(sent);
	}
}

class ConsoleEmailService extends BaseEmailService {
	async sendEmail(options: EmailOptions): Promise<boolean> {
		console.log('\n📧 EMAIL (Console Mode)');
		console.log('=======================');
		console.log(`To: ${options.to}`);
		console.log(`Subject: ${options.subject}`);
		console.log(options.text ?? options.html);
		console.log('=======================\n');
		return true;
	}
}

class ResendEmailService extends BaseEmailService {
	private readonly fetchImpl: typeof fetch;
	private readonly sleep: (ms: number) => Promise<void>;
	private readonly newIdempotencyKey: () => string;
	private readonly timeoutMs: number;
	private readonly log: (message: string) => void;

	constructor(
		private apiKey: string,
		private resolveFrom: ResolveFrom,
		opts: TransportOptions = {}
	) {
		super();
		this.fetchImpl = opts.fetchImpl ?? ((...args) => globalThis.fetch(...args));
		this.sleep = opts.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
		this.newIdempotencyKey = opts.newIdempotencyKey ?? (() => crypto.randomUUID());
		this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		this.log = opts.log ?? ((message) => console.error(message));
	}

	/**
	 * One POST, with at most one retry.
	 *
	 * Both attempts carry the SAME Idempotency-Key. That is what makes retrying
	 * an ambiguous 5xx safe: if the first attempt actually delivered, Resend
	 * dedupes the second within its 24h window rather than mailing twice.
	 *
	 * A timeout is deliberately NOT retried — it rejects out of this loop. Two
	 * 8s attempts stacked inside a login form action is worse than one failure,
	 * and a timeout means the request may well have been received anyway.
	 */
	private async post(
		url: string,
		payload: unknown
	): Promise<{ ok: boolean; status: number; body: unknown }> {
		const idempotencyKey = this.newIdempotencyKey();

		for (let attempt = 0; ; attempt++) {
			const response = await this.fetchImpl(url, {
				method: 'POST',
				headers: {
					Authorization: `Bearer ${this.apiKey}`,
					'Content-Type': 'application/json',
					'Idempotency-Key': idempotencyKey
				},
				body: JSON.stringify(payload),
				signal: AbortSignal.timeout(this.timeoutMs)
			});

			const body = await response.json().catch(() => null);
			if (response.ok) return { ok: true, status: response.status, body };

			// 422 and friends are permanent — an unverified domain or a malformed
			// address does not become valid on a second try, and re-sending burns
			// quota and risks a duplicate.
			if (attempt === 0 && isRetryable(response.status)) {
				const wait = retryDelayMs(response);
				if (wait !== null) {
					await this.sleep(wait);
					continue;
				}
			}

			return { ok: false, status: response.status, body };
		}
	}

	async sendEmail(options: EmailOptions): Promise<boolean> {
		try {
			// Inside the try, deliberately. The EmailService contract promises never
			// to throw and the login form action depends on it: an exception here
			// would make a provisioned address error while an unknown address still
			// redirects quietly, reopening the enumeration oracle that
			// login/+page.server.ts exists to close.
			const from = formatFromHeader(this.resolveFrom());

			const { ok, status, body } = await this.post(RESEND_ENDPOINT, {
				from,
				to: [options.to],
				subject: options.subject,
				html: options.html,
				...(options.text ? { text: options.text } : {})
			});

			if (!ok) {
				this.log(`[email] Resend refused the message to ${options.to}: ${describeFailure(status, body)}`);
				return false;
			}

			return true;
		} catch (error) {
			this.log(`[email] Resend request for ${options.to} failed: ${error}`);
			return false;
		}
	}

	async sendBatch(messages: EmailOptions[]): Promise<BatchResult> {
		if (messages.length === 0) return tally([]);

		const sent: boolean[] = [];
		try {
			const from = formatFromHeader(this.resolveFrom());

			// Chunks go out one after another. Sending them concurrently would
			// recreate exactly the burst this method replaces.
			for (let i = 0; i < messages.length; i += MAX_BATCH) {
				const chunk = messages.slice(i, i + MAX_BATCH);
				sent.push(...(await this.postChunk(from, chunk)));
			}
		} catch (error) {
			this.log(`[email] Resend batch failed: ${error}`);
		}

		// Anything we never got a verdict for — a throw partway through, or a
		// resolveFrom() that failed before the first request — counts as failed.
		while (sent.length < messages.length) sent.push(false);
		return tally(sent);
	}

	private async postChunk(from: string, chunk: EmailOptions[]): Promise<boolean[]> {
		const { ok, status, body } = await this.post(
			RESEND_BATCH_ENDPOINT,
			chunk.map((message) => ({
				from,
				to: [message.to],
				subject: message.subject,
				html: message.html,
				...(message.text ? { text: message.text } : {})
			}))
		);

		if (!ok) {
			this.log(
				`[email] Resend refused a batch of ${chunk.length}: ${describeFailure(status, body)}`
			);
			return chunk.map(() => false);
		}

		const data = (body as { data?: unknown } | null)?.data;
		const entries = Array.isArray(data) ? data : [];

		// Positions are only trustworthy when Resend vouches for every message.
		// Same-index correspondence is documented for the all-success shape only;
		// the partially-failed shape is not documented at all. A short array means
		// an entry was omitted and we cannot tell which, so attributing ids by
		// position would name the wrong managers AND mark a failed address sent.
		// Fail the chunk and log the body verbatim — a spurious "failed" beats a
		// confident lie.
		if (entries.length !== chunk.length) {
			this.log(
				`[email] Resend batch response covered ${entries.length} of ${chunk.length} messages; ` +
					`not attributing ids by position. Body: ${JSON.stringify(body)}`
			);
			return chunk.map(() => false);
		}

		return chunk.map((_, i) => {
			const id = (entries[i] as { id?: unknown } | null)?.id;
			return typeof id === 'string' && id.length > 0;
		});
	}
}

class SendGridEmailService extends BaseEmailService {
	private readonly fetchImpl: typeof fetch;
	private readonly timeoutMs: number;
	private readonly log: (message: string) => void;

	constructor(
		private apiKey: string,
		private resolveFrom: ResolveFrom,
		opts: TransportOptions = {}
	) {
		super();
		this.fetchImpl = opts.fetchImpl ?? ((...args) => globalThis.fetch(...args));
		this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		this.log = opts.log ?? ((message) => console.error(message));
	}

	async sendEmail(options: EmailOptions): Promise<boolean> {
		try {
			const from = this.resolveFrom();

			const response = await this.fetchImpl('https://api.sendgrid.com/v3/mail/send', {
				method: 'POST',
				headers: {
					Authorization: `Bearer ${this.apiKey}`,
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					personalizations: [{ to: [{ email: options.to }], subject: options.subject }],
					from: { email: from.address, ...(from.name ? { name: from.name } : {}) },
					content: [
						{ type: 'text/html', value: options.html },
						...(options.text ? [{ type: 'text/plain', value: options.text }] : [])
					]
				}),
				signal: AbortSignal.timeout(this.timeoutMs)
			});

			if (!response.ok) {
				this.log(`[email] SendGrid API error: ${response.status} ${await response.text()}`);
				return false;
			}

			return true;
		} catch (error) {
			this.log(`[email] SendGrid request for ${options.to} failed: ${error}`);
			return false;
		}
	}
}

/**
 * A thunk, not a value.
 *
 * createEmailService() runs at module load, which happens during `vite build`
 * while SvelteKit analyses routes — so resolving EMAIL_FROM here would fail the
 * build on a machine that has no reason to hold mail config. Transports call
 * this inside their try/catch instead, where a missing value degrades to
 * `return false` plus a named log.
 */
function resolveFrom(): FromAddress {
	return { address: requireEmailFrom(), name: EMAIL_FROM_NAME || null };
}

function createEmailService(): EmailService {
	switch (EMAIL_PROVIDER) {
		case 'resend':
			if (!RESEND_API_KEY) {
				console.warn('⚠️  Resend API key not found, falling back to console mode');
				return new ConsoleEmailService();
			}
			return new ResendEmailService(RESEND_API_KEY, resolveFrom);

		case 'sendgrid':
			if (!SENDGRID_API_KEY) {
				console.warn('⚠️  SendGrid API key not found, falling back to console mode');
				return new ConsoleEmailService();
			}
			return new SendGridEmailService(SENDGRID_API_KEY, resolveFrom);

		case 'console':
		default:
			return new ConsoleEmailService();
	}
}

export const emailService = createEmailService();

/** Exported for tests, which construct transports directly with stubbed dependencies. */
export { ConsoleEmailService, ResendEmailService, SendGridEmailService };

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

function layout(heading: string, bodyHtml: string): string {
	return `
		<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; max-width: 560px; margin: 0 auto; padding: 24px; background-color: #0f172a;">
			<div style="background-color: #1e293b; padding: 32px; border-radius: 12px; border: 1px solid #334155;">
				<h1 style="color: #f8fafc; margin: 0 0 4px 0; font-size: 20px;">🏆 The League</h1>
				<p style="color: #94a3b8; margin: 0 0 24px 0; font-size: 14px;">${heading}</p>
				${bodyHtml}
			</div>
		</div>
	`;
}

function button(url: string, label: string): string {
	// Escaped even though today's URL cannot break out: the token is
	// encodeURIComponent'd and ORIGIN is operator-set, but that invariant lives
	// two modules away and is one refactor from being false.
	//
	// Turning `&` into `&amp;` inside an href is CORRECT — attribute values are
	// entity-decoded before the URL is resolved — so a future multi-parameter
	// link still works. Nobody should "fix" this back.
	const href = escapeHtml(url);

	return `
		<div style="text-align: center; margin: 28px 0;">
			<a href="${href}" style="background-color: #f59e0b; color: #0f172a; padding: 12px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">${label}</a>
		</div>
		<p style="color: #94a3b8; font-size: 13px; margin-bottom: 8px;">If the button doesn't work, paste this into your browser:</p>
		<p style="color: #cbd5e1; font-size: 13px; word-break: break-all; background-color: #0f172a; padding: 10px; border-radius: 6px; margin: 0;">${href}</p>
	`;
}

export function generateMagicLinkEmail(
	url: string,
	to: string,
	opts: { purpose: 'login' | 'invite'; displayName?: string | null }
): EmailOptions {
	// Two greetings, not one. The HTML body must be escaped or a display name is
	// an injection point; the text/plain body must NOT be, or a manager called
	// O'Brien reads "Hi O&#39;Brien," in their mail client. Sharing one value
	// here fixes the injection and introduces that bug in the same edit.
	const greetingHtml = opts.displayName ? `Hi ${escapeHtml(opts.displayName)},` : 'Hi,';
	const greetingText = opts.displayName ? `Hi ${opts.displayName},` : 'Hi,';
	const isInvite = opts.purpose === 'invite';

	const intro = isInvite
		? `You've been added to The League's site. Use the link below to sign in for the first time — there's no password to set up.`
		: `Here's your sign-in link.`;

	const html = layout(
		isInvite ? "You're in" : 'Sign in',
		`
			<p style="color: #e2e8f0; margin-bottom: 16px;">${greetingHtml}</p>
			<p style="color: #e2e8f0; margin-bottom: 8px;">${intro}</p>
			${button(url, isInvite ? 'Sign in to The League' : 'Sign in')}
			<p style="color: #94a3b8; font-size: 13px; margin-top: 24px;">
				This link expires in 15 minutes and can only be used once.
				If you didn't request it, you can ignore this email.
			</p>
		`
	);

	// Raw url, deliberately. This is the copy-and-paste fallback; an entity-encoded
	// link here would be pasted broken.
	const text = `${greetingText}

${intro}

${url}

This link expires in 15 minutes and can only be used once.
If you didn't request it, you can ignore this email.
`;

	return {
		to,
		subject: isInvite ? "You're invited to The League" : 'Your sign-in link for The League',
		html,
		text
	};
}
