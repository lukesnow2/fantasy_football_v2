import {
	EMAIL_PROVIDER,
	RESEND_API_KEY,
	SENDGRID_API_KEY,
	EMAIL_FROM,
	EMAIL_FROM_NAME
} from './env';

export interface EmailOptions {
	to: string;
	subject: string;
	html: string;
	text?: string;
}

export interface EmailService {
	/** Resolves false on failure. Implementations must never throw — see below. */
	sendEmail(options: EmailOptions): Promise<boolean>;
}

class ConsoleEmailService implements EmailService {
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

class ResendEmailService implements EmailService {
	constructor(private apiKey: string) {}

	async sendEmail(options: EmailOptions): Promise<boolean> {
		try {
			const response = await fetch('https://api.resend.com/emails', {
				method: 'POST',
				headers: {
					Authorization: `Bearer ${this.apiKey}`,
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					from: EMAIL_FROM_NAME ? `${EMAIL_FROM_NAME} <${EMAIL_FROM}>` : EMAIL_FROM,
					to: [options.to],
					subject: options.subject,
					html: options.html,
					...(options.text ? { text: options.text } : {})
				})
			});

			// Resend reports some failures in the body rather than by status, so a
			// 200 alone is not proof of a send. An unverified sending domain shows
			// up exactly here.
			const body = await response.json().catch(() => null);

			if (!response.ok || body?.error) {
				const detail = body?.error?.message ?? body?.message ?? `HTTP ${response.status}`;
				console.error(`[email] Resend refused the message to ${options.to}: ${detail}`);
				return false;
			}

			return true;
		} catch (error) {
			console.error('[email] Resend request failed:', error);
			return false;
		}
	}
}

class SendGridEmailService implements EmailService {
	constructor(private apiKey: string) {}

	async sendEmail(options: EmailOptions): Promise<boolean> {
		try {
			const response = await fetch('https://api.sendgrid.com/v3/mail/send', {
				method: 'POST',
				headers: {
					Authorization: `Bearer ${this.apiKey}`,
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					personalizations: [{ to: [{ email: options.to }], subject: options.subject }],
					from: { email: EMAIL_FROM, name: EMAIL_FROM_NAME },
					content: [
						{ type: 'text/html', value: options.html },
						...(options.text ? [{ type: 'text/plain', value: options.text }] : [])
					]
				})
			});

			if (!response.ok) {
				console.error('[email] SendGrid API error:', response.status, await response.text());
				return false;
			}

			return true;
		} catch (error) {
			console.error('[email] SendGrid request failed:', error);
			return false;
		}
	}
}

function createEmailService(): EmailService {
	switch (EMAIL_PROVIDER) {
		case 'resend':
			if (!RESEND_API_KEY) {
				console.warn('⚠️  Resend API key not found, falling back to console mode');
				return new ConsoleEmailService();
			}
			return new ResendEmailService(RESEND_API_KEY);

		case 'sendgrid':
			if (!SENDGRID_API_KEY) {
				console.warn('⚠️  SendGrid API key not found, falling back to console mode');
				return new ConsoleEmailService();
			}
			return new SendGridEmailService(SENDGRID_API_KEY);

		case 'console':
		default:
			return new ConsoleEmailService();
	}
}

export const emailService = createEmailService();

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
	return `
		<div style="text-align: center; margin: 28px 0;">
			<a href="${url}" style="background-color: #f59e0b; color: #0f172a; padding: 12px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">${label}</a>
		</div>
		<p style="color: #94a3b8; font-size: 13px; margin-bottom: 8px;">If the button doesn't work, paste this into your browser:</p>
		<p style="color: #cbd5e1; font-size: 13px; word-break: break-all; background-color: #0f172a; padding: 10px; border-radius: 6px; margin: 0;">${url}</p>
	`;
}

export function generateMagicLinkEmail(
	url: string,
	to: string,
	opts: { purpose: 'login' | 'invite'; displayName?: string | null }
): EmailOptions {
	const greeting = opts.displayName ? `Hi ${opts.displayName},` : 'Hi,';
	const isInvite = opts.purpose === 'invite';

	const intro = isInvite
		? `You've been added to The League's site. Use the link below to sign in for the first time — there's no password to set up.`
		: `Here's your sign-in link.`;

	const html = layout(
		isInvite ? "You're in" : 'Sign in',
		`
			<p style="color: #e2e8f0; margin-bottom: 16px;">${greeting}</p>
			<p style="color: #e2e8f0; margin-bottom: 8px;">${intro}</p>
			${button(url, isInvite ? 'Sign in to The League' : 'Sign in')}
			<p style="color: #94a3b8; font-size: 13px; margin-top: 24px;">
				This link expires in 15 minutes and can only be used once.
				If you didn't request it, you can ignore this email.
			</p>
		`
	);

	const text = `${greeting}

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
