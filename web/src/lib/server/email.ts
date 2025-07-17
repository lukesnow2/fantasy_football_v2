import { 
	EMAIL_PROVIDER, 
	SENDGRID_API_KEY, 
	EMAIL_FROM, 
	EMAIL_FROM_NAME,
	ORIGIN,
	isDevelopment 
} from './env';

export interface EmailOptions {
	to: string;
	subject: string;
	html: string;
	text?: string;
}

export interface EmailService {
	sendEmail(options: EmailOptions): Promise<boolean>;
}

class ConsoleEmailService implements EmailService {
	async sendEmail(options: EmailOptions): Promise<boolean> {
		console.log('\n📧 EMAIL (Console Mode)');
		console.log('=======================');
		console.log(`To: ${options.to}`);
		console.log(`Subject: ${options.subject}`);
		console.log('HTML Body:');
		console.log(options.html);
		console.log('=======================\n');
		return true;
	}
}

class SendGridEmailService implements EmailService {
	private apiKey: string;

	constructor(apiKey: string) {
		this.apiKey = apiKey;
	}

	async sendEmail(options: EmailOptions): Promise<boolean> {
		try {
			const response = await fetch('https://api.sendgrid.com/v3/mail/send', {
				method: 'POST',
				headers: {
					'Authorization': `Bearer ${this.apiKey}`,
					'Content-Type': 'application/json',
				},
				body: JSON.stringify({
					personalizations: [{
						to: [{ email: options.to }],
						subject: options.subject
					}],
					from: {
						email: EMAIL_FROM,
						name: EMAIL_FROM_NAME
					},
					content: [
						{
							type: 'text/html',
							value: options.html
						},
						...(options.text ? [{
							type: 'text/plain',
							value: options.text
						}] : [])
					]
				})
			});

			if (!response.ok) {
				console.error('SendGrid API error:', response.status, await response.text());
				return false;
			}

			console.log(`📧 Email sent successfully to ${options.to}`);
			return true;
		} catch (error) {
			console.error('SendGrid email error:', error);
			return false;
		}
	}
}

// Create email service instance based on configuration
function createEmailService(): EmailService {
	switch (EMAIL_PROVIDER) {
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

// Email templates
export function generatePasswordResetEmail(resetToken: string, userEmail: string): EmailOptions {
	const resetUrl = `${ORIGIN}/reset-password?token=${resetToken}`;
	
	const html = `
		<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f8fafc;">
			<div style="background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
				<div style="text-align: center; margin-bottom: 30px;">
					<h1 style="color: #1e293b; margin: 0;">🏆 Fantasy League</h1>
					<p style="color: #64748b; margin: 10px 0 0 0;">Password Reset Request</p>
				</div>
				
				<p style="color: #374151; margin-bottom: 20px;">Hi there!</p>
				
				<p style="color: #374151; margin-bottom: 20px;">
					We received a request to reset your password for your Fantasy League account. 
					If you didn't make this request, you can safely ignore this email.
				</p>
				
				<div style="text-align: center; margin: 30px 0;">
					<a href="${resetUrl}" 
					   style="background-color: #3b82f6; color: white; padding: 12px 24px; 
					          text-decoration: none; border-radius: 6px; font-weight: bold; 
					          display: inline-block;">
						Reset Your Password
					</a>
				</div>
				
				<p style="color: #64748b; font-size: 14px; margin-bottom: 10px;">
					This link will expire in 1 hour for security reasons.
				</p>
				
				<p style="color: #64748b; font-size: 14px; margin-bottom: 20px;">
					If the button doesn't work, copy and paste this link into your browser:
				</p>
				
				<p style="color: #64748b; font-size: 14px; word-break: break-all; background-color: #f1f5f9; padding: 10px; border-radius: 4px;">
					${resetUrl}
				</p>
				
				<hr style="border: none; border-top: 1px solid #e2e8f0; margin: 30px 0;">
				
				<p style="color: #64748b; font-size: 12px; text-align: center; margin: 0;">
					Fantasy League | Secure Password Management
				</p>
			</div>
		</div>
	`;

	const text = `
Fantasy League - Password Reset Request

Hi there!

We received a request to reset your password for your Fantasy League account. 
If you didn't make this request, you can safely ignore this email.

To reset your password, click this link or copy it into your browser:
${resetUrl}

This link will expire in 1 hour for security reasons.

Fantasy League | Secure Password Management
	`;

	return {
		to: userEmail,
		subject: 'Reset Your Fantasy League Password',
		html,
		text
	};
}

export function generateWelcomeEmail(username: string, userEmail: string): EmailOptions {
	const html = `
		<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f8fafc;">
			<div style="background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
				<div style="text-align: center; margin-bottom: 30px;">
					<h1 style="color: #1e293b; margin: 0;">🏆 Fantasy League</h1>
					<p style="color: #64748b; margin: 10px 0 0 0;">Welcome to the League!</p>
				</div>
				
				<p style="color: #374151; margin-bottom: 20px;">Hi ${username}!</p>
				
				<p style="color: #374151; margin-bottom: 20px;">
					Welcome to the Fantasy League! Your account has been successfully created and you're now part of our competitive community.
				</p>
				
				<div style="text-align: center; margin: 30px 0;">
					<a href="${ORIGIN}" 
					   style="background-color: #10b981; color: white; padding: 12px 24px; 
					          text-decoration: none; border-radius: 6px; font-weight: bold; 
					          display: inline-block;">
						Access Your League
					</a>
				</div>
				
				<p style="color: #374151; margin-bottom: 20px;">
					You can now access all league features including:
				</p>
				
				<ul style="color: #374151; margin-bottom: 20px;">
					<li>Hall of Fame rankings</li>
					<li>Manager profiles and statistics</li>
					<li>Rule proposals and voting</li>
					<li>Trade history and analysis</li>
					<li>Draft information</li>
				</ul>
				
				<hr style="border: none; border-top: 1px solid #e2e8f0; margin: 30px 0;">
				
				<p style="color: #64748b; font-size: 12px; text-align: center; margin: 0;">
					Fantasy League | Your Competitive Edge
				</p>
			</div>
		</div>
	`;

	return {
		to: userEmail,
		subject: 'Welcome to Fantasy League!',
		html
	};
} 