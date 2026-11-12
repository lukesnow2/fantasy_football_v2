import { NODE_ENV, isProduction } from './env';

export enum LogLevel {
	ERROR = 'ERROR',
	WARN = 'WARN',
	INFO = 'INFO',
	DEBUG = 'DEBUG'
}

interface LogEntry {
	timestamp: string;
	level: LogLevel;
	message: string;
	context?: Record<string, any>;
	error?: Error;
	userId?: string;
	ip?: string;
	userAgent?: string;
}

class Logger {
	private logLevel: LogLevel;

	constructor() {
		// Set log level based on environment
		this.logLevel = isProduction ? LogLevel.WARN : LogLevel.DEBUG;
	}

	private shouldLog(level: LogLevel): boolean {
		const levels = [LogLevel.ERROR, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG];
		const currentLevelIndex = levels.indexOf(this.logLevel);
		const messageLevelIndex = levels.indexOf(level);
		return messageLevelIndex <= currentLevelIndex;
	}

	private formatLog(entry: LogEntry): string {
		const { timestamp, level, message, context, error, userId, ip, userAgent } = entry;
		
		if (isProduction) {
			// Structured JSON logging for production
			return JSON.stringify({
				timestamp,
				level,
				message,
				...(context && { context }),
				...(error && { 
					error: {
						name: error.name,
						message: error.message,
						stack: error.stack
					}
				}),
				...(userId && { userId }),
				...(ip && { ip }),
				...(userAgent && { userAgent })
			});
		} else {
			// Human-readable logging for development
			let logMessage = `[${timestamp}] ${level}: ${message}`;
			
			if (context && Object.keys(context).length > 0) {
				logMessage += `\n  Context: ${JSON.stringify(context, null, 2)}`;
			}
			
			if (error) {
				logMessage += `\n  Error: ${error.message}`;
				if (error.stack) {
					logMessage += `\n  Stack: ${error.stack}`;
				}
			}
			
			if (userId || ip || userAgent) {
				logMessage += `\n  User: ${userId || 'anonymous'} | IP: ${ip || 'unknown'} | Agent: ${userAgent || 'unknown'}`;
			}
			
			return logMessage;
		}
	}

	private log(level: LogLevel, message: string, options: Partial<LogEntry> = {}): void {
		if (!this.shouldLog(level)) return;

		const entry: LogEntry = {
			timestamp: new Date().toISOString(),
			level,
			message,
			...options
		};

		const formattedMessage = this.formatLog(entry);

		// Output to appropriate stream
		if (level === LogLevel.ERROR) {
			console.error(formattedMessage);
		} else {
			console.log(formattedMessage);
		}
	}

	error(message: string, error?: Error, context?: Record<string, any>, userInfo?: { userId?: string; ip?: string; userAgent?: string }): void {
		this.log(LogLevel.ERROR, message, { error, context, ...userInfo });
	}

	warn(message: string, context?: Record<string, any>, userInfo?: { userId?: string; ip?: string; userAgent?: string }): void {
		this.log(LogLevel.WARN, message, { context, ...userInfo });
	}

	info(message: string, context?: Record<string, any>, userInfo?: { userId?: string; ip?: string; userAgent?: string }): void {
		this.log(LogLevel.INFO, message, { context, ...userInfo });
	}

	debug(message: string, context?: Record<string, any>, userInfo?: { userId?: string; ip?: string; userAgent?: string }): void {
		this.log(LogLevel.DEBUG, message, { context, ...userInfo });
	}

	// Convenience methods for specific scenarios
	auth(message: string, userId?: string, ip?: string, success: boolean = true): void {
		this.info(`AUTH: ${message}`, { success, userId, ip });
	}

	api(method: string, path: string, status: number, userId?: string, ip?: string, duration?: number): void {
		this.info(`API: ${method} ${path} - ${status}`, { 
			method, 
			path, 
			status, 
			userId, 
			ip,
			...(duration && { duration })
		});
	}

	database(operation: string, table?: string, userId?: string, success: boolean = true): void {
		this.debug(`DB: ${operation}`, { table, userId, success });
	}
}

// Create singleton logger instance
export const logger = new Logger();

// Helper function to extract user info from request
export function getUserInfo(event: any): { userId?: string; ip?: string; userAgent?: string } {
	return {
		userId: event.locals?.user?.id,
		ip: event.getClientAddress?.() || event.request?.headers?.get('x-forwarded-for') || 'unknown',
		userAgent: event.request?.headers?.get('user-agent') || 'unknown'
	};
}

// Middleware helper for API logging
export function logApiCall(
	event: any, 
	method: string, 
	path: string, 
	status: number, 
	startTime: number = Date.now()
): void {
	const userInfo = getUserInfo(event);
	const duration = Date.now() - startTime;
	logger.api(method, path, status, userInfo.userId, userInfo.ip, duration);
} 