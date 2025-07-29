import { env } from '$env/dynamic/private';

export interface WebAuthnLogEntry {
  timestamp: Date;
  operation: 'registration' | 'authentication' | 'challenge_created' | 'challenge_expired';
  userId?: string;
  credentialId?: string;
  success: boolean;
  error?: string;
  ip?: string;
  userAgent?: string;
  metadata?: Record<string, any>;
}

export class WebAuthnLogger {
  private static isStaging = env.NODE_ENV === 'staging';
  private static isDevelopment = env.NODE_ENV === 'development';

  static log(entry: WebAuthnLogEntry): void {
    const logMessage = this.formatLogEntry(entry);
    
    if (entry.success) {
      console.log(`✅ WebAuthn: ${logMessage}`);
    } else {
      console.error(`❌ WebAuthn: ${logMessage}`);
    }

    // In staging, log detailed payloads for debugging
    if (this.isStaging && entry.metadata) {
      console.log(`🔍 WebAuthn Debug:`, JSON.stringify(entry.metadata, null, 2));
    }
  }

  private static formatLogEntry(entry: WebAuthnLogEntry): string {
    const parts = [
      entry.operation,
      entry.userId ? `user:${entry.userId}` : '',
      entry.credentialId ? `credential:${entry.credentialId.slice(0, 8)}...` : '',
      entry.ip ? `ip:${entry.ip}` : '',
      entry.error ? `error:${entry.error}` : ''
    ].filter(Boolean);

    return parts.join(' | ');
  }

  static logRegistration(userId: string, success: boolean, error?: string, metadata?: Record<string, any>): void {
    this.log({
      timestamp: new Date(),
      operation: 'registration',
      userId,
      success,
      error,
      metadata: this.isStaging ? metadata : undefined
    });
  }

  static logAuthentication(userId: string, credentialId: string, success: boolean, error?: string, metadata?: Record<string, any>): void {
    this.log({
      timestamp: new Date(),
      operation: 'authentication',
      userId,
      credentialId,
      success,
      error,
      metadata: this.isStaging ? metadata : undefined
    });
  }

  static logChallengeCreated(challengeId: string, userId?: string): void {
    this.log({
      timestamp: new Date(),
      operation: 'challenge_created',
      userId,
      success: true,
      metadata: this.isStaging ? { challengeId } : undefined
    });
  }

  static logChallengeExpired(challengeId: string): void {
    this.log({
      timestamp: new Date(),
      operation: 'challenge_expired',
      success: false,
      error: 'Challenge expired',
      metadata: this.isStaging ? { challengeId } : undefined
    });
  }

  static logFailedVerification(operation: 'registration' | 'authentication', error: string, metadata?: Record<string, any>): void {
    this.log({
      timestamp: new Date(),
      operation,
      success: false,
      error,
      metadata: this.isStaging ? metadata : undefined
    });
  }
} 