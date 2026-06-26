import { WebAuthnCore } from './core';
import { CredentialManager } from './credentials';
import { ChallengeManager } from './challenges';
import { WebAuthnLogger } from './logging';
import { db } from '$lib/server/db';
import { user } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';

export class WebAuthnService {
  static async startRegistration(userId: string, username: string) {
    try {
      const { options, challengeId } = await WebAuthnCore.generateRegistrationOptions(userId, username);
      
      WebAuthnLogger.logChallengeCreated(challengeId, userId);
      
      return { options, challengeId };
    } catch (error) {
      WebAuthnLogger.logFailedVerification('registration', error instanceof Error ? error.message : 'Unknown error');
      throw error;
    }
  }

  static async completeRegistration(
    response: any,
    challengeId: string,
    userId: string,
    origin: string
  ) {
    try {
      const verification = await WebAuthnCore.verifyRegistrationResponse(response, challengeId, origin);
      
      if (verification.verified && verification.registrationInfo) {
        // Store the credential
        await CredentialManager.create({
          userId,
          credentialId: verification.registrationInfo.credential.id,
          publicKey: Buffer.from(verification.registrationInfo.credential.publicKey).toString('base64'),
          signCount: verification.registrationInfo.credential.counter,
          backupEligible: verification.registrationInfo.credentialDeviceType === 'multiDevice',
          backupState: verification.registrationInfo.credentialBackedUp,
          transports: response.response.transports || [],
          deviceType: this.detectDeviceType(),
          authenticatorType: 'platform'
        });

        // Update user to indicate passkey is enabled
        await db
          .update(user)
          .set({ 
            passkeyEnabled: true,
            passkeyRegisteredAt: new Date()
          })
          .where(eq(user.id, userId));

        WebAuthnLogger.logRegistration(userId, true);
        return { success: true };
      } else {
        WebAuthnLogger.logRegistration(userId, false, 'Verification failed');
        throw new Error('Registration verification failed');
      }
    } catch (error) {
      WebAuthnLogger.logRegistration(userId, false, error instanceof Error ? error.message : 'Unknown error');
      throw error;
    }
  }

  static async startAuthentication(userId?: string) {
    try {
      const { options, challengeId } = await WebAuthnCore.generateAuthenticationOptions(userId);
      
      WebAuthnLogger.logChallengeCreated(challengeId, userId);
      
      return { options, challengeId };
    } catch (error) {
      WebAuthnLogger.logFailedVerification('authentication', error instanceof Error ? error.message : 'Unknown error');
      throw error;
    }
  }

  static async completeAuthentication(
    response: any,
    challengeId: string,
    origin: string
  ) {
    try {
      const verification = await WebAuthnCore.verifyAuthenticationResponse(response, challengeId, origin);
      
      if (verification.verified && verification.authenticationInfo) {
        const credential = await CredentialManager.getByCredentialId(response.id);
        if (!credential) {
          throw new Error('Credential not found after verification');
        }

        WebAuthnLogger.logAuthentication(credential.userId, credential.credentialId, true);
        return { 
          success: true, 
          userId: credential.userId,
          credentialId: credential.credentialId
        };
      } else {
        WebAuthnLogger.logAuthentication('unknown', response.id, false, 'Verification failed');
        throw new Error('Authentication verification failed');
      }
    } catch (error) {
      WebAuthnLogger.logAuthentication('unknown', response.id, false, error instanceof Error ? error.message : 'Unknown error');
      throw error;
    }
  }

  static async getUserCredentials(userId: string) {
    return await CredentialManager.getActiveCredentials(userId);
  }

  static async deleteCredential(credentialId: string, userId: string) {
    const credential = await CredentialManager.getByCredentialId(credentialId);
    if (!credential || credential.userId !== userId) {
      throw new Error('Credential not found or access denied');
    }

    await CredentialManager.delete(credential.id);
    WebAuthnLogger.log({
      timestamp: new Date(),
      operation: 'registration',
      userId,
      credentialId,
      success: true,
      metadata: { action: 'credential_deleted' }
    });
  }

  private static detectDeviceType(): string {
    // This would be enhanced with actual device detection logic
    return 'unknown';
  }
} 