import { 
  generateRegistrationOptions, 
  verifyRegistrationResponse,
  generateAuthenticationOptions,
  verifyAuthenticationResponse,
  type VerifiedRegistrationResponse,
  type VerifiedAuthenticationResponse
} from '@simplewebauthn/server';
import { webauthnConfig } from './config';
import { ChallengeManager } from './challenges';
import { CredentialManager } from './credentials';

export class WebAuthnCore {
  static async generateRegistrationOptions(userId: string, username: string) {
    const challenge = await ChallengeManager.create(userId, 'registration');
    
    const options = await generateRegistrationOptions({
      rpName: webauthnConfig.rpName,
      rpID: webauthnConfig.rpID,
      userID: new TextEncoder().encode(userId),
      userName: username,
      challenge: challenge.challenge,
      attestationType: webauthnConfig.attestation,
      authenticatorSelection: {
        authenticatorAttachment: webauthnConfig.authenticatorAttachment,
        residentKey: webauthnConfig.residentKey,
        userVerification: webauthnConfig.userVerification,
      },
      timeout: webauthnConfig.timeout,
      supportedAlgorithmIDs: [...webauthnConfig.algorithms],
    });

    return { options, challengeId: challenge.id };
  }

  static async verifyRegistrationResponse(
    response: any, 
    challengeId: string,
    origin: string
  ): Promise<VerifiedRegistrationResponse> {
    const challenge = await ChallengeManager.validate(challengeId);
    if (!challenge) {
      throw new Error('Invalid or expired challenge');
    }

    const verification = await verifyRegistrationResponse({
      response,
      expectedChallenge: challenge.challenge,
      expectedOrigin: webauthnConfig.rpOrigin,
      expectedRPID: webauthnConfig.rpID,
      requireUserVerification: true,
    });

    if (verification.verified) {
      await ChallengeManager.delete(challengeId);
    }

    return verification;
  }

  static async generateAuthenticationOptions(userId?: string) {
    const challenge = await ChallengeManager.create(userId, 'authentication');
    
    const options = await generateAuthenticationOptions({
      rpID: webauthnConfig.rpID,
      challenge: challenge.challenge,
      userVerification: webauthnConfig.userVerification,
      timeout: webauthnConfig.timeout,
    });

    return { options, challengeId: challenge.id };
  }

  static async verifyAuthenticationResponse(
    response: any,
    challengeId: string,
    origin: string
  ): Promise<VerifiedAuthenticationResponse> {
    const challenge = await ChallengeManager.validate(challengeId);
    if (!challenge) {
      throw new Error('Invalid or expired challenge');
    }

    // Get the credential for verification
    const credential = await CredentialManager.getByCredentialId(response.id);
    if (!credential) {
      throw new Error('Credential not found');
    }

    const verification = await verifyAuthenticationResponse({
      response,
      expectedChallenge: challenge.challenge,
      expectedOrigin: webauthnConfig.rpOrigin,
      expectedRPID: webauthnConfig.rpID,
      credential: {
        id: credential.credentialId,
        publicKey: Buffer.from(credential.publicKey, 'base64'),
        counter: credential.signCount,
        transports: credential.transports as any || [],
      },
      requireUserVerification: true,
    });

    if (verification.verified) {
      // Update sign count to prevent replay attacks
      await CredentialManager.updateSignCount(
        credential.credentialId, 
        verification.authenticationInfo.newCounter
      );
      await ChallengeManager.delete(challengeId);
    }

    return verification;
  }
} 