import { env } from '$env/dynamic/private';

export const webauthnConfig = {
  // Relying Party Configuration
  rpName: env.WEBAUTHN_RP_NAME || 'The League',
  rpID: env.WEBAUTHN_RP_ID || 'localhost',
  rpOrigin: env.WEBAUTHN_RP_ORIGIN || 'http://localhost:5173',
  
  // Security Configuration
  attestation: 'none' as const, // Prevents tracking across sites
  authenticatorAttachment: 'platform' as const, // Biometric-only
  residentKey: 'preferred' as const, // Enables passkey sync
  userVerification: 'preferred' as const, // Biometric verification
  
  // Challenge Configuration
  challengeTimeout: 60000, // 60 seconds
  challengeLength: 32, // 256-bit challenge
  
  // Credential Configuration
  algorithms: [-7, -257], // ES256, RS256
  pubKeyCredParams: [
    { alg: -7, type: 'public-key' }, // ES256
    { alg: -257, type: 'public-key' } // RS256
  ],
  
  // Timeout Configuration
  timeout: 60000, // 60 seconds for user interaction
} as const;

// Type-safe configuration access
export type WebAuthnConfig = typeof webauthnConfig; 