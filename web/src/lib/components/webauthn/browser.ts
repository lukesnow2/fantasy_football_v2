import { 
  startRegistration, 
  startAuthentication,
  type RegistrationResponseJSON,
  type AuthenticationResponseJSON
} from '@simplewebauthn/browser';

export interface WebAuthnError extends Error {
  code: string;
  details?: any;
}

export class WebAuthnBrowser {
  static async register(options: any): Promise<RegistrationResponseJSON> {
    try {
      const response = await startRegistration(options);
      return response;
    } catch (error) {
      const webauthnError = new Error(
        error instanceof Error ? error.message : 'Registration failed'
      ) as WebAuthnError;
      webauthnError.code = 'REGISTRATION_FAILED';
      webauthnError.details = error;
      throw webauthnError;
    }
  }

  static async authenticate(options: any): Promise<AuthenticationResponseJSON> {
    try {
      const response = await startAuthentication(options);
      return response;
    } catch (error) {
      const webauthnError = new Error(
        error instanceof Error ? error.message : 'Authentication failed'
      ) as WebAuthnError;
      webauthnError.code = 'AUTHENTICATION_FAILED';
      webauthnError.details = error;
      throw webauthnError;
    }
  }

  static isSupported(): boolean {
    return typeof window !== 'undefined' && 
           'credentials' in navigator && 
           'create' in navigator.credentials! &&
           'get' in navigator.credentials!;
  }

  static isPlatformAuthenticatorAvailable(): Promise<boolean> {
    if (!this.isSupported()) return Promise.resolve(false);
    
    return window.PublicKeyCredential?.isUserVerifyingPlatformAuthenticatorAvailable?.() || 
           Promise.resolve(false);
  }

  static detectDeviceType(): string {
    if (typeof window === 'undefined') return 'unknown';
    
    const userAgent = navigator.userAgent.toLowerCase();
    if (/iphone|ipad|ipod/.test(userAgent)) return 'ios';
    if (/android/.test(userAgent)) return 'android';
    if (/windows/.test(userAgent)) return 'windows';
    if (/macintosh|mac os x/.test(userAgent)) return 'macos';
    if (/linux/.test(userAgent)) return 'linux';
    
    return 'unknown';
  }

  static getBiometricType(): string {
    const deviceType = this.detectDeviceType();
    switch (deviceType) {
      case 'ios': return 'Face ID / Touch ID';
      case 'android': return 'Fingerprint / Face';
      case 'windows': return 'Windows Hello';
      case 'macos': return 'Touch ID';
      default: return 'Biometric';
    }
  }
} 