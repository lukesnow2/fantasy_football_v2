import { logAuditEvent, AuditEventType, AuditSeverity } from './audit';

export enum AttestationConveyancePreference {
	NONE = 'none',
	INDIRECT = 'indirect',
	DIRECT = 'direct'
}

export enum AttestationFormat {
	PACKED = 'packed',
	TPM = 'tpm',
	ANDROID_KEY = 'android-key',
	ANDROID_SAFETYNET = 'android-safetynet',
	FIDO_U2F = 'fido-u2f',
	APPLE = 'apple',
	NONE = 'none'
}

export interface AttestationPolicy {
	conveyancePreference: AttestationConveyancePreference;
	acceptedFormats: AttestationFormat[];
	requireVerification: boolean;
	allowSelfAttestation: boolean;
	requireBackupEligibility: boolean;
	requireUserVerification: boolean;
	requireResidentKey: boolean;
}

export interface AttestationVerificationResult {
	valid: boolean;
	format?: AttestationFormat;
	trustPath?: string[];
	verificationData?: any;
	error?: string;
}

// Default attestation policies for different security levels
export const AttestationPolicies = {
	// High security - requires attestation verification
	HIGH: {
		conveyancePreference: AttestationConveyancePreference.DIRECT,
		acceptedFormats: [
			AttestationFormat.PACKED,
			AttestationFormat.TPM,
			AttestationFormat.ANDROID_KEY,
			AttestationFormat.APPLE
		],
		requireVerification: true,
		allowSelfAttestation: false,
		requireBackupEligibility: true,
		requireUserVerification: true,
		requireResidentKey: true
	} as AttestationPolicy,

	// Medium security - accepts indirect attestation
	MEDIUM: {
		conveyancePreference: AttestationConveyancePreference.INDIRECT,
		acceptedFormats: [
			AttestationFormat.PACKED,
			AttestationFormat.TPM,
			AttestationFormat.ANDROID_KEY,
			AttestationFormat.APPLE,
			AttestationFormat.FIDO_U2F
		],
		requireVerification: false,
		allowSelfAttestation: false,
		requireBackupEligibility: true,
		requireUserVerification: true,
		requireResidentKey: false
	} as AttestationPolicy,

	// Low security - accepts any attestation
	LOW: {
		conveyancePreference: AttestationConveyancePreference.NONE,
		acceptedFormats: [
			AttestationFormat.PACKED,
			AttestationFormat.TPM,
			AttestationFormat.ANDROID_KEY,
			AttestationFormat.ANDROID_SAFETYNET,
			AttestationFormat.FIDO_U2F,
			AttestationFormat.APPLE,
			AttestationFormat.NONE
		],
		requireVerification: false,
		allowSelfAttestation: true,
		requireBackupEligibility: false,
		requireUserVerification: false,
		requireResidentKey: false
	} as AttestationPolicy
};

export function getAttestationPolicy(securityLevel: 'HIGH' | 'MEDIUM' | 'LOW' = 'MEDIUM'): AttestationPolicy {
	return AttestationPolicies[securityLevel];
}

export function createAttestationOptions(policy: AttestationPolicy, challenge: string, rpName: string, rpId: string) {
	return {
		challenge: challenge,
		rp: {
			name: rpName,
			id: rpId
		},
		user: {
			id: new Uint8Array(16), // Will be set by caller
			name: '', // Will be set by caller
			displayName: '' // Will be set by caller
		},
		pubKeyCredParams: [
			{ alg: -7, type: 'public-key' }, // ES256
			{ alg: -257, type: 'public-key' } // RS256
		],
		timeout: 60000, // 60 seconds
		attestation: policy.conveyancePreference,
		authenticatorSelection: {
			authenticatorAttachment: 'platform',
			userVerification: policy.requireUserVerification ? 'required' : 'preferred',
			requireResidentKey: policy.requireResidentKey,
			residentKey: policy.requireResidentKey ? 'required' : 'preferred'
		},
		extensions: {
			credProps: true,
			appid: undefined,
			uvm: true
		}
	};
}

export async function verifyAttestation(
	attestationResponse: any,
	expectedChallenge: string,
	policy: AttestationPolicy,
	context?: { userId?: string; ipAddress?: string }
): Promise<AttestationVerificationResult> {
	try {
		// Basic validation
		if (!attestationResponse || !attestationResponse.attestationObject) {
			return {
				valid: false,
				error: 'Invalid attestation response'
			};
		}

		// Parse attestation object
		const attestationObject = attestationResponse.attestationObject;
		
		// Check if attestation format is accepted
		const format = attestationObject.fmt;
		if (!policy.acceptedFormats.includes(format as AttestationFormat)) {
			await logAuditEvent(
				AuditEventType.REGISTRATION_FAILED,
				AuditSeverity.WARNING,
				{
					reason: 'unaccepted_attestation_format',
					format,
					acceptedFormats: policy.acceptedFormats
				},
				context
			);
			
			return {
				valid: false,
				format: format as AttestationFormat,
				error: `Attestation format '${format}' not accepted`
			};
		}

		// Check for self-attestation if not allowed
		if (!policy.allowSelfAttestation && format === AttestationFormat.NONE) {
			await logAuditEvent(
				AuditEventType.REGISTRATION_FAILED,
				AuditSeverity.WARNING,
				{
					reason: 'self_attestation_not_allowed',
					format
				},
				context
			);
			
			return {
				valid: false,
				format: format as AttestationFormat,
				error: 'Self-attestation not allowed'
			};
		}

		// Verify challenge
		const clientData = attestationResponse.clientDataJSON;
		const clientDataObj = JSON.parse(new TextDecoder().decode(clientData));
		
		if (clientDataObj.challenge !== expectedChallenge) {
			await logAuditEvent(
				AuditEventType.REGISTRATION_FAILED,
				AuditSeverity.ERROR,
				{
					reason: 'challenge_mismatch',
					expectedChallenge: expectedChallenge.substring(0, 8) + '...',
					receivedChallenge: clientDataObj.challenge?.substring(0, 8) + '...'
				},
				context
			);
			
			return {
				valid: false,
				error: 'Challenge verification failed'
			};
		}

		// Check origin
		const expectedOrigin = `https://${process.env.DOMAIN || 'localhost'}`;
		if (clientDataObj.origin !== expectedOrigin) {
			await logAuditEvent(
				AuditEventType.SECURITY_VIOLATION,
				AuditSeverity.CRITICAL,
				{
					reason: 'origin_mismatch',
					expectedOrigin,
					receivedOrigin: clientDataObj.origin
				},
				context
			);
			
			return {
				valid: false,
				error: 'Origin verification failed'
			};
		}

		// For high security, perform additional verification
		if (policy.requireVerification && format !== AttestationFormat.NONE) {
			// This would typically involve verifying the attestation statement
			// against a trusted attestation root or certificate authority
			// For now, we'll log that verification would be required
			await logAuditEvent(
				AuditEventType.REGISTRATION_COMPLETED,
				AuditSeverity.INFO,
				{
					format,
					verificationRequired: true,
					note: 'Attestation verification would be performed in production'
				},
				context
			);
		}

		// Log successful attestation
		await logAuditEvent(
			AuditEventType.REGISTRATION_COMPLETED,
			AuditSeverity.INFO,
			{
				format,
				verificationRequired: policy.requireVerification,
				policyLevel: policy.conveyancePreference
			},
			context
		);

		return {
			valid: true,
			format: format as AttestationFormat,
			verificationData: {
				format,
				conveyancePreference: policy.conveyancePreference,
				verificationRequired: policy.requireVerification
			}
		};

	} catch (error) {
		await logAuditEvent(
			AuditEventType.REGISTRATION_FAILED,
			AuditSeverity.ERROR,
			{
				reason: 'attestation_verification_error',
				error: error instanceof Error ? error.message : String(error)
			},
			context
		);

		return {
			valid: false,
			error: error instanceof Error ? error.message : 'Attestation verification failed'
		};
	}
}

export function getAttestationPolicyForUser(userId: string, userRole?: string): AttestationPolicy {
	// In a real application, you might determine policy based on:
	// - User role (admin vs regular user)
	// - User's security requirements
	// - Compliance requirements
	// - Risk assessment
	
	if (userRole === 'admin') {
		return AttestationPolicies.HIGH;
	}
	
	// Default to medium security for regular users
	return AttestationPolicies.MEDIUM;
} 