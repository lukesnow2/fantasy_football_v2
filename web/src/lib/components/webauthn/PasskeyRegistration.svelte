<script lang="ts">
  import { onMount } from 'svelte';
  import { createEventDispatcher } from 'svelte';
  import { WebAuthnBrowser, type WebAuthnError } from './browser';
  import { goto } from '$app/navigation';
  import { enhance } from '$app/forms';
  import { Fingerprint, Shield, AlertTriangle, CheckCircle, Loader2, ArrowRight, Key } from 'lucide-svelte';

  export let userId: string;
  export let username: string;
  export let managerKey: string | undefined = undefined;
  export let isRotationMode: boolean = false;

  const dispatch = createEventDispatcher();

  let credentialName = '';
  let isLoading = false;
  let error = '';
  let success = false;
  let isSupported = false;
  let isAvailable = false;
  let biometricType = '';

  onMount(async () => {
    isSupported = WebAuthnBrowser.isSupported();
    isAvailable = await WebAuthnBrowser.isPlatformAuthenticatorAvailable();
    biometricType = WebAuthnBrowser.getBiometricType();
    
    // Auto-generate credential name based on device
    const deviceType = WebAuthnBrowser.detectDeviceType();
    credentialName = `${deviceType.charAt(0).toUpperCase() + deviceType.slice(1)} ${biometricType}`;
  });

  async function handleRegistration() {
    if (!managerKey?.trim() && !isRotationMode) {
      error = 'Manager key is required';
      return;
    }

    isLoading = true;
    error = '';

    try {
      // Step 1: Get registration options from server
      const optionsResponse = await fetch('/api/webauthn/register/options', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, username, managerKey })
      });

      if (!optionsResponse.ok) {
        throw new Error('Failed to get registration options');
      }

      const { options, challengeId } = await optionsResponse.json();

      // Step 2: Register with browser
      const registrationResponse = await WebAuthnBrowser.register(options);

      // Step 3: Verify registration with server
      const verifyResponse = await fetch('/api/webauthn/register/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          response: registrationResponse,
          challengeId,
          managerKey,
          credentialName
        })
      });

      if (!verifyResponse.ok) {
        const errorData = await verifyResponse.json();
        throw new Error(errorData.error || 'Registration verification failed');
      }

      // If in rotation mode, call the rotation endpoint
      if (isRotationMode) {
        const rotationResponse = await fetch('/api/webauthn/credentials/rotate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            userId, 
            newCredentialId: registrationResponse.id 
          })
        });

        if (!rotationResponse.ok) {
          throw new Error('Failed to rotate credentials');
        }
      }

      success = true;
      // Emit success event for parent component to handle
      dispatch('registrationSuccess', { userId, username, managerKey });

    } catch (err) {
      const webauthnError = err as WebAuthnError;
      
      if (webauthnError.code === 'REGISTRATION_FAILED') {
        error = 'Biometric registration was cancelled or failed. Please try again.';
      } else {
        error = webauthnError.message || 'Registration failed. Please try again.';
      }
    } finally {
      isLoading = false;
    }
  }

  function handleRetry() {
    error = '';
    handleRegistration();
  }

  function handleSkip() {
    // Emit skip event for parent component to handle
    dispatch('registrationSkipped');
  }
</script>

<div class="passkey-registration">
  {#if success}
    <div class="success-state">
      <CheckCircle class="h-12 w-12 text-green-400" />
      <h2>Passkey Created Successfully!</h2>
      <p>Your passkey "{credentialName}" has been registered and is ready to use.</p>
      <div class="success-details">
        <p><strong>Device:</strong> {WebAuthnBrowser.detectDeviceType()}</p>
        <p><strong>Authentication:</strong> {biometricType}</p>
        <p><strong>Username:</strong> {username}</p>
      </div>
    </div>
  {:else}
    <div class="registration-form">
      <div class="form-header">
        <Fingerprint class="h-8 w-8 text-blue-400" />
        <h2>{isRotationMode ? 'Rotate Your Passkey' : 'Create Your Passkey'}</h2>
        <p>Set up biometric authentication for secure sign-in</p>
      </div>

      {#if !isRotationMode}
        <div class="manager-info">
          <Key class="h-5 w-5 text-blue-400" />
          <div>
            <h4>Manager Information</h4>
            <div class="info-grid">
              <div>
                <span class="label">Username:</span>
                <span class="value">{username}</span>
              </div>
              <div>
                <span class="label">Manager Key:</span>
                <span class="value">{managerKey || 'Not set'}</span>
              </div>
            </div>
          </div>
        </div>
      {/if}

      <div class="credential-section">
        <Shield class="h-5 w-5 text-blue-400" />
        <div>
          <h4>Passkey Details</h4>
          <div class="credential-info">
            <div>
              <span class="label">Device:</span>
              <span class="value">{WebAuthnBrowser.detectDeviceType()}</span>
            </div>
            <div>
              <span class="label">Authentication:</span>
              <span class="value">{biometricType}</span>
            </div>
            <div>
              <span class="label">Name:</span>
              <span class="value">{credentialName}</span>
            </div>
          </div>
        </div>
      </div>

      {#if error}
        <div class="error-message">
          <AlertTriangle class="h-4 w-4" />
          <div>
            <p>{error}</p>
            <button on:click={handleRetry} class="retry-button">
              Try Again
            </button>
          </div>
        </div>
      {/if}

      <div class="registration-steps">
        <h4>What happens next:</h4>
        <ol>
          <li>Click "Create Passkey" below</li>
          <li>Your browser will prompt for {biometricType}</li>
          <li>Complete the biometric verification</li>
          <li>Your passkey will be created and saved</li>
        </ol>
      </div>

      <div class="button-group">
        <button
          type="button"
          on:click={handleRegistration}
          disabled={isLoading}
          class="primary-button"
        >
          {#if isLoading}
            <Loader2 class="h-4 w-4 animate-spin" />
            <span>Creating Passkey...</span>
          {:else}
            <Fingerprint class="h-4 w-4" />
            <span>{isRotationMode ? 'Rotate Passkey' : 'Create Passkey'}</span>
          {/if}
        </button>

        {#if !isRotationMode}
          <button on:click={handleSkip} class="secondary-button">
            Skip for Now
          </button>
        {/if}
      </div>
    </div>
  {/if}
</div>

<style>
  .passkey-registration {
    width: 100%;
  }

  .success-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1rem;
    padding: 2rem;
    text-align: center;
    color: white;
  }

  .success-state h2 {
    margin: 0;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .success-state p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .success-details {
    background: rgba(51, 65, 85, 0.3);
    padding: 1rem;
    border-radius: 8px;
    margin-top: 1rem;
    border: 1px solid rgba(148, 163, 184, 0.1);
    text-align: left;
  }

  .success-details p {
    margin: 0.25rem 0;
    font-size: 0.875rem;
  }

  .registration-form {
    width: 100%;
  }

  .form-header {
    text-align: center;
    margin-bottom: 2rem;
  }

  .form-header h2 {
    margin: 0.5rem 0 0.25rem 0;
    color: white;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .form-header p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .manager-info, .credential-section {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    background: rgba(51, 65, 85, 0.3);
    padding: 1rem;
    border-radius: 8px;
    margin: 1rem 0;
    border: 1px solid rgba(148, 163, 184, 0.1);
  }

  .manager-info h4, .credential-section h4 {
    margin: 0 0 0.5rem 0;
    color: white;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .info-grid, .credential-info {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .label {
    color: #94a3b8;
    font-size: 0.75rem;
    font-weight: 500;
    display: block;
  }

  .value {
    color: #cbd5e1;
    font-size: 0.875rem;
    font-weight: 500;
  }

  .error-message {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.3);
    color: #fca5a5;
    padding: 1rem;
    border-radius: 8px;
    margin: 1rem 0;
  }

  .error-message p {
    margin: 0 0 0.5rem 0;
    font-size: 0.875rem;
  }

  .registration-steps {
    background: rgba(51, 65, 85, 0.3);
    padding: 1rem;
    border-radius: 8px;
    margin: 1.5rem 0;
    border: 1px solid rgba(148, 163, 184, 0.1);
  }

  .registration-steps h4 {
    margin: 0 0 0.75rem 0;
    color: white;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .registration-steps ol {
    margin: 0;
    padding-left: 1.25rem;
    color: #cbd5e1;
    font-size: 0.875rem;
  }

  .registration-steps li {
    margin-bottom: 0.25rem;
  }

  .button-group {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    margin-top: 1.5rem;
  }

  .primary-button {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding: 0.875rem 1rem;
    background: #3b82f6;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 0.875rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
  }

  .primary-button:hover:not(:disabled) {
    background: #2563eb;
  }

  .primary-button:disabled {
    background: #64748b;
    cursor: not-allowed;
  }

  .secondary-button {
    padding: 0.75rem 1rem;
    background: transparent;
    color: #94a3b8;
    border: 1px solid #475569;
    border-radius: 8px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.2s;
  }

  .secondary-button:hover {
    background: #475569;
    color: white;
  }

  .retry-button {
    margin-top: 0.5rem;
    padding: 0.5rem 1rem;
    background: #dc2626;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .retry-button:hover {
    background: #b91c1c;
  }
</style> 