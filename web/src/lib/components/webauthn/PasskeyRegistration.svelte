<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser, type WebAuthnError } from './browser';
  import { goto } from '$app/navigation';
  import { enhance } from '$app/forms';

  export let userId: string;
  export let username: string;

  let managerKey = '';
  let credentialName = '';
  let isSupported = false;
  let isAvailable = false;
  let isLoading = false;
  let error = '';
  let success = false;
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
    if (!managerKey.trim()) {
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

      success = true;
      setTimeout(() => goto('/dashboard'), 2000);

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
</script>

<div class="passkey-registration">
  {#if !isSupported}
    <div class="error-banner">
      <h3>⚠️ WebAuthn Not Supported</h3>
      <p>Your browser doesn't support passkeys. Please use a modern browser like Chrome, Safari, or Firefox.</p>
    </div>
  {:else if !isAvailable}
    <div class="warning-banner">
      <h3>⚠️ Platform Authenticator Not Available</h3>
      <p>Your device doesn't support biometric authentication. You may need to enable it in your system settings.</p>
    </div>
  {:else}
    <div class="registration-form">
      <h2>🔐 Set Up Passkey</h2>
      <p class="subtitle">Secure your account with {biometricType}</p>

      <form method="POST" use:enhance>
        <div class="form-group">
          <label for="managerKey">Manager Key *</label>
          <input
            id="managerKey"
            type="text"
            bind:value={managerKey}
            placeholder="Enter your fantasy league manager key"
            required
            disabled={isLoading}
          />
          <small>This links your account to your fantasy league manager</small>
        </div>

        <div class="form-group">
          <label for="credentialName">Device Name</label>
          <input
            id="credentialName"
            type="text"
            bind:value={credentialName}
            placeholder="My iPhone"
            disabled={isLoading}
          />
          <small>Give this device a memorable name</small>
        </div>

        <div class="info-box">
          <h4>📱 What happens next?</h4>
          <ol>
            <li>Click "Create Passkey" below</li>
            <li>Your browser will prompt for {biometricType}</li>
            <li>Complete the biometric verification</li>
            <li>Your passkey will be saved securely</li>
          </ol>
        </div>

        {#if error}
          <div class="error-message">
            <p>{error}</p>
          </div>
        {/if}

        {#if success}
          <div class="success-message">
            <p>✅ Passkey created successfully! Redirecting...</p>
          </div>
        {/if}

        <button
          type="button"
          on:click={handleRegistration}
          disabled={isLoading || !managerKey.trim()}
          class="primary-button"
        >
          {#if isLoading}
            <span class="loading">Creating Passkey...</span>
          {:else}
            <span>🔐 Create Passkey</span>
          {/if}
        </button>
      </form>
    </div>
  {/if}
</div>

<style>
  .passkey-registration {
    max-width: 500px;
    margin: 0 auto;
    padding: 2rem;
  }

  .error-banner, .warning-banner {
    padding: 1rem;
    border-radius: 8px;
    margin-bottom: 1rem;
  }

  .error-banner {
    background: #fee;
    border: 1px solid #fcc;
    color: #c33;
  }

  .warning-banner {
    background: #fff3cd;
    border: 1px solid #ffeaa7;
    color: #856404;
  }

  .registration-form {
    background: white;
    padding: 2rem;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  }

  h2 {
    margin: 0 0 0.5rem 0;
    color: #333;
  }

  .subtitle {
    color: #666;
    margin-bottom: 2rem;
  }

  .form-group {
    margin-bottom: 1.5rem;
  }

  label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 600;
    color: #333;
  }

  input {
    width: 100%;
    padding: 0.75rem;
    border: 2px solid #e1e5e9;
    border-radius: 6px;
    font-size: 1rem;
    transition: border-color 0.2s;
  }

  input:focus {
    outline: none;
    border-color: #007bff;
  }

  input:disabled {
    background: #f8f9fa;
    cursor: not-allowed;
  }

  small {
    display: block;
    margin-top: 0.25rem;
    color: #666;
    font-size: 0.875rem;
  }

  .info-box {
    background: #f8f9fa;
    padding: 1rem;
    border-radius: 6px;
    margin: 1.5rem 0;
  }

  .info-box h4 {
    margin: 0 0 0.5rem 0;
    color: #333;
  }

  .info-box ol {
    margin: 0;
    padding-left: 1.5rem;
  }

  .info-box li {
    margin-bottom: 0.25rem;
    color: #555;
  }

  .error-message {
    background: #fee;
    color: #c33;
    padding: 0.75rem;
    border-radius: 6px;
    margin: 1rem 0;
  }

  .success-message {
    background: #d4edda;
    color: #155724;
    padding: 0.75rem;
    border-radius: 6px;
    margin: 1rem 0;
  }

  .primary-button {
    width: 100%;
    padding: 1rem;
    background: #007bff;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .primary-button:hover:not(:disabled) {
    background: #0056b3;
  }

  .primary-button:disabled {
    background: #6c757d;
    cursor: not-allowed;
  }

  .loading {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
  }

  .loading::before {
    content: '';
    width: 16px;
    height: 16px;
    border: 2px solid transparent;
    border-top: 2px solid currentColor;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style> 