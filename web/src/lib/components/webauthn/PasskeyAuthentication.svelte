<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser, type WebAuthnError } from './browser';
  import { goto } from '$app/navigation';
  import { enhance } from '$app/forms';

  export let userId: string | undefined = undefined;

  let isSupported = false;
  let isAvailable = false;
  let isLoading = false;
  let error = '';
  let biometricType = '';
  let retryCount = 0;
  const maxRetries = 3;

  onMount(async () => {
    isSupported = WebAuthnBrowser.isSupported();
    isAvailable = await WebAuthnBrowser.isPlatformAuthenticatorAvailable();
    biometricType = WebAuthnBrowser.getBiometricType();
  });

  async function handleAuthentication() {
    isLoading = true;
    error = '';

    try {
      // Step 1: Get authentication options from server
      const optionsResponse = await fetch('/api/webauthn/authenticate/options', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: userId || undefined })
      });

      if (!optionsResponse.ok) {
        throw new Error('Failed to get authentication options');
      }

      const { options, challengeId } = await optionsResponse.json();

      // Step 2: Authenticate with browser
      const authenticationResponse = await WebAuthnBrowser.authenticate(options);

      // Step 3: Verify authentication with server
      const verifyResponse = await fetch('/api/webauthn/authenticate/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          response: authenticationResponse,
          challengeId
        })
      });

      if (!verifyResponse.ok) {
        const errorData = await verifyResponse.json();
        throw new Error(errorData.error || 'Authentication verification failed');
      }

      const { success, userId: authenticatedUserId } = await verifyResponse.json();
      
      if (success) {
        // Redirect to dashboard or intended page
        goto('/dashboard');
      } else {
        throw new Error('Authentication failed');
      }

    } catch (err) {
      const webauthnError = err as WebAuthnError;
      retryCount++;
      
      if (webauthnError.code === 'AUTHENTICATION_FAILED') {
        if (retryCount < maxRetries) {
          error = `Authentication was cancelled. Please try again. (${retryCount}/${maxRetries})`;
        } else {
          error = 'Authentication failed after multiple attempts. Please contact support.';
        }
      } else {
        error = webauthnError.message || 'Authentication failed. Please try again.';
      }
    } finally {
      isLoading = false;
    }
  }

  function handleRetry() {
    retryCount = 0;
    error = '';
    handleAuthentication();
  }

  function handleFallback() {
    // Redirect to password login or backup method
    goto('/login?fallback=true');
  }
</script>

<div class="passkey-authentication">
  {#if !isSupported}
    <div class="error-banner">
      <h3>⚠️ WebAuthn Not Supported</h3>
      <p>Your browser doesn't support passkeys. Please use a modern browser like Chrome, Safari, or Firefox.</p>
      <button on:click={handleFallback} class="fallback-button">
        Use Password Login
      </button>
    </div>
  {:else if !isAvailable}
    <div class="warning-banner">
      <h3>⚠️ Platform Authenticator Not Available</h3>
      <p>Your device doesn't support biometric authentication. You may need to enable it in your system settings.</p>
      <button on:click={handleFallback} class="fallback-button">
        Use Password Login
      </button>
    </div>
  {:else}
    <div class="authentication-form">
      <h2>🔐 Sign In with Passkey</h2>
      <p class="subtitle">Use {biometricType} to sign in securely</p>

      <div class="info-box">
        <h4>📱 How to sign in:</h4>
        <ol>
          <li>Click "Sign In with Passkey" below</li>
          <li>Your browser will prompt for {biometricType}</li>
          <li>Complete the biometric verification</li>
          <li>You'll be signed in automatically</li>
        </ol>
      </div>

      {#if error}
        <div class="error-message">
          <p>{error}</p>
          {#if retryCount < maxRetries}
            <button on:click={handleRetry} class="retry-button">
              Try Again
            </button>
          {/if}
        </div>
      {/if}

      <button
        type="button"
        on:click={handleAuthentication}
        disabled={isLoading}
        class="primary-button"
      >
        {#if isLoading}
          <span class="loading">Signing In...</span>
        {:else}
          <span>🔐 Sign In with Passkey</span>
        {/if}
      </button>

      <div class="fallback-options">
        <p>Having trouble?</p>
        <button on:click={handleFallback} class="secondary-button">
          Use Password Instead
        </button>
      </div>
    </div>
  {/if}
</div>

<style>
  .passkey-authentication {
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

  .authentication-form {
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
    margin-bottom: 1rem;
  }

  .primary-button:hover:not(:disabled) {
    background: #0056b3;
  }

  .primary-button:disabled {
    background: #6c757d;
    cursor: not-allowed;
  }

  .secondary-button {
    padding: 0.5rem 1rem;
    background: transparent;
    color: #007bff;
    border: 1px solid #007bff;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.2s;
  }

  .secondary-button:hover {
    background: #007bff;
    color: white;
  }

  .fallback-button {
    margin-top: 1rem;
    padding: 0.75rem 1rem;
    background: #6c757d;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .fallback-button:hover {
    background: #545b62;
  }

  .retry-button {
    margin-top: 0.5rem;
    padding: 0.5rem 1rem;
    background: #dc3545;
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .retry-button:hover {
    background: #c82333;
  }

  .fallback-options {
    text-align: center;
    margin-top: 1.5rem;
    padding-top: 1.5rem;
    border-top: 1px solid #e1e5e9;
  }

  .fallback-options p {
    margin: 0 0 0.5rem 0;
    color: #666;
    font-size: 0.875rem;
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