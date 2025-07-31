<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser, type WebAuthnError } from './browser';
  import { goto } from '$app/navigation';
  import { enhance } from '$app/forms';
  import { Fingerprint, Shield, AlertTriangle, ArrowRight } from 'lucide-svelte';

  export let username: string | undefined = undefined;

  let isSupported = false;
  let isAvailable = false;
  let isLoading = false;
  let error = '';
  let biometricType = '';
  let retryCount = 0;
  let currentFlow: 'authentication' | 'registration' | null = null;
  let flowMessage = '';
  const maxRetries = 3;

  onMount(async () => {
    console.log('🔧 PasskeyAuthentication component mounting...');
    console.log('🌐 Checking WebAuthn support...');
    
    isSupported = WebAuthnBrowser.isSupported();
    console.log('✅ WebAuthn supported:', isSupported);
    
    if (isSupported) {
      console.log('🔍 Checking platform authenticator availability...');
      isAvailable = await WebAuthnBrowser.isPlatformAuthenticatorAvailable();
      console.log('✅ Platform authenticator available:', isAvailable);
      
      if (isAvailable) {
        biometricType = WebAuthnBrowser.getBiometricType();
        console.log('📱 Biometric type detected:', biometricType);
      }
    }
    
    console.log('🎯 Final state:', { isSupported, isAvailable, biometricType });
  });

  async function handleAuthentication() {
    isLoading = true;
    error = '';

    try {
      console.log('🔐 Starting WebAuthn authentication...');
      console.log('📊 Current state:', { isSupported, isAvailable, biometricType, username });
      
      // Step 1: Get authentication options from server
      console.log('🌐 Making request to /api/webauthn/authenticate/options...');
      const optionsResponse = await fetch('/api/webauthn/authenticate/options', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username || undefined })
      });

      console.log('📡 Response received:', {
        status: optionsResponse.status,
        statusText: optionsResponse.statusText,
        ok: optionsResponse.ok,
        headers: Object.fromEntries(optionsResponse.headers.entries())
      });

      if (!optionsResponse.ok) {
        const errorText = await optionsResponse.text();
        console.error('❌ Authentication options request failed:', {
          status: optionsResponse.status,
          statusText: optionsResponse.statusText,
          errorText
        });
        throw new Error(`Failed to get authentication options: ${optionsResponse.status} ${optionsResponse.statusText}`);
      }

      const responseData = await optionsResponse.json();
      console.log('✅ Authentication options received:', {
        hasOptions: !!responseData.options,
        hasChallengeId: !!responseData.challengeId,
        optionsKeys: responseData.options ? Object.keys(responseData.options) : [],
        challengeId: responseData.challengeId,
        flow: responseData.flow,
        message: responseData.message
      });

      const { options, challengeId, flow, message } = responseData;
      currentFlow = flow;
      flowMessage = message;

      // If this is a registration flow (no passkey exists), redirect to setup page
      if (flow === 'registration') {
        console.log('🔄 No passkey found, redirecting to setup page...');
        // Get the userId from the server response or make a separate request
        const userId = responseData.userId;
        if (userId) {
          goto(`/setup-passkey?userId=${userId}&username=${encodeURIComponent(username || '')}&managerKey=${responseData.managerKey || ''}`);
        } else {
          // Fallback: redirect to login with error message
          goto('/login?message=setup-passkey-required');
        }
        return;
      }

      // Step 2: Authenticate with browser
      console.log('🔑 Starting browser authentication...');
      const authenticationResponse = await WebAuthnBrowser.authenticate(options);
      console.log('✅ Browser authentication completed:', {
        hasResponse: !!authenticationResponse,
        responseKeys: authenticationResponse ? Object.keys(authenticationResponse) : []
      });

      // Step 3: Verify authentication with server
      console.log('🔍 Verifying authentication with server...');
      const verifyResponse = await fetch('/api/webauthn/authenticate/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          response: authenticationResponse,
          challengeId
        })
      });

      console.log('📡 Verification response received:', {
        status: verifyResponse.status,
        statusText: verifyResponse.statusText,
        ok: verifyResponse.ok
      });

      if (!verifyResponse.ok) {
        const errorData = await verifyResponse.json();
        console.error('❌ Authentication verification failed:', errorData);
        throw new Error(errorData.error || 'Authentication verification failed');
      }

      const { success, userId: authenticatedUserId } = await verifyResponse.json();
      console.log('✅ Authentication verification completed:', { success, authenticatedUserId });
      
      if (success) {
        // Redirect to dashboard or intended page
        console.log('🚀 Authentication successful, redirecting to dashboard...');
        goto('/dashboard');
      } else {
        throw new Error('Authentication failed');
      }

    } catch (err) {
      const webauthnError = err as WebAuthnError;
      retryCount++;
      
      console.error('💥 Authentication error:', {
        error: webauthnError,
        message: webauthnError.message,
        code: webauthnError.code,
        retryCount,
        maxRetries
      });
      
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
      <AlertTriangle class="h-5 w-5" />
      <div>
        <h3>WebAuthn Not Supported</h3>
        <p>Your browser doesn't support passkeys. Please use a modern browser like Chrome, Safari, or Firefox.</p>
      </div>
      <button on:click={handleFallback} class="fallback-button">
        Use Password Login
      </button>
    </div>
  {:else if !isAvailable}
    <div class="warning-banner">
      <AlertTriangle class="h-5 w-5" />
      <div>
        <h3>Platform Authenticator Not Available</h3>
        <p>Your device doesn't support biometric authentication. You may need to enable it in your system settings.</p>
      </div>
      <button on:click={handleFallback} class="fallback-button">
        Use Password Login
      </button>
    </div>
  {:else}
    <div class="authentication-form">
      <div class="form-header">
        <Fingerprint class="h-8 w-8 text-blue-400" />
        <h2>
          {#if currentFlow === 'registration'}
            Set Up Your Passkey
          {:else if currentFlow === 'authentication'}
            Sign In with Passkey
          {:else}
            Passkey Authentication
          {/if}
        </h2>
        <p class="subtitle">
          {#if currentFlow === 'registration'}
            Create your first passkey for secure sign-in
          {:else if currentFlow === 'authentication'}
            Use {biometricType} to sign in securely
          {:else}
            Use {biometricType} to sign in securely
          {/if}
        </p>
        {#if flowMessage}
          <p class="flow-message">{flowMessage}</p>
        {/if}
      </div>

      <div class="info-box">
        <Shield class="h-5 w-5 text-blue-400" />
        <div>
          <h4>
            {#if currentFlow === 'registration'}
              How to set up your passkey:
            {:else}
              How to sign in:
            {/if}
          </h4>
          <ol>
            {#if currentFlow === 'registration'}
              <li>Click "Set Up Passkey" below</li>
              <li>Your browser will prompt for {biometricType}</li>
              <li>Complete the biometric verification</li>
              <li>Your passkey will be created and you'll be signed in</li>
            {:else}
              <li>Click "Sign In with Passkey" below</li>
              <li>Your browser will prompt for {biometricType}</li>
              <li>Complete the biometric verification</li>
              <li>You'll be signed in automatically</li>
            {/if}
          </ol>
        </div>
      </div>

      {#if error}
        <div class="error-message">
          <AlertTriangle class="h-4 w-4" />
          <div>
            <p>{error}</p>
            {#if retryCount < maxRetries}
              <button on:click={handleRetry} class="retry-button">
                Try Again
              </button>
            {/if}
          </div>
        </div>
      {/if}

      <button
        type="button"
        on:click={handleAuthentication}
        disabled={isLoading}
        class="primary-button"
      >
        {#if isLoading}
          <span class="loading">
            {#if currentFlow === 'registration'}
              Setting Up...
            {:else}
              Signing In...
            {/if}
          </span>
        {:else}
          <Fingerprint class="h-4 w-4" />
          <span>
            {#if currentFlow === 'registration'}
              Set Up Your First Passkey
            {:else}
              Sign In with Passkey
            {/if}
          </span>
        {/if}
      </button>

      <div class="fallback-options">
        <p>Having trouble?</p>
        <button on:click={handleFallback} class="secondary-button">
          <ArrowRight class="h-4 w-4" />
          Use Password Instead
        </button>
      </div>
    </div>
  {/if}
</div>

<style>
  .passkey-authentication {
    width: 100%;
  }

  .error-banner, .warning-banner {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    padding: 1rem;
    border-radius: 8px;
    margin-bottom: 1.5rem;
  }

  .error-banner {
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.3);
    color: #fca5a5;
  }

  .warning-banner {
    background: rgba(245, 158, 11, 0.1);
    border: 1px solid rgba(245, 158, 11, 0.3);
    color: #fcd34d;
  }

  .error-banner h3, .warning-banner h3 {
    margin: 0 0 0.5rem 0;
    font-size: 1rem;
    font-weight: 600;
  }

  .error-banner p, .warning-banner p {
    margin: 0 0 1rem 0;
    font-size: 0.875rem;
  }

  .authentication-form {
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

  .subtitle {
    color: #94a3b8;
    margin: 0;
    font-size: 0.875rem;
  }

  .flow-message {
    color: #94a3b8;
    margin-top: 0.5rem;
    font-size: 0.875rem;
  }

  .info-box {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    background: rgba(51, 65, 85, 0.3);
    padding: 1rem;
    border-radius: 8px;
    margin: 1.5rem 0;
    border: 1px solid rgba(148, 163, 184, 0.1);
  }

  .info-box h4 {
    margin: 0 0 0.5rem 0;
    color: white;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .info-box ol {
    margin: 0;
    padding-left: 1.25rem;
    color: #cbd5e1;
    font-size: 0.875rem;
  }

  .info-box li {
    margin-bottom: 0.25rem;
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

  .primary-button {
    width: 100%;
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
    margin-bottom: 1rem;
  }

  .primary-button:hover:not(:disabled) {
    background: #2563eb;
  }

  .primary-button:disabled {
    background: #64748b;
    cursor: not-allowed;
  }

  .secondary-button {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    background: transparent;
    color: #94a3b8;
    border: 1px solid #475569;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.2s;
  }

  .secondary-button:hover {
    background: #475569;
    color: white;
  }

  .fallback-button {
    margin-top: 0.75rem;
    padding: 0.5rem 1rem;
    background: #64748b;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .fallback-button:hover {
    background: #475569;
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

  .fallback-options {
    text-align: center;
    margin-top: 1.5rem;
    padding-top: 1.5rem;
    border-top: 1px solid rgba(148, 163, 184, 0.1);
  }

  .fallback-options p {
    margin: 0 0 0.75rem 0;
    color: #94a3b8;
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