<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser } from './browser';
  import PasskeyRegistration from './PasskeyRegistration.svelte';
  import { goto } from '$app/navigation';

  export let userId: string;
  export let username: string;
  export let managerKey: string | undefined = undefined;

  let currentStep = 1;
  let isLoading = true;
  let error = '';
  let isSupported = false;
  let isAvailable = false;
  let biometricType = '';
  let setupComplete = false;

  onMount(async () => {
    isSupported = WebAuthnBrowser.isSupported();
    isAvailable = await WebAuthnBrowser.isPlatformAuthenticatorAvailable();
    biometricType = WebAuthnBrowser.getBiometricType();
    isLoading = false;
  });

  function nextStep() {
    if (currentStep < 3) {
      currentStep++;
    }
  }

  function previousStep() {
    if (currentStep > 1) {
      currentStep--;
    }
  }

  function handleSetupComplete() {
    setupComplete = true;
    // Create session for the user
    fetch('/api/webauthn/setup-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId })
    }).then(response => {
      if (response.ok) {
        setTimeout(() => goto('/dashboard'), 2000);
      } else {
        console.error('Failed to create session');
        setTimeout(() => goto('/dashboard'), 2000);
      }
    }).catch(error => {
      console.error('Session creation error:', error);
      setTimeout(() => goto('/dashboard'), 2000);
    });
  }

  function handleRegistrationSuccess(event: CustomEvent) {
    handleSetupComplete();
  }

  function handleSkip() {
    // Redirect to dashboard with a note that passkey setup is recommended
    goto('/dashboard?setup=skipped');
  }
</script>

<div class="first-time-setup">
  {#if isLoading}
    <div class="loading-state">
      <div class="spinner"></div>
      <p>Loading setup...</p>
    </div>
  {:else if !isSupported}
    <div class="error-banner">
      <h3>⚠️ WebAuthn Not Supported</h3>
      <p>Your browser doesn't support passkeys. You can continue using password authentication.</p>
      <button on:click={handleSkip} class="primary-button">
        Continue to Dashboard
      </button>
    </div>
  {:else if !isAvailable}
    <div class="warning-banner">
      <h3>⚠️ Platform Authenticator Not Available</h3>
      <p>Your device doesn't support biometric authentication. You can continue using password authentication.</p>
      <button on:click={handleSkip} class="primary-button">
        Continue to Dashboard
      </button>
    </div>
  {:else if setupComplete}
    <div class="success-state">
      <h2>✅ Setup Complete!</h2>
      <p>Your passkey has been successfully configured. You can now sign in securely with {biometricType}.</p>
      <div class="spinner"></div>
      <p>Redirecting to dashboard...</p>
    </div>
  {:else}
    <div class="setup-container">
      <div class="setup-header">
        <h2>🔐 Welcome to Secure Authentication</h2>
        <p class="subtitle">Set up your passkey for secure, passwordless sign-in</p>
      </div>

      <div class="setup-progress">
        <div class="progress-bar">
          <div class="progress-fill" style="width: {(currentStep / 3) * 100}%"></div>
        </div>
        <p class="progress-text">Step {currentStep} of 3</p>
      </div>

      {#if currentStep === 1}
        <div class="setup-step">
          <h3>📱 What are Passkeys?</h3>
          <div class="info-grid">
            <div class="info-card">
              <h4>🔐 Secure</h4>
              <p>Biometric authentication using {biometricType} on your device</p>
            </div>
            <div class="info-card">
              <h4>⚡ Fast</h4>
              <p>Sign in with a single touch or glance</p>
            </div>
            <div class="info-card">
              <h4>🛡️ Safe</h4>
              <p>No passwords to remember or risk of theft</p>
            </div>
            <div class="info-card">
              <h4>🌐 Universal</h4>
              <p>Works across all your devices automatically</p>
            </div>
          </div>
          
          <div class="step-actions">
            <button on:click={handleSkip} class="secondary-button">
              Skip for Now
            </button>
            <button on:click={nextStep} class="primary-button">
              Get Started
            </button>
          </div>
        </div>
      {:else if currentStep === 2}
        <div class="setup-step">
          <h3>🔑 Manager Key Verification</h3>
          <p>To link your account to your fantasy league manager, please verify your manager key:</p>
          
          <div class="manager-info">
            <p><strong>Username:</strong> {username}</p>
            {#if managerKey}
              <p><strong>Manager Key:</strong> {managerKey}</p>
            {:else}
              <p><strong>Manager Key:</strong> <em>Not set</em></p>
            {/if}
          </div>

          <div class="verification-note">
            <p>✅ Your manager key will be preserved during setup</p>
            <p>✅ You'll be able to access all your league data</p>
            <p>✅ Your account will be linked to your fantasy league profile</p>
          </div>

          <div class="step-actions">
            <button on:click={previousStep} class="secondary-button">
              Back
            </button>
            <button on:click={nextStep} class="primary-button">
              Continue
            </button>
          </div>
        </div>
      {:else if currentStep === 3}
        <div class="setup-step">
          <h3>🔐 Set Up Your Passkey</h3>
          <p>Create your passkey to enable secure, passwordless sign-in:</p>
          
          <PasskeyRegistration 
            {userId} 
            {username} 
            {managerKey}
            on:registrationSuccess={handleRegistrationSuccess}
          />
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .first-time-setup {
    max-width: 800px;
    margin: 0 auto;
    padding: 2rem;
  }

  .loading-state, .success-state {
    text-align: center;
    padding: 3rem;
  }

  .spinner {
    width: 48px;
    height: 48px;
    border: 4px solid #f3f3f3;
    border-top: 4px solid #007bff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin: 0 auto 1rem;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .error-banner, .warning-banner {
    padding: 2rem;
    border-radius: 12px;
    text-align: center;
    margin-bottom: 2rem;
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

  .setup-container {
    background: white;
    border-radius: 12px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    overflow: hidden;
  }

  .setup-header {
    background: linear-gradient(135deg, #007bff, #0056b3);
    color: white;
    padding: 2rem;
    text-align: center;
  }

  .setup-header h2 {
    margin: 0 0 0.5rem 0;
    font-size: 1.75rem;
  }

  .subtitle {
    margin: 0;
    opacity: 0.9;
    font-size: 1.1rem;
  }

  .setup-progress {
    padding: 1rem 2rem;
    background: #f8f9fa;
    border-bottom: 1px solid #e1e5e9;
  }

  .progress-bar {
    width: 100%;
    height: 8px;
    background: #e1e5e9;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 0.5rem;
  }

  .progress-fill {
    height: 100%;
    background: linear-gradient(90deg, #007bff, #0056b3);
    transition: width 0.3s ease;
  }

  .progress-text {
    margin: 0;
    text-align: center;
    color: #666;
    font-size: 0.875rem;
  }

  .setup-step {
    padding: 2rem;
  }

  .setup-step h3 {
    margin: 0 0 1rem 0;
    color: #333;
    font-size: 1.5rem;
  }

  .info-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin: 2rem 0;
  }

  .info-card {
    background: #f8f9fa;
    padding: 1.5rem;
    border-radius: 8px;
    text-align: center;
    border: 1px solid #e1e5e9;
  }

  .info-card h4 {
    margin: 0 0 0.5rem 0;
    color: #333;
    font-size: 1.1rem;
  }

  .info-card p {
    margin: 0;
    color: #666;
    font-size: 0.875rem;
  }

  .manager-info {
    background: #f8f9fa;
    padding: 1rem;
    border-radius: 6px;
    margin: 1rem 0;
  }

  .manager-info p {
    margin: 0.25rem 0;
  }

  .verification-note {
    background: #d4edda;
    color: #155724;
    padding: 1rem;
    border-radius: 6px;
    margin: 1rem 0;
  }

  .verification-note p {
    margin: 0.25rem 0;
    font-size: 0.875rem;
  }

  .step-actions {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 2rem;
    padding-top: 1rem;
    border-top: 1px solid #e1e5e9;
  }

  .primary-button {
    padding: 0.75rem 1.5rem;
    background: #007bff;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .primary-button:hover {
    background: #0056b3;
  }

  .secondary-button {
    padding: 0.75rem 1.5rem;
    background: transparent;
    color: #6c757d;
    border: 1px solid #6c757d;
    border-radius: 6px;
    font-size: 1rem;
    cursor: pointer;
    transition: all 0.2s;
  }

  .secondary-button:hover {
    background: #6c757d;
    color: white;
  }
</style> 