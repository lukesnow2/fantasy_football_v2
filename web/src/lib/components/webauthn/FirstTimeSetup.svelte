<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser } from './browser';
  import PasskeyRegistration from './PasskeyRegistration.svelte';
  import { goto } from '$app/navigation';
  import { Fingerprint, Shield, AlertTriangle, CheckCircle, Loader2, ArrowRight } from 'lucide-svelte';

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
    // Get redirect URL from URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    const redirectUrl = urlParams.get('redirect') || '/dashboard';
    
    // Create session for the user
    fetch('/api/webauthn/setup-complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId })
    }).then(response => {
      if (response.ok) {
        setTimeout(() => goto(redirectUrl), 2000);
      } else {
        console.error('Failed to create session');
        setTimeout(() => goto(redirectUrl), 2000);
      }
    }).catch(error => {
      console.error('Session creation error:', error);
      setTimeout(() => goto(redirectUrl), 2000);
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
      <Loader2 class="h-8 w-8 animate-spin text-blue-400" />
      <p>Loading setup...</p>
    </div>
  {:else if !isSupported}
    <div class="error-banner">
      <AlertTriangle class="h-5 w-5" />
      <div>
        <h3>WebAuthn Not Supported</h3>
        <p>Your browser doesn't support passkeys. You can continue using password authentication.</p>
      </div>
      <button on:click={handleSkip} class="primary-button">
        <ArrowRight class="h-4 w-4" />
        Continue to Dashboard
      </button>
    </div>
  {:else if !isAvailable}
    <div class="warning-banner">
      <AlertTriangle class="h-5 w-5" />
      <div>
        <h3>Platform Authenticator Not Available</h3>
        <p>Your device doesn't support biometric authentication. You can continue using password authentication.</p>
      </div>
      <button on:click={handleSkip} class="primary-button">
        <ArrowRight class="h-4 w-4" />
        Continue to Dashboard
      </button>
    </div>
  {:else if setupComplete}
    <div class="success-state">
      <CheckCircle class="h-12 w-12 text-green-400" />
      <h2>Setup Complete!</h2>
      <p>Your passkey has been successfully configured. You can now sign in securely with {biometricType}.</p>
      <Loader2 class="h-6 w-6 animate-spin text-blue-400" />
      <p>Redirecting to dashboard...</p>
    </div>
  {:else}
    <div class="setup-container">
      <div class="setup-header">
        <Fingerprint class="h-8 w-8 text-blue-400" />
        <h1>Set Up Your Passkey</h1>
        <p>Secure your account with biometric authentication</p>
      </div>

      <div class="step-indicator">
        <div class="step {currentStep >= 1 ? 'active' : ''}">
          <span class="step-number">1</span>
          <span class="step-label">Welcome</span>
        </div>
        <div class="step-connector"></div>
        <div class="step {currentStep >= 2 ? 'active' : ''}">
          <span class="step-number">2</span>
          <span class="step-label">Create Passkey</span>
        </div>
        <div class="step-connector"></div>
        <div class="step {currentStep >= 3 ? 'active' : ''}">
          <span class="step-number">3</span>
          <span class="step-label">Complete</span>
        </div>
      </div>

      {#if currentStep === 1}
        <div class="step-content">
          <div class="welcome-section">
            <Shield class="h-12 w-12 text-blue-400" />
            <h2>Welcome to Secure Authentication</h2>
            <p>We're setting up a passkey for your account. This will allow you to sign in securely using {biometricType}.</p>
            
            <div class="benefits-list">
              <h3>Benefits of using a passkey:</h3>
              <ul>
                <li>🔐 More secure than passwords</li>
                <li>⚡ Faster sign-in experience</li>
                <li>📱 Works across your devices</li>
                <li>🛡️ Protection against phishing</li>
              </ul>
            </div>

            <div class="info-box">
              <h4>How it works:</h4>
              <ol>
                <li>We'll create a unique passkey for your account</li>
                <li>Your device will store it securely</li>
                <li>You'll use {biometricType} to sign in</li>
                <li>No more remembering passwords!</li>
              </ol>
            </div>

            <div class="button-group">
              <button on:click={nextStep} class="primary-button">
                <ArrowRight class="h-4 w-4" />
                Get Started
              </button>
              <button on:click={handleSkip} class="secondary-button">
                Skip for Now
              </button>
            </div>
          </div>
        </div>
      {:else if currentStep === 2}
        <div class="step-content">
          <PasskeyRegistration 
            {userId} 
            {username} 
            {managerKey}
            on:registrationSuccess={handleRegistrationSuccess}
          />
        </div>
      {:else if currentStep === 3}
        <div class="step-content">
          <div class="completion-section">
            <CheckCircle class="h-12 w-12 text-green-400" />
            <h2>Almost Done!</h2>
            <p>Your passkey has been created successfully. You'll be redirected to your dashboard in a moment.</p>
          </div>
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .first-time-setup {
    width: 100%;
    max-width: 600px;
    margin: 0 auto;
  }

  .loading-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1rem;
    padding: 2rem;
    text-align: center;
    color: #94a3b8;
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

  .setup-container {
    width: 100%;
  }

  .setup-header {
    text-align: center;
    margin-bottom: 2rem;
  }

  .setup-header h1 {
    margin: 0.5rem 0 0.25rem 0;
    color: white;
    font-size: 1.875rem;
    font-weight: 700;
  }

  .setup-header p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .step-indicator {
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 2rem;
    gap: 0.5rem;
  }

  .step {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.25rem;
    opacity: 0.5;
    transition: opacity 0.2s;
  }

  .step.active {
    opacity: 1;
  }

  .step-number {
    width: 2rem;
    height: 2rem;
    border-radius: 50%;
    background: #475569;
    color: white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .step.active .step-number {
    background: #3b82f6;
  }

  .step-label {
    font-size: 0.75rem;
    color: #94a3b8;
    font-weight: 500;
  }

  .step.active .step-label {
    color: white;
  }

  .step-connector {
    width: 2rem;
    height: 2px;
    background: #475569;
  }

  .step-content {
    width: 100%;
  }

  .welcome-section {
    text-align: center;
  }

  .welcome-section h2 {
    margin: 1rem 0 0.5rem 0;
    color: white;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .welcome-section p {
    margin: 0 0 2rem 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .benefits-list {
    background: rgba(51, 65, 85, 0.3);
    padding: 1.5rem;
    border-radius: 8px;
    margin: 1.5rem 0;
    border: 1px solid rgba(148, 163, 184, 0.1);
    text-align: left;
  }

  .benefits-list h3 {
    margin: 0 0 1rem 0;
    color: white;
    font-size: 1rem;
    font-weight: 600;
  }

  .benefits-list ul {
    margin: 0;
    padding-left: 1.25rem;
    color: #cbd5e1;
    font-size: 0.875rem;
  }

  .benefits-list li {
    margin-bottom: 0.5rem;
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
    text-align: left;
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

  .button-group {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    margin-top: 2rem;
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

  .primary-button:hover {
    background: #2563eb;
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

  .completion-section {
    text-align: center;
    padding: 2rem;
  }

  .completion-section h2 {
    margin: 1rem 0 0.5rem 0;
    color: white;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .completion-section p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }
</style> 