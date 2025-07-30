<script lang="ts">
  import { onMount } from 'svelte';
  import { createEventDispatcher } from 'svelte';
  import { WebAuthnBrowser } from './browser';
  import PasskeyRegistration from './PasskeyRegistration.svelte';

  export let userId: string;

  const dispatch = createEventDispatcher();

  let credentials: Array<{
    id: string;
    credentialId: string;
    deviceType: string;
    authenticatorType: string;
    createdAt: string;
    lastUsedAt: string | null;
  }> = [];
  let isLoading = true;
  let error = '';
  let showRotationForm = false;
  let isSupported = false;
  let isAvailable = false;

  onMount(async () => {
    isSupported = WebAuthnBrowser.isSupported();
    isAvailable = await WebAuthnBrowser.isPlatformAuthenticatorAvailable();
    await loadCredentials();
  });

  async function loadCredentials() {
    try {
      const response = await fetch(`/api/webauthn/credentials?userId=${userId}`);
      if (!response.ok) {
        throw new Error('Failed to load credentials');
      }
      const data = await response.json();
      if (data.success) {
        credentials = data.credentials;
      }
    } catch (err) {
      error = err instanceof Error ? err.message : 'Failed to load credentials';
    } finally {
      isLoading = false;
    }
  }

  async function deleteCredential(credentialId: string) {
    if (!confirm('Are you sure you want to delete this passkey? You will need to create a new one to sign in.')) {
      return;
    }

    try {
      const response = await fetch('/api/webauthn/credentials/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, credentialId })
      });

      if (!response.ok) {
        throw new Error('Failed to delete credential');
      }

      await loadCredentials();
      dispatch('credentialDeleted', { credentialId });
    } catch (err) {
      error = err instanceof Error ? err.message : 'Failed to delete credential';
    }
  }

  function startRotation() {
    showRotationForm = true;
  }

  function cancelRotation() {
    showRotationForm = false;
  }

  function handleRotationSuccess() {
    showRotationForm = false;
    loadCredentials();
    dispatch('credentialRotated');
  }

  function formatDate(dateString: string) {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  }

  function getDeviceIcon(deviceType: string): string {
    switch (deviceType?.toLowerCase()) {
      case 'phone': return '📱';
      case 'laptop': return '💻';
      case 'desktop': return '🖥️';
      case 'tablet': return '📱';
      default: return '🔐';
    }
  }
</script>

<div class="credential-manager">
  <div class="header">
    <h3>🔐 Passkey Management</h3>
    <p class="subtitle">Manage your registered passkeys</p>
  </div>

  {#if error}
    <div class="error-message">
      <p>{error}</p>
    </div>
  {/if}

  {#if isLoading}
    <div class="loading-state">
      <div class="spinner"></div>
      <p>Loading credentials...</p>
    </div>
  {:else if credentials.length === 0}
    <div class="empty-state">
      <h4>No Passkeys Found</h4>
      <p>You haven't registered any passkeys yet.</p>
    </div>
  {:else}
    <div class="credentials-list">
      {#each credentials as credential}
        <div class="credential-item">
          <div class="credential-info">
            <div class="device-icon">
              {getDeviceIcon(credential.deviceType)}
            </div>
            <div class="credential-details">
              <h4>{credential.deviceType || 'Unknown Device'}</h4>
              <p class="credential-id">
                ID: {credential.credentialId.substring(0, 16)}...
              </p>
              <p class="credential-dates">
                Created: {formatDate(credential.createdAt)}
                {#if credential.lastUsedAt}
                  • Last used: {formatDate(credential.lastUsedAt)}
                {/if}
              </p>
            </div>
          </div>
          <div class="credential-actions">
            <button
              on:click={() => deleteCredential(credential.credentialId)}
              class="delete-button"
              title="Delete passkey"
            >
              🗑️
            </button>
          </div>
        </div>
      {/each}
    </div>

    <div class="rotation-section">
      <h4>🔄 Replace Passkey</h4>
      <p>Create a new passkey to replace your existing ones. This is useful when you get a new device or want to update your security.</p>
      
      {#if !isSupported}
        <div class="error-banner">
          <h5>⚠️ WebAuthn Not Supported</h5>
          <p>Your browser doesn't support passkeys.</p>
        </div>
      {:else if !isAvailable}
        <div class="warning-banner">
          <h5>⚠️ Platform Authenticator Not Available</h5>
          <p>Your device doesn't support biometric authentication.</p>
        </div>
      {:else}
        <button
          on:click={startRotation}
          class="rotation-button"
          disabled={showRotationForm}
        >
          🔄 Replace Passkey
        </button>
      {/if}
    </div>
  {/if}

  {#if showRotationForm}
    <div class="rotation-modal">
      <div class="modal-content">
        <div class="modal-header">
          <h4>🔄 Replace Passkey</h4>
          <button on:click={cancelRotation} class="close-button">×</button>
        </div>
        
        <div class="modal-body">
          <p>Creating a new passkey will replace your existing ones. Make sure you can complete the setup process.</p>
          
          <PasskeyRegistration 
            {userId}
            username=""
            managerKey=""
            isRotationMode={true}
            on:registrationSuccess={handleRotationSuccess}
          />
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .credential-manager {
    max-width: 800px;
    margin: 0 auto;
    padding: 2rem;
  }

  .header {
    text-align: center;
    margin-bottom: 2rem;
  }

  .header h3 {
    margin: 0 0 0.5rem 0;
    color: #333;
    font-size: 1.5rem;
  }

  .subtitle {
    margin: 0;
    color: #666;
    font-size: 1rem;
  }

  .loading-state, .empty-state {
    text-align: center;
    padding: 3rem;
    color: #666;
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

  .error-message {
    background: #fee;
    border: 1px solid #fcc;
    color: #c33;
    padding: 1rem;
    border-radius: 6px;
    margin-bottom: 1rem;
  }

  .credentials-list {
    margin-bottom: 2rem;
  }

  .credential-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem;
    border: 1px solid #e1e5e9;
    border-radius: 8px;
    margin-bottom: 1rem;
    background: white;
  }

  .credential-info {
    display: flex;
    align-items: center;
    gap: 1rem;
  }

  .device-icon {
    font-size: 2rem;
  }

  .credential-details h4 {
    margin: 0 0 0.25rem 0;
    color: #333;
    font-size: 1.1rem;
  }

  .credential-id {
    margin: 0 0 0.25rem 0;
    color: #666;
    font-size: 0.875rem;
    font-family: monospace;
  }

  .credential-dates {
    margin: 0;
    color: #888;
    font-size: 0.75rem;
  }

  .credential-actions {
    display: flex;
    gap: 0.5rem;
  }

  .delete-button {
    background: none;
    border: none;
    font-size: 1.25rem;
    cursor: pointer;
    padding: 0.5rem;
    border-radius: 4px;
    transition: background-color 0.2s;
  }

  .delete-button:hover {
    background: #fee;
  }

  .rotation-section {
    background: #f8f9fa;
    padding: 1.5rem;
    border-radius: 8px;
    border: 1px solid #e1e5e9;
  }

  .rotation-section h4 {
    margin: 0 0 0.5rem 0;
    color: #333;
  }

  .rotation-section p {
    margin: 0 0 1rem 0;
    color: #666;
    font-size: 0.875rem;
  }

  .error-banner, .warning-banner {
    padding: 1rem;
    border-radius: 6px;
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

  .error-banner h5, .warning-banner h5 {
    margin: 0 0 0.25rem 0;
    font-size: 0.875rem;
  }

  .error-banner p, .warning-banner p {
    margin: 0;
    font-size: 0.75rem;
  }

  .rotation-button {
    background: #007bff;
    color: white;
    border: none;
    padding: 0.75rem 1.5rem;
    border-radius: 6px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: background-color 0.2s;
  }

  .rotation-button:hover:not(:disabled) {
    background: #0056b3;
  }

  .rotation-button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .rotation-modal {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.5);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }

  .modal-content {
    background: white;
    border-radius: 12px;
    max-width: 600px;
    width: 90%;
    max-height: 90vh;
    overflow-y: auto;
  }

  .modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1.5rem;
    border-bottom: 1px solid #e1e5e9;
  }

  .modal-header h4 {
    margin: 0;
    color: #333;
  }

  .close-button {
    background: none;
    border: none;
    font-size: 1.5rem;
    cursor: pointer;
    padding: 0.5rem;
    border-radius: 4px;
    transition: background-color 0.2s;
  }

  .close-button:hover {
    background: #f8f9fa;
  }

  .modal-body {
    padding: 1.5rem;
  }

  .modal-body p {
    margin: 0 0 1rem 0;
    color: #666;
  }
</style> 