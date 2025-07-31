<script lang="ts">
  import { onMount } from 'svelte';
  import { createEventDispatcher } from 'svelte';
  import { WebAuthnBrowser } from './browser';
  import PasskeyRegistration from './PasskeyRegistration.svelte';
  import { Fingerprint, Shield, AlertTriangle, Trash2, RotateCcw, Plus, Smartphone, Monitor, Laptop, Tablet } from 'lucide-svelte';

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

  function getDeviceIcon(deviceType: string) {
    switch (deviceType?.toLowerCase()) {
      case 'phone': return Smartphone;
      case 'laptop': return Laptop;
      case 'desktop': return Monitor;
      case 'tablet': return Tablet;
      default: return Smartphone;
    }
  }

  function getAuthenticatorIcon(authenticatorType: string) {
    switch (authenticatorType?.toLowerCase()) {
      case 'fingerprint': return Fingerprint;
      case 'face': return Shield;
      default: return Fingerprint;
    }
  }
</script>

<div class="credential-manager">
  {#if showRotationForm}
    <div class="rotation-form">
      <PasskeyRegistration 
        {userId} 
        username=""
        managerKey=""
        isRotationMode={true}
        on:registrationSuccess={handleRotationSuccess}
      />
      <button on:click={cancelRotation} class="secondary-button">
        Cancel Rotation
      </button>
    </div>
  {:else}
    <div class="manager-container">
      <div class="manager-header">
        <Fingerprint class="h-8 w-8 text-blue-400" />
        <h2>Passkey Manager</h2>
        <p>Manage your biometric authentication devices</p>
      </div>

      {#if error}
        <div class="error-message">
          <AlertTriangle class="h-4 w-4" />
          <div>
            <p>{error}</p>
          </div>
        </div>
      {/if}

      {#if isLoading}
        <div class="loading-state">
          <div class="spinner"></div>
          <p>Loading credentials...</p>
        </div>
      {:else if credentials.length === 0}
        <div class="empty-state">
          <Shield class="h-12 w-12 text-blue-400" />
          <h3>No Passkeys Found</h3>
          <p>You haven't set up any passkeys yet. Create one to enable secure biometric authentication.</p>
        </div>
      {:else}
        <div class="credentials-list">
          {#each credentials as credential}
            <div class="credential-card">
              <div class="credential-header">
                <div class="credential-icon">
                  {#if getDeviceIcon(credential.deviceType)}
                    <svelte:component this={getDeviceIcon(credential.deviceType)} class="h-5 w-5 text-blue-400" />
                  {/if}
                </div>
                <div class="credential-info">
                  <h4>{credential.deviceType} Passkey</h4>
                  <p class="credential-details">
                    {credential.authenticatorType} • Created {formatDate(credential.createdAt)}
                  </p>
                  {#if credential.lastUsedAt}
                    <p class="last-used">Last used: {formatDate(credential.lastUsedAt)}</p>
                  {/if}
                </div>
                <div class="credential-actions">
                  <button 
                    on:click={() => deleteCredential(credential.credentialId)}
                    class="delete-button"
                    title="Delete this passkey"
                  >
                    <Trash2 class="h-4 w-4" />
                  </button>
                </div>
              </div>
            </div>
          {/each}
        </div>

        <div class="manager-actions">
          <button on:click={startRotation} class="primary-button">
            <RotateCcw class="h-4 w-4" />
            Rotate Passkey
          </button>
          <button on:click={() => dispatch('addNew')} class="secondary-button">
            <Plus class="h-4 w-4" />
            Add New Passkey
          </button>
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .credential-manager {
    width: 100%;
  }

  .rotation-form {
    width: 100%;
  }

  .manager-container {
    width: 100%;
  }

  .manager-header {
    text-align: center;
    margin-bottom: 2rem;
  }

  .manager-header h2 {
    margin: 0.5rem 0 0.25rem 0;
    color: white;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .manager-header p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
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
    margin: 0;
    font-size: 0.875rem;
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

  .spinner {
    width: 2rem;
    height: 2rem;
    border: 2px solid transparent;
    border-top: 2px solid #3b82f6;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1rem;
    padding: 2rem;
    text-align: center;
    color: white;
  }

  .empty-state h3 {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 600;
  }

  .empty-state p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .credentials-list {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    margin-bottom: 2rem;
  }

  .credential-card {
    background: rgba(51, 65, 85, 0.3);
    border: 1px solid rgba(148, 163, 184, 0.1);
    border-radius: 8px;
    padding: 1rem;
    transition: all 0.2s;
  }

  .credential-card:hover {
    background: rgba(51, 65, 85, 0.5);
    border-color: rgba(148, 163, 184, 0.2);
  }

  .credential-header {
    display: flex;
    align-items: center;
    gap: 1rem;
  }

  .credential-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 2.5rem;
    height: 2.5rem;
    background: rgba(59, 130, 246, 0.1);
    border-radius: 6px;
  }

  .credential-info {
    flex: 1;
  }

  .credential-info h4 {
    margin: 0 0 0.25rem 0;
    color: white;
    font-size: 1rem;
    font-weight: 600;
  }

  .credential-details {
    margin: 0;
    color: #cbd5e1;
    font-size: 0.875rem;
  }

  .last-used {
    margin: 0.25rem 0 0 0;
    color: #94a3b8;
    font-size: 0.75rem;
  }

  .credential-actions {
    display: flex;
    gap: 0.5rem;
  }

  .delete-button {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 2rem;
    height: 2rem;
    background: rgba(239, 68, 68, 0.1);
    color: #fca5a5;
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 6px;
    cursor: pointer;
    transition: all 0.2s;
  }

  .delete-button:hover {
    background: rgba(239, 68, 68, 0.2);
    color: #f87171;
  }

  .manager-actions {
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

  .primary-button:hover {
    background: #2563eb;
  }

  .secondary-button {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
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
</style> 