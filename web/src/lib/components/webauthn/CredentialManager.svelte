<script lang="ts">
  import { onMount } from 'svelte';
  import { WebAuthnBrowser } from './browser';

  export let userId: string;

  interface Credential {
    id: string;
    credentialId: string;
    deviceType?: string;
    authenticatorType?: string;
    createdAt: string;
    lastUsedAt?: string;
    transports?: string[];
  }

  let credentials: Credential[] = [];
  let isLoading = true;
  let error = '';
  let deletingId = '';

  onMount(async () => {
    await loadCredentials();
  });

  async function loadCredentials() {
    try {
      const response = await fetch(`/api/webauthn/credentials?userId=${userId}`);
      if (!response.ok) {
        throw new Error('Failed to load credentials');
      }
      credentials = await response.json();
    } catch (err) {
      error = err instanceof Error ? err.message : 'Failed to load credentials';
    } finally {
      isLoading = false;
    }
  }

  async function deleteCredential(credentialId: string) {
    if (!confirm('Are you sure you want to delete this passkey? You won\'t be able to sign in with it anymore.')) {
      return;
    }

    deletingId = credentialId;
    try {
      const response = await fetch('/api/webauthn/credentials/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ credentialId, userId })
      });

      if (!response.ok) {
        throw new Error('Failed to delete credential');
      }

      // Remove from local list
      credentials = credentials.filter(c => c.credentialId !== credentialId);
    } catch (err) {
      error = err instanceof Error ? err.message : 'Failed to delete credential';
    } finally {
      deletingId = '';
    }
  }

  function formatDate(dateString: string): string {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  }

  function getDeviceIcon(deviceType?: string): string {
    switch (deviceType?.toLowerCase()) {
      case 'ios': return '📱';
      case 'android': return '📱';
      case 'windows': return '💻';
      case 'macos': return '💻';
      case 'linux': return '💻';
      default: return '🔐';
    }
  }

  function getTransportIcons(transports?: string[]): string {
    if (!transports || transports.length === 0) return '🔐';
    
    const icons = transports.map(transport => {
      switch (transport) {
        case 'internal': return '📱';
        case 'usb': return '🔌';
        case 'nfc': return '📡';
        case 'ble': return '📶';
        default: return '🔐';
      }
    });
    
    return icons.join(' ');
  }
</script>

<div class="credential-manager">
  <h2>🔐 Manage Your Passkeys</h2>
  <p class="subtitle">View and manage your registered passkeys</p>

  {#if error}
    <div class="error-message">
      <p>{error}</p>
      <button on:click={loadCredentials} class="retry-button">
        Try Again
      </button>
    </div>
  {/if}

  {#if isLoading}
    <div class="loading-state">
      <div class="spinner"></div>
      <p>Loading your passkeys...</p>
    </div>
  {:else if credentials.length === 0}
    <div class="empty-state">
      <h3>📱 No Passkeys Found</h3>
      <p>You haven't registered any passkeys yet. Set up your first passkey to get started.</p>
    </div>
  {:else}
    <div class="credentials-list">
      {#each credentials as credential (credential.id)}
        <div class="credential-card">
          <div class="credential-info">
            <div class="credential-icon">
              {getDeviceIcon(credential.deviceType)}
            </div>
            <div class="credential-details">
              <h4>{credential.deviceType || 'Unknown Device'}</h4>
              <p class="credential-id">
                ID: {credential.credentialId.slice(0, 8)}...
              </p>
              <p class="credential-meta">
                Created: {formatDate(credential.createdAt)}
                {#if credential.lastUsedAt}
                  • Last used: {formatDate(credential.lastUsedAt)}
                {/if}
              </p>
              <p class="credential-transports">
                {getTransportIcons(credential.transports)}
              </p>
            </div>
          </div>
          
          <div class="credential-actions">
            <button
              on:click={() => deleteCredential(credential.credentialId)}
              disabled={deletingId === credential.credentialId}
              class="delete-button"
              title="Delete this passkey"
            >
              {#if deletingId === credential.credentialId}
                <span class="loading">Deleting...</span>
              {:else}
                🗑️
              {/if}
            </button>
          </div>
        </div>
      {/each}
    </div>

    <div class="credential-stats">
      <p>Total passkeys: {credentials.length}</p>
    </div>
  {/if}
</div>

<style>
  .credential-manager {
    max-width: 600px;
    margin: 0 auto;
    padding: 2rem;
  }

  h2 {
    margin: 0 0 0.5rem 0;
    color: #333;
  }

  .subtitle {
    color: #666;
    margin-bottom: 2rem;
  }

  .error-message {
    background: #fee;
    color: #c33;
    padding: 1rem;
    border-radius: 6px;
    margin-bottom: 1rem;
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
  }

  .loading-state {
    text-align: center;
    padding: 2rem;
  }

  .spinner {
    width: 32px;
    height: 32px;
    border: 3px solid #f3f3f3;
    border-top: 3px solid #007bff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin: 0 auto 1rem;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .empty-state {
    text-align: center;
    padding: 2rem;
    background: #f8f9fa;
    border-radius: 8px;
  }

  .empty-state h3 {
    margin: 0 0 0.5rem 0;
    color: #666;
  }

  .empty-state p {
    color: #666;
    margin: 0;
  }

  .credentials-list {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .credential-card {
    background: white;
    border: 1px solid #e1e5e9;
    border-radius: 8px;
    padding: 1rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: box-shadow 0.2s;
  }

  .credential-card:hover {
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .credential-info {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex: 1;
  }

  .credential-icon {
    font-size: 2rem;
    width: 48px;
    text-align: center;
  }

  .credential-details h4 {
    margin: 0 0 0.25rem 0;
    color: #333;
    font-size: 1rem;
  }

  .credential-id {
    margin: 0 0 0.25rem 0;
    color: #666;
    font-size: 0.875rem;
    font-family: monospace;
  }

  .credential-meta {
    margin: 0 0 0.25rem 0;
    color: #666;
    font-size: 0.75rem;
  }

  .credential-transports {
    margin: 0;
    color: #666;
    font-size: 0.875rem;
  }

  .credential-actions {
    display: flex;
    gap: 0.5rem;
  }

  .delete-button {
    padding: 0.5rem;
    background: transparent;
    color: #dc3545;
    border: 1px solid #dc3545;
    border-radius: 4px;
    cursor: pointer;
    transition: all 0.2s;
    font-size: 1rem;
  }

  .delete-button:hover:not(:disabled) {
    background: #dc3545;
    color: white;
  }

  .delete-button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .credential-stats {
    margin-top: 1rem;
    padding-top: 1rem;
    border-top: 1px solid #e1e5e9;
    text-align: center;
    color: #666;
    font-size: 0.875rem;
  }
</style> 