<script lang="ts">
  import { onMount } from 'svelte';

  export let userId: string;

  let codes: string[] = [];
  let isLoading = false;
  let error = '';
  let showCodes = false;
  let status = {
    totalCodes: 0,
    usedCodes: 0,
    availableCodes: 0,
    hasCodes: false
  };

  onMount(async () => {
    await loadStatus();
  });

  async function loadStatus() {
    try {
      const response = await fetch(`/api/webauthn/backup-codes?userId=${userId}`);
      if (!response.ok) {
        throw new Error('Failed to load backup codes status');
      }
      const data = await response.json();
      if (data.success) {
        status = data.status;
      }
    } catch (err) {
      console.error('Failed to load backup codes status:', err);
    }
  }

  async function generateCodes() {
    isLoading = true;
    error = '';

    try {
      const response = await fetch('/api/webauthn/backup-codes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId })
      });

      if (!response.ok) {
        throw new Error('Failed to generate backup codes');
      }

      const data = await response.json();
      if (data.success) {
        codes = data.codes;
        showCodes = true;
        await loadStatus();
      } else {
        error = data.message || 'Failed to generate backup codes';
      }
    } catch (err) {
      error = err instanceof Error ? err.message : 'Failed to generate backup codes';
    } finally {
      isLoading = false;
    }
  }

  function hideCodes() {
    showCodes = false;
    codes = [];
  }

  function copyToClipboard(text: string) {
    navigator.clipboard.writeText(text).then(() => {
      // Show a brief success message
      const button = event?.target as HTMLButtonElement;
      const originalText = button.textContent;
      button.textContent = 'Copied!';
      setTimeout(() => {
        button.textContent = originalText;
      }, 2000);
    });
  }

  function downloadCodes() {
    const content = codes.map((code, index) => `${index + 1}. ${code}`).join('\n');
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'backup-codes.txt';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }
</script>

<div class="backup-codes">
  <h3>🔑 Backup Codes</h3>
  <p class="subtitle">Generate backup codes for emergency access to your account</p>

  {#if error}
    <div class="error-message">
      <p>{error}</p>
    </div>
  {/if}

  <div class="status-info">
    <p><strong>Available codes:</strong> {status.availableCodes}</p>
    <p><strong>Used codes:</strong> {status.usedCodes}</p>
    <p><strong>Total codes:</strong> {status.totalCodes}</p>
  </div>

  {#if !status.hasCodes}
    <div class="generate-section">
      <h4>📝 Generate Backup Codes</h4>
      <p>Backup codes allow you to access your account if you lose access to your passkey. Each code can only be used once.</p>
      
      <button 
        on:click={generateCodes} 
        disabled={isLoading}
        class="primary-button"
      >
        {#if isLoading}
          <span class="loading">Generating...</span>
        {:else}
          Generate Backup Codes
        {/if}
      </button>
    </div>
  {:else if showCodes}
    <div class="codes-section">
      <h4>🔐 Your Backup Codes</h4>
      <p class="warning">⚠️ Save these codes in a secure location. Each code can only be used once.</p>
      
      <div class="codes-grid">
        {#each codes as code, index}
          <div class="code-item">
            <span class="code-number">{index + 1}.</span>
            <span class="code-text">{code}</span>
            <button 
              on:click={() => copyToClipboard(code)}
              class="copy-button"
              title="Copy code"
            >
              📋
            </button>
          </div>
        {/each}
      </div>

      <div class="code-actions">
        <button on:click={downloadCodes} class="secondary-button">
          📥 Download Codes
        </button>
        <button on:click={hideCodes} class="secondary-button">
          Hide Codes
        </button>
      </div>
    </div>
  {:else}
    <div class="existing-codes">
      <h4>✅ Backup Codes Available</h4>
      <p>You have {status.availableCodes} backup codes remaining.</p>
      
      <div class="code-actions">
        <button on:click={generateCodes} class="secondary-button">
          🔄 Regenerate Codes
        </button>
      </div>
    </div>
  {/if}
</div>

<style>
  .backup-codes {
    max-width: 600px;
    margin: 0 auto;
    padding: 2rem;
  }

  h3 {
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

  .status-info {
    background: #f8f9fa;
    padding: 1rem;
    border-radius: 6px;
    margin-bottom: 1.5rem;
  }

  .status-info p {
    margin: 0.25rem 0;
    font-size: 0.875rem;
  }

  .generate-section, .codes-section, .existing-codes {
    background: white;
    padding: 1.5rem;
    border-radius: 8px;
    border: 1px solid #e1e5e9;
    margin-bottom: 1rem;
  }

  .generate-section h4, .codes-section h4, .existing-codes h4 {
    margin: 0 0 0.5rem 0;
    color: #333;
  }

  .warning {
    background: #fff3cd;
    color: #856404;
    padding: 0.75rem;
    border-radius: 4px;
    margin: 1rem 0;
    font-size: 0.875rem;
  }

  .codes-grid {
    display: grid;
    gap: 0.5rem;
    margin: 1rem 0;
  }

  .code-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem;
    background: #f8f9fa;
    border-radius: 4px;
    font-family: monospace;
    font-size: 0.875rem;
  }

  .code-number {
    color: #666;
    min-width: 2rem;
  }

  .code-text {
    flex: 1;
    font-weight: 600;
    color: #333;
  }

  .copy-button {
    background: transparent;
    border: none;
    cursor: pointer;
    padding: 0.25rem;
    border-radius: 4px;
    transition: background-color 0.2s;
  }

  .copy-button:hover {
    background: #e1e5e9;
  }

  .code-actions {
    display: flex;
    gap: 1rem;
    margin-top: 1rem;
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

  .loading {
    display: flex;
    align-items: center;
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