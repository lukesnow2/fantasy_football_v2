<script lang="ts">
  import { onMount } from 'svelte';
  import { Key, Download, Copy, Eye, EyeOff, AlertTriangle, CheckCircle } from 'lucide-svelte';

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
  <div class="codes-header">
    <Key class="h-8 w-8 text-blue-400" />
    <h2>Backup Codes</h2>
    <p>Generate backup codes for emergency access to your account</p>
  </div>

  {#if error}
    <div class="error-message">
      <AlertTriangle class="h-4 w-4" />
      <div>
        <p>{error}</p>
      </div>
    </div>
  {/if}

  <div class="status-section">
    <div class="status-card">
      <div class="status-item">
        <span class="status-label">Total Codes:</span>
        <span class="status-value">{status.totalCodes}</span>
      </div>
      <div class="status-item">
        <span class="status-label">Available:</span>
        <span class="status-value available">{status.availableCodes}</span>
      </div>
      <div class="status-item">
        <span class="status-label">Used:</span>
        <span class="status-value used">{status.usedCodes}</span>
      </div>
    </div>
  </div>

  {#if status.hasCodes && !showCodes}
    <div class="existing-codes">
      <CheckCircle class="h-5 w-5 text-green-400" />
      <div>
        <h4>Backup Codes Available</h4>
        <p>You have {status.availableCodes} backup codes remaining. Generate new codes to replace the existing ones.</p>
      </div>
    </div>
  {/if}

  {#if showCodes}
    <div class="codes-display">
      <div class="codes-header-section">
        <h4>Your Backup Codes</h4>
        <p>Save these codes in a secure location. Each code can only be used once.</p>
      </div>

      <div class="codes-grid">
        {#each codes as code, index}
          <div class="code-item">
            <span class="code-number">{index + 1}</span>
            <span class="code-text">{code}</span>
            <button 
              on:click={() => copyToClipboard(code)}
              class="copy-button"
              title="Copy code"
            >
              <Copy class="h-3 w-3" />
            </button>
          </div>
        {/each}
      </div>

      <div class="codes-actions">
        <button on:click={downloadCodes} class="secondary-button">
          <Download class="h-4 w-4" />
          Download Codes
        </button>
        <button on:click={hideCodes} class="secondary-button">
          Hide Codes
        </button>
      </div>
    </div>
  {:else}
    <div class="generate-section">
      <div class="info-box">
        <AlertTriangle class="h-5 w-5 text-yellow-400" />
        <div>
          <h4>Important Security Information</h4>
          <ul>
            <li>Backup codes provide emergency access to your account</li>
            <li>Each code can only be used once</li>
            <li>Store codes securely and separately from your device</li>
            <li>Generate new codes if you suspect they've been compromised</li>
          </ul>
        </div>
      </div>

      <button
        on:click={generateCodes}
        disabled={isLoading}
        class="primary-button"
      >
        {#if isLoading}
          <span class="loading">Generating Codes...</span>
        {:else}
          <Key class="h-4 w-4" />
          <span>Generate Backup Codes</span>
        {/if}
      </button>
    </div>
  {/if}
</div>

<style>
  .backup-codes {
    width: 100%;
  }

  .codes-header {
    text-align: center;
    margin-bottom: 2rem;
  }

  .codes-header h2 {
    margin: 0.5rem 0 0.25rem 0;
    color: white;
    font-size: 1.5rem;
    font-weight: 600;
  }

  .codes-header p {
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

  .status-section {
    margin-bottom: 1.5rem;
  }

  .status-card {
    background: rgba(51, 65, 85, 0.3);
    border: 1px solid rgba(148, 163, 184, 0.1);
    border-radius: 8px;
    padding: 1rem;
    display: flex;
    justify-content: space-around;
  }

  .status-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.25rem;
  }

  .status-label {
    color: #94a3b8;
    font-size: 0.75rem;
    font-weight: 500;
  }

  .status-value {
    color: white;
    font-size: 1.25rem;
    font-weight: 600;
  }

  .status-value.available {
    color: #22c55e;
  }

  .status-value.used {
    color: #64748b;
  }

  .existing-codes {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    background: rgba(34, 197, 94, 0.1);
    border: 1px solid rgba(34, 197, 94, 0.3);
    color: #86efac;
    padding: 1rem;
    border-radius: 8px;
    margin: 1rem 0;
  }

  .existing-codes h4 {
    margin: 0 0 0.25rem 0;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .existing-codes p {
    margin: 0;
    font-size: 0.875rem;
  }

  .codes-display {
    background: rgba(51, 65, 85, 0.3);
    border: 1px solid rgba(148, 163, 184, 0.1);
    border-radius: 8px;
    padding: 1.5rem;
    margin: 1rem 0;
  }

  .codes-header-section {
    text-align: center;
    margin-bottom: 1.5rem;
  }

  .codes-header-section h4 {
    margin: 0 0 0.25rem 0;
    color: white;
    font-size: 1rem;
    font-weight: 600;
  }

  .codes-header-section p {
    margin: 0;
    color: #94a3b8;
    font-size: 0.875rem;
  }

  .codes-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.5rem;
  }

  .code-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: rgba(30, 41, 59, 0.5);
    border: 1px solid rgba(148, 163, 184, 0.2);
    border-radius: 6px;
    padding: 0.75rem;
    font-family: monospace;
  }

  .code-number {
    color: #64748b;
    font-size: 0.75rem;
    font-weight: 600;
    min-width: 1.5rem;
  }

  .code-text {
    color: #cbd5e1;
    font-size: 0.875rem;
    font-weight: 500;
    flex: 1;
  }

  .copy-button {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 1.5rem;
    height: 1.5rem;
    background: rgba(59, 130, 246, 0.1);
    color: #60a5fa;
    border: 1px solid rgba(59, 130, 246, 0.3);
    border-radius: 4px;
    cursor: pointer;
    transition: all 0.2s;
  }

  .copy-button:hover {
    background: rgba(59, 130, 246, 0.2);
    color: #93c5fd;
  }

  .codes-actions {
    display: flex;
    gap: 0.75rem;
    justify-content: center;
  }

  .generate-section {
    display: flex;
    flex-direction: column;
    gap: 1.5rem;
  }

  .info-box {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    background: rgba(245, 158, 11, 0.1);
    border: 1px solid rgba(245, 158, 11, 0.3);
    color: #fcd34d;
    padding: 1rem;
    border-radius: 8px;
  }

  .info-box h4 {
    margin: 0 0 0.5rem 0;
    color: #fcd34d;
    font-size: 0.875rem;
    font-weight: 600;
  }

  .info-box ul {
    margin: 0;
    padding-left: 1.25rem;
    color: #fcd34d;
    font-size: 0.875rem;
  }

  .info-box li {
    margin-bottom: 0.25rem;
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
    display: flex;
    align-items: center;
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