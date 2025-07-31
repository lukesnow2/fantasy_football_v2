<script lang="ts">
	import { onMount } from 'svelte';
	import { Users, Lock, Mail, Eye, EyeOff, Fingerprint, Shield } from 'lucide-svelte';
	import { enhance } from '$app/forms';
	import PasskeyAuthentication from '$lib/components/webauthn/PasskeyAuthentication.svelte';

	export let data: {
		availableManagers?: Array<{
			managerKey: number;
			managerName: string;
			displayName: string | null;
		}>;
		successMessage?: string | null;
		message?: string | null;
	};

	let showPassword = false;
	let authMode: 'passkey' | 'password' = 'passkey';
	let isSupported = false;
	let isAvailable = false;
	let selectedManager = '';
	let username = '';

	onMount(async () => {
		// Check WebAuthn support
		if (typeof window !== 'undefined') {
			isSupported = window.PublicKeyCredential !== undefined;
			if (isSupported) {
				isAvailable = await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable();
			}
		}
	});

	function toggleAuthMode() {
		authMode = authMode === 'passkey' ? 'password' : 'passkey';
	}

	function togglePasswordVisibility() {
		showPassword = !showPassword;
	}

	// Update selected manager when username changes
	$: if (username && data?.availableManagers) {
		const user = data.availableManagers.find(m => m.managerName === username);
		selectedManager = user ? user.managerName : '';
	}
</script>

<svelte:head>
	<title>Login - The League</title>
	<meta name="description" content="Sign in to your fantasy league account" />
</svelte:head>

<div class="login-container">
	<div class="login-card">
		<div class="login-header">
			<h1>Welcome Back</h1>
			<p>Sign in to your fantasy league account</p>
		</div>

		<!-- Authentication Mode Toggle -->
		<div class="auth-mode-toggle">
			<button 
				class="toggle-button {authMode === 'passkey' ? 'active' : ''}"
				on:click={() => authMode = 'passkey'}
				disabled={!isSupported || !isAvailable}
			>
				<Fingerprint class="h-4 w-4" />
				Passkey
			</button>
			<button 
				class="toggle-button {authMode === 'password' ? 'active' : ''}"
				on:click={() => authMode = 'password'}
			>
				<Lock class="h-4 w-4" />
				Password
			</button>
		</div>

		{#if authMode === 'passkey'}
			<!-- Passkey Authentication -->
			{#if !isSupported}
				<div class="error-banner">
					<h3>⚠️ WebAuthn Not Supported</h3>
					<p>Your browser doesn't support passkeys. Please use password authentication.</p>
					<button on:click={() => authMode = 'password'} class="secondary-button">
						Switch to Password
					</button>
				</div>
			{:else if !isAvailable}
				<div class="warning-banner">
					<h3>⚠️ Platform Authenticator Not Available</h3>
					<p>Your device doesn't support biometric authentication. Please use password authentication.</p>
					<button on:click={() => authMode = 'password'} class="secondary-button">
						Switch to Password
					</button>
				</div>
			{:else}
				<!-- Username and Manager Selection for Passkey Authentication -->
				<div class="space-y-4">
					<div>
						<label for="username-passkey" class="block text-sm font-medium text-slate-300 mb-2">
							<Users class="h-4 w-4 inline mr-2" />
							Username
						</label>
						<input
							type="text"
							id="username-passkey"
							bind:value={username}
							required
							class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
								text-white placeholder-slate-400 focus:outline-none focus:ring-2 
								focus:ring-blue-500 focus:border-transparent transition-colors"
							placeholder="Enter your username"
						/>
					</div>

					{#if username && selectedManager}
						<div class="bg-slate-700/30 border border-slate-600 rounded-lg p-4">
							<p class="text-slate-300 text-sm mb-2">Authenticating as:</p>
							<p class="text-white font-medium">{selectedManager}</p>
						</div>
						<PasskeyAuthentication username={username} />
					{:else if username && !selectedManager}
						<div class="text-center py-8">
							<p class="text-red-400">No account found for username: {username}</p>
							<p class="text-slate-400 text-sm mt-2">Please check your username or sign up for an account.</p>
						</div>
					{:else}
						<div class="text-center py-8">
							<p class="text-slate-400">Please enter your username to continue with passkey authentication.</p>
						</div>
					{/if}
				</div>
			{/if}
		{:else}
			<!-- Password Authentication -->
			{#if data?.successMessage}
				<div class="success-message">
					<p>{data.successMessage}</p>
				</div>
			{/if}

			{#if data?.message}
				<div class="error-message">
					<p>{data.message}</p>
				</div>
			{/if}

			<form method="post" action="?/login" use:enhance>
				<div class="space-y-4">
					<div>
						<label for="username-password" class="block text-sm font-medium text-slate-300 mb-2">
							<Users class="h-4 w-4 inline mr-2" />
							Username
						</label>
						<input
							type="text"
							id="username-password"
							name="username"
							bind:value={username}
							required
							class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
								text-white placeholder-slate-400 focus:outline-none focus:ring-2 
								focus:ring-blue-500 focus:border-transparent transition-colors"
							placeholder="Enter your username"
						/>
					</div>

					<div>
						<label for="password" class="block text-sm font-medium text-slate-300 mb-2">
							<Lock class="h-4 w-4 inline mr-2" />
							Password
						</label>
						<div class="relative">
							<input
								type={showPassword ? 'text' : 'password'}
								id="password"
								name="password"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors pr-12"
								placeholder="Enter your password"
							/>
							<button
								type="button"
								on:click={togglePasswordVisibility}
								class="absolute right-3 top-1/2 transform -translate-y-1/2 text-slate-400 hover:text-slate-300"
							>
								{#if showPassword}
									<EyeOff class="h-4 w-4" />
								{:else}
									<Eye class="h-4 w-4" />
								{/if}
							</button>
						</div>
					</div>

					<button
						type="submit"
						class="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-4 rounded-lg 
							transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 
							focus:ring-offset-slate-900"
					>
						Sign In
					</button>
				</div>
			</form>

			<!-- Hybrid Auth Notice for Users with Passkey Enabled -->
			{#if username && selectedManager}
				<div class="mt-6 p-4 bg-blue-900/20 border border-blue-600/30 rounded-lg">
					<div class="flex items-start space-x-3">
						<Shield class="h-5 w-5 text-blue-400 mt-0.5 flex-shrink-0" />
						<div>
							<h4 class="text-blue-300 font-medium">Enhanced Security Available</h4>
							<p class="text-blue-200 text-sm mt-1">
								You can also sign in with your passkey for faster, more secure authentication.
							</p>
							<button 
								on:click={() => authMode = 'passkey'}
								class="mt-2 text-blue-300 hover:text-blue-200 text-sm font-medium underline"
							>
								Try Passkey Sign In →
							</button>
						</div>
					</div>
				</div>
			{/if}
		{/if}

		<div class="mt-8 text-center">
			<p class="text-slate-400 text-sm">
				Don't have an account?
				<a href="/register" class="text-blue-400 hover:text-blue-300 ml-1">
					Sign up
				</a>
			</p>
		</div>
	</div>
</div>

<style>
	.login-container {
		min-height: 100vh;
		background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 2rem;
	}

	.login-card {
		background: rgba(30, 41, 59, 0.8);
		backdrop-filter: blur(10px);
		border: 1px solid rgba(148, 163, 184, 0.1);
		border-radius: 16px;
		padding: 2.5rem;
		width: 100%;
		max-width: 400px;
		box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
	}

	.login-header {
		text-align: center;
		margin-bottom: 2rem;
	}

	.login-header h1 {
		color: white;
		font-size: 1.875rem;
		font-weight: 700;
		margin: 0 0 0.5rem 0;
	}

	.login-header p {
		color: #94a3b8;
		margin: 0;
	}

	.auth-mode-toggle {
		display: flex;
		background: rgba(51, 65, 85, 0.5);
		border-radius: 8px;
		padding: 4px;
		margin-bottom: 2rem;
	}

	.toggle-button {
		flex: 1;
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 0.5rem;
		padding: 0.75rem;
		border: none;
		border-radius: 6px;
		background: transparent;
		color: #94a3b8;
		font-size: 0.875rem;
		font-weight: 500;
		cursor: pointer;
		transition: all 0.2s;
	}

	.toggle-button.active {
		background: #3b82f6;
		color: white;
	}

	.toggle-button:not(.active):hover {
		color: #cbd5e1;
	}

	.toggle-button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.error-banner, .warning-banner {
		padding: 1rem;
		border-radius: 8px;
		margin-bottom: 1.5rem;
		text-align: center;
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

	.success-message {
		background: rgba(34, 197, 94, 0.1);
		border: 1px solid rgba(34, 197, 94, 0.3);
		color: #86efac;
		padding: 1rem;
		border-radius: 8px;
		margin-bottom: 1.5rem;
		text-align: center;
	}

	.error-message {
		background: rgba(239, 68, 68, 0.1);
		border: 1px solid rgba(239, 68, 68, 0.3);
		color: #fca5a5;
		padding: 1rem;
		border-radius: 8px;
		margin-bottom: 1.5rem;
		text-align: center;
	}

	.secondary-button {
		background: transparent;
		color: #94a3b8;
		border: 1px solid #475569;
		padding: 0.5rem 1rem;
		border-radius: 6px;
		font-size: 0.875rem;
		cursor: pointer;
		transition: all 0.2s;
	}

	.secondary-button:hover {
		background: #475569;
		color: white;
	}

	@media (max-width: 640px) {
		.login-container {
			padding: 1rem;
		}

		.login-card {
			padding: 2rem;
		}
	}
</style> 