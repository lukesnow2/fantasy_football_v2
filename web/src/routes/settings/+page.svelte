<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { Fingerprint, Shield, AlertTriangle, CheckCircle, Trash2, Plus, ArrowRight } from 'lucide-svelte';
	import PasskeyRegistration from '$lib/components/webauthn/PasskeyRegistration.svelte';
	import CredentialManager from '$lib/components/webauthn/CredentialManager.svelte';

	export let data;

	let user = data.user;
	let isLoading = false;
	let showPasskeySetup = false;
	let showCredentialManager = false;

	onMount(() => {
		console.log('🔧 Settings page loaded for user:', user);
	});

	function togglePasskeySetup() {
		showPasskeySetup = !showPasskeySetup;
	}

	function toggleCredentialManager() {
		showCredentialManager = !showCredentialManager;
	}

	function handlePasskeySuccess() {
		showPasskeySetup = false;
		// Refresh user data
		window.location.reload();
	}
</script>

<svelte:head>
	<title>Account Settings - The League</title>
</svelte:head>

<div class="min-h-screen bg-slate-900 text-white">
	<div class="container mx-auto px-4 py-8">
		<div class="max-w-4xl mx-auto">
			<!-- Header -->
			<div class="mb-8">
				<h1 class="text-3xl font-bold text-white mb-2">Account Settings</h1>
				<p class="text-slate-400">Manage your account and authentication preferences</p>
			</div>

			<!-- User Info -->
			<div class="bg-slate-800 rounded-lg p-6 mb-6">
				<h2 class="text-xl font-semibold text-white mb-4">Account Information</h2>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-1">Username</label>
						<p class="text-white font-medium">{user?.username}</p>
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-1">Manager</label>
						<p class="text-white font-medium">{user?.displayName || 'Unknown'}</p>
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-1">Email</label>
						<p class="text-white">{user?.email || 'Not provided'}</p>
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-1">Account Status</label>
						<p class="text-white">{user?.accountStatus || 'active'}</p>
					</div>
				</div>
			</div>

			<!-- Authentication Settings -->
			<div class="bg-slate-800 rounded-lg p-6 mb-6">
				<h2 class="text-xl font-semibold text-white mb-4">Authentication</h2>
				
				<!-- Password Authentication -->
				<div class="mb-6">
					<div class="flex items-center justify-between">
						<div class="flex items-center space-x-3">
							<Shield class="h-5 w-5 text-blue-400" />
							<div>
								<h3 class="text-white font-medium">Password Authentication</h3>
								<p class="text-slate-400 text-sm">Sign in with your username and password</p>
							</div>
						</div>
						<div class="flex items-center space-x-2">
							<CheckCircle class="h-4 w-4 text-green-400" />
							<span class="text-green-400 text-sm font-medium">Enabled</span>
						</div>
					</div>
				</div>

				<!-- Passkey Authentication -->
				<div class="mb-6">
					<div class="flex items-center justify-between">
						<div class="flex items-center space-x-3">
							<Fingerprint class="h-5 w-5 text-purple-400" />
							<div>
								<h3 class="text-white font-medium">Passkey Authentication</h3>
								<p class="text-slate-400 text-sm">
									{#if user?.passkeyEnabled}
										Sign in with biometric authentication
									{:else}
										Enable biometric authentication for faster, more secure sign-in
									{/if}
								</p>
							</div>
						</div>
						<div class="flex items-center space-x-2">
							{#if user?.passkeyEnabled}
								<CheckCircle class="h-4 w-4 text-green-400" />
								<span class="text-green-400 text-sm font-medium">Enabled</span>
							{:else}
								<AlertTriangle class="h-4 w-4 text-yellow-400" />
								<span class="text-yellow-400 text-sm font-medium">Not Set Up</span>
							{/if}
						</div>
					</div>

					{#if user?.passkeyEnabled}
						<div class="mt-4 flex space-x-3">
							<button
								on:click={toggleCredentialManager}
								class="flex items-center space-x-2 px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
							>
								<Trash2 class="h-4 w-4" />
								<span>Manage Passkeys</span>
							</button>
							<button
								on:click={togglePasskeySetup}
								class="flex items-center space-x-2 px-4 py-2 bg-purple-600 hover:bg-purple-700 rounded-lg transition-colors"
							>
								<Plus class="h-4 w-4" />
								<span>Add New Passkey</span>
							</button>
						</div>
					{:else}
						<div class="mt-4">
							<button
								on:click={togglePasskeySetup}
								class="flex items-center space-x-2 px-4 py-2 bg-purple-600 hover:bg-purple-700 rounded-lg transition-colors"
							>
								<Fingerprint class="h-4 w-4" />
								<span>Set Up Passkey</span>
								<ArrowRight class="h-4 w-4" />
							</button>
						</div>
					{/if}
				</div>

				<!-- Passkey Setup Modal -->
				{#if showPasskeySetup}
					<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
						<div class="bg-slate-800 rounded-lg p-6 max-w-md w-full mx-4">
							<h3 class="text-xl font-semibold text-white mb-4">Set Up Passkey</h3>
							<PasskeyRegistration 
								userId={user?.id}
								username={user?.username}
								on:registrationSuccess={handlePasskeySuccess}
							/>
							<button
								on:click={togglePasskeySetup}
								class="mt-4 w-full px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
							>
								Cancel
							</button>
						</div>
					</div>
				{/if}

				<!-- Credential Manager Modal -->
				{#if showCredentialManager}
					<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
						<div class="bg-slate-800 rounded-lg p-6 max-w-2xl w-full mx-4 max-h-[80vh] overflow-y-auto">
							<h3 class="text-xl font-semibold text-white mb-4">Manage Passkeys</h3>
							<CredentialManager userId={user?.id} />
							<button
								on:click={toggleCredentialManager}
								class="mt-4 w-full px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
							>
								Close
							</button>
						</div>
					</div>
				{/if}
			</div>

			<!-- Security Notice -->
			<div class="bg-blue-900/20 border border-blue-600/30 rounded-lg p-4">
				<div class="flex items-start space-x-3">
					<Shield class="h-5 w-5 text-blue-400 mt-0.5 flex-shrink-0" />
					<div>
						<h4 class="text-blue-300 font-medium">Enhanced Security</h4>
						<p class="text-blue-200 text-sm mt-1">
							Passkeys provide stronger security than passwords and are resistant to phishing attacks. 
							You can use both password and passkey authentication simultaneously.
						</p>
					</div>
				</div>
			</div>
		</div>
	</div>
</div> 