<script lang="ts">
	import { enhance } from '$app/forms';
	import { Lock, ArrowLeft, Trophy, AlertCircle, CheckCircle } from 'lucide-svelte';
	import type { ActionData, PageData } from './$types';

	let { form, data }: { form: ActionData; data: PageData } = $props();
	
	let password = $state('');
	let confirmPassword = $state('');
	
	let passwordsMatch = $derived(password === confirmPassword);
	let passwordValid = $derived(password.length >= 6);
	let formValid = $derived(passwordValid && passwordsMatch && password.length > 0);
</script>

<svelte:head>
	<title>Reset Password - Fantasy League</title>
</svelte:head>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center p-4">
	<div class="w-full max-w-md">
		<!-- Header -->
		<div class="text-center mb-8">
			<div class="flex items-center justify-center mb-4">
				<Trophy class="h-12 w-12 text-amber-400 mr-3" />
				<h1 class="text-3xl font-bold text-white">Fantasy League</h1>
			</div>
			<p class="text-slate-400">Set your new password</p>
		</div>

		<!-- Main Card -->
		<div class="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl shadow-2xl p-8">
			{#if data?.error}
				<!-- Token Error -->
				<div class="text-center">
					<AlertCircle class="h-16 w-16 text-red-400 mx-auto mb-4" />
					<h2 class="text-xl font-semibold text-white mb-2">Invalid Reset Link</h2>
					<p class="text-slate-400 mb-6">{data.error}</p>
					<a 
						href="/forgot-password"
						class="inline-flex items-center bg-blue-600 hover:bg-blue-700 text-white font-medium py-3 px-4 
							rounded-lg transition-colors duration-200 focus:outline-none focus:ring-2 
							focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-slate-800"
					>
						Request New Reset Link
					</a>
				</div>
			{:else}
				<!-- Reset Form -->
				<form method="post" use:enhance>
					<input type="hidden" name="token" value={data?.token} />
					
					<!-- Error Message -->
					{#if form?.message}
						<div class="mb-6 p-4 bg-red-900/20 border border-red-500/50 rounded-lg">
							<p class="text-red-300 text-sm">{form.message}</p>
						</div>
					{/if}

					<div class="mb-6">
						<h2 class="text-xl font-semibold text-white mb-2">Reset Your Password</h2>
						<p class="text-slate-400 text-sm">
							{#if data?.email}
								Resetting password for: <span class="text-slate-300">{data.email}</span>
							{:else}
								Enter your new password below.
							{/if}
						</p>
					</div>

					<div class="space-y-4">
						<div>
							<label for="password" class="block text-sm font-medium text-slate-300 mb-2">
								<Lock class="h-4 w-4 inline mr-2" />
								New Password
							</label>
							<input
								type="password"
								id="password"
								name="password"
								required
								bind:value={password}
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Enter your new password"
							/>
							{#if password.length > 0 && !passwordValid}
								<p class="text-red-400 text-xs mt-1">Password must be at least 6 characters</p>
							{:else if passwordValid}
								<p class="text-green-400 text-xs mt-1 flex items-center">
									<CheckCircle class="h-3 w-3 mr-1" />
									Password meets requirements
								</p>
							{/if}
						</div>

						<div>
							<label for="confirmPassword" class="block text-sm font-medium text-slate-300 mb-2">
								<Lock class="h-4 w-4 inline mr-2" />
								Confirm New Password
							</label>
							<input
								type="password"
								id="confirmPassword"
								name="confirmPassword"
								required
								bind:value={confirmPassword}
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Confirm your new password"
							/>
							{#if confirmPassword.length > 0 && !passwordsMatch}
								<p class="text-red-400 text-xs mt-1">Passwords do not match</p>
							{:else if confirmPassword.length > 0 && passwordsMatch}
								<p class="text-green-400 text-xs mt-1 flex items-center">
									<CheckCircle class="h-3 w-3 mr-1" />
									Passwords match
								</p>
							{/if}
						</div>

						<button
							type="submit"
							disabled={!formValid}
							class="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-slate-600 disabled:cursor-not-allowed 
								text-white font-medium py-3 px-4 rounded-lg transition-colors duration-200 
								focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-slate-800"
						>
							Reset Password
						</button>
					</div>
				</form>

				<!-- Back to Login -->
				<div class="mt-6 text-center">
					<a 
						href="/login"
						class="inline-flex items-center text-slate-400 hover:text-white transition-colors text-sm"
					>
						<ArrowLeft class="w-4 h-4 mr-2" />
						Back to Login
					</a>
				</div>
			{/if}
		</div>

		<!-- Security Notice -->
		<div class="mt-6 text-center">
			<p class="text-slate-500 text-xs">
				For your security, this link will expire after 1 hour and can only be used once.
			</p>
		</div>
	</div>
</div> 