<script lang="ts">
	import { enhance } from '$app/forms';
	import { Mail, ArrowLeft, Trophy } from 'lucide-svelte';
	import type { ActionData } from './$types';

	let { form }: { form: ActionData } = $props();
</script>

<svelte:head>
	<title>Forgot Password - Fantasy League</title>
</svelte:head>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center p-4">
	<div class="w-full max-w-md">
		<!-- Header -->
		<div class="text-center mb-8">
			<div class="flex items-center justify-center mb-4">
				<Trophy class="h-12 w-12 text-amber-400 mr-3" />
				<h1 class="text-3xl font-bold text-white">Fantasy League</h1>
			</div>
			<p class="text-slate-400">Reset your password</p>
		</div>

		<!-- Main Card -->
		<div class="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl shadow-2xl p-8">
			<!-- Success Message -->
			{#if form?.success}
				<div class="mb-6 p-4 bg-green-900/20 border border-green-500/50 rounded-lg">
					<p class="text-green-300 text-sm">{form.message}</p>
				</div>
				
				<div class="text-center">
					<p class="text-slate-300 mb-4">Check your email for the password reset link.</p>
					<a 
						href="/login"
						class="inline-flex items-center text-blue-400 hover:text-blue-300 transition-colors"
					>
						<ArrowLeft class="w-4 h-4 mr-2" />
						Back to Login
					</a>
				</div>
			{:else}
				<!-- Form -->
				<form method="post" use:enhance>
					<!-- Error Message -->
					{#if form?.message && !form?.success}
						<div class="mb-6 p-4 bg-red-900/20 border border-red-500/50 rounded-lg">
							<p class="text-red-300 text-sm">{form.message}</p>
						</div>
					{/if}

					<div class="mb-6">
						<h2 class="text-xl font-semibold text-white mb-2">Forgot your password?</h2>
						<p class="text-slate-400 text-sm">
							Enter your email address and we'll send you a link to reset your password.
						</p>
					</div>

					<div class="space-y-4">
						<div>
							<label for="email" class="block text-sm font-medium text-slate-300 mb-2">
								<Mail class="h-4 w-4 inline mr-2" />
								Email Address
							</label>
							<input
								type="email"
								id="email"
								name="email"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Enter your email address"
							/>
						</div>

						<button
							type="submit"
							class="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-3 px-4 
								rounded-lg transition-colors duration-200 focus:outline-none focus:ring-2 
								focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-slate-800"
						>
							Send Reset Link
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

		<!-- Help Text -->
		<div class="mt-6 text-center">
			<p class="text-slate-500 text-xs">
				If you don't receive an email within a few minutes, check your spam folder or contact the commissioner.
			</p>
		</div>
	</div>
</div> 