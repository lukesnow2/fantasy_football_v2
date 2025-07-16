<script lang="ts">
	import { enhance } from '$app/forms';
	import { page } from '$app/stores';
	import { Users, Lock, Mail, UserCheck, Trophy } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import type { ActionData, PageData } from './$types';

	let { form, data }: { form: ActionData; data: PageData } = $props();
	
	let isLoginMode = $state(true);
	let selectedManagerKey = $state('');
	let mounted = $state(false);

	onMount(() => {
		mounted = true;
	});

	function toggleMode() {
		isLoginMode = !isLoginMode;
		// Clear any form errors when switching
		if (form) {
			form = null;
		}
	}

	function getManagerProfileImage(managerName: string): string {
		// Convert manager name to filename format
		const filename = managerName
			.toLowerCase()
			.replace(/\s+/g, '_')
			.replace(/[^a-z0-9_]/g, '');
		
		// Map common variations to actual filenames
		const nameMap: Record<string, string> = {
			'craig': 'craig.jpeg',
			'erik_snow': 'erik.jpeg', 
			'gabe_flores': 'gabe_flores.jpeg',
			'gabe_the_younger': 'gabe_the_younger.jpeg',
			'israel': 'israel_flores.jpg',
			'luke_s': 'luke_s.JPG',
			'nick': 'nick.jpeg',
			'omar': 'omar.jpg',
			'trevor': 'trevor_cramer.jpeg',
			'troy_colvin': 'troy.jpeg'
		};

		return `/manager-profiles/${nameMap[filename] || 'default.jpg'}`;
	}
</script>

<svelte:head>
	<title>Fantasy League - {mounted ? (isLoginMode ? 'Login' : 'Register') : 'Login'}</title>
</svelte:head>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center p-4">
	<div class="w-full max-w-md">
		<!-- Header -->
		<div class="text-center mb-8">
			<div class="flex items-center justify-center mb-4">
				<Trophy class="h-12 w-12 text-amber-400 mr-3" />
				<h1 class="text-3xl font-bold text-white">Fantasy League</h1>
			</div>
			<p class="text-slate-400">
				{isLoginMode ? 'Welcome back!' : 'Join the league'}
			</p>
		</div>

		<!-- Main Card -->
		<div class="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl shadow-2xl p-8">
			<!-- Tab Switcher -->
			<div class="flex bg-slate-700/50 rounded-lg p-1 mb-6">
				<button
					class="flex-1 py-2 px-4 text-sm font-medium rounded-md transition-colors duration-200
						{isLoginMode 
							? 'bg-blue-600 text-white shadow-sm' 
							: 'text-slate-300 hover:text-white hover:bg-slate-600/50'}"
					onclick={() => toggleMode()}
					disabled={isLoginMode}
				>
					Login
				</button>
				<button
					class="flex-1 py-2 px-4 text-sm font-medium rounded-md transition-colors duration-200
						{!isLoginMode 
							? 'bg-blue-600 text-white shadow-sm' 
							: 'text-slate-300 hover:text-white hover:bg-slate-600/50'}"
					onclick={() => toggleMode()}
					disabled={!isLoginMode}
				>
					Register
				</button>
			</div>

			<!-- Success Message -->
			{#if data.successMessage}
				<div class="mb-6 p-4 bg-green-900/20 border border-green-500/50 rounded-lg">
					<p class="text-green-300 text-sm">{data.successMessage}</p>
				</div>
			{/if}

			<!-- Error Message -->
			{#if form?.message}
				<div class="mb-6 p-4 bg-red-900/20 border border-red-500/50 rounded-lg">
					<p class="text-red-300 text-sm">{form.message}</p>
				</div>
			{/if}

			<!-- Login Form -->
			{#if isLoginMode}
				<form method="post" action="?/login" use:enhance>
					<div class="space-y-4">
						<div>
							<label for="username" class="block text-sm font-medium text-slate-300 mb-2">
								<Users class="h-4 w-4 inline mr-2" />
								Username
							</label>
							<input
								type="text"
								id="username"
								name="username"
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
							<input
								type="password"
								id="password"
								name="password"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Enter your password"
							/>
						</div>

						<button
							type="submit"
							class="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-3 px-4 
								rounded-lg transition-colors duration-200 focus:outline-none focus:ring-2 
								focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-slate-800"
						>
							Sign In
						</button>
					</div>
				</form>

				<!-- Forgot Password Link -->
				<div class="mt-4 text-center">
					<a 
						href="/forgot-password"
						class="text-slate-400 hover:text-blue-400 text-sm transition-colors"
					>
						Forgot your password?
					</a>
				</div>
			{:else}
				<!-- Register Form -->
				<form method="post" action="?/register" use:enhance>
					<div class="space-y-4">
						<div>
							<label for="reg-username" class="block text-sm font-medium text-slate-300 mb-2">
								<Users class="h-4 w-4 inline mr-2" />
								Username
							</label>
							<input
								type="text"
								id="reg-username"
								name="username"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Choose a username"
							/>
						</div>

						<div>
							<label for="email" class="block text-sm font-medium text-slate-300 mb-2">
								<Mail class="h-4 w-4 inline mr-2" />
								Email
							</label>
							<input
								type="email"
								id="email"
								name="email"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="your@email.com"
							/>
						</div>

						<div>
							<label for="reg-password" class="block text-sm font-medium text-slate-300 mb-2">
								<Lock class="h-4 w-4 inline mr-2" />
								Password
							</label>
							<input
								type="password"
								id="reg-password"
								name="password"
								required
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white placeholder-slate-400 focus:outline-none focus:ring-2 
									focus:ring-blue-500 focus:border-transparent transition-colors"
								placeholder="Create a password (min 6 characters)"
							/>
						</div>

						<div>
							<label for="managerKey" class="block text-sm font-medium text-slate-300 mb-2">
								<UserCheck class="h-4 w-4 inline mr-2" />
								Select Your Manager Profile
							</label>
							<select
								id="managerKey"
								name="managerKey"
								required
								bind:value={selectedManagerKey}
								class="w-full px-4 py-3 bg-slate-700/50 border border-slate-600 rounded-lg 
									text-white focus:outline-none focus:ring-2 focus:ring-blue-500 
									focus:border-transparent transition-colors"
							>
								<option value="">Choose your manager profile...</option>
								{#each data.availableManagers as manager}
									<option value={manager.managerKey}>
										{manager.displayName || manager.managerName}
									</option>
								{/each}
							</select>
						</div>

						<!-- Manager Preview -->
						{#if mounted && selectedManagerKey}
							{#each data.availableManagers as manager}
								{#if manager.managerKey.toString() === selectedManagerKey}
									<div class="p-4 bg-slate-700/30 rounded-lg border border-slate-600/50">
										<div class="flex items-center space-x-3">
											<img
												src={getManagerProfileImage(manager.managerName)}
												alt={manager.managerName}
												class="h-12 w-12 rounded-full object-cover border-2 border-slate-500"
												onerror={(e) => { 
													const target = e.target as HTMLImageElement;
													if (target) target.src = '/manager-profiles/default.jpg'; 
												}}
											/>
											<div>
												<p class="text-white font-medium">{manager.displayName || manager.managerName}</p>
												<p class="text-slate-400 text-sm">Fantasy Manager</p>
											</div>
										</div>
									</div>
								{/if}
							{/each}
						{/if}

						<button
							type="submit"
							class="w-full bg-green-600 hover:bg-green-700 text-white font-medium py-3 px-4 
								rounded-lg transition-colors duration-200 focus:outline-none focus:ring-2 
								focus:ring-green-500 focus:ring-offset-2 focus:ring-offset-slate-800"
						>
							Create Account
						</button>
					</div>
				</form>
			{/if}

			<!-- Footer -->
			<div class="mt-6 text-center">
				<p class="text-slate-400 text-sm">
					{isLoginMode ? "Don't have an account?" : 'Already have an account?'}
					<button
						type="button"
						onclick={() => toggleMode()}
						class="text-blue-400 hover:text-blue-300 font-medium transition-colors ml-1"
					>
						{isLoginMode ? 'Register here' : 'Login here'}
					</button>
				</p>
			</div>
		</div>

		<!-- Help Text -->
		<div class="mt-6 text-center">
			<p class="text-slate-500 text-sm">
				Need help? Contact your league commissioner.
			</p>
		</div>
	</div>
</div>

<style>
	/* Custom scrollbar for select */
	select::-webkit-scrollbar {
		width: 8px;
	}
	
	select::-webkit-scrollbar-track {
		background: rgb(51 65 85);
		border-radius: 4px;
	}
	
	select::-webkit-scrollbar-thumb {
		background: rgb(71 85 105);
		border-radius: 4px;
	}
	
	select::-webkit-scrollbar-thumb:hover {
		background: rgb(100 116 139);
	}
</style> 