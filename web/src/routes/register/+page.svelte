<script lang="ts">
	import { Users, Lock, Mail, UserCheck, Trophy, ArrowLeft } from 'lucide-svelte';
	import { enhance } from '$app/forms';
	import { onMount } from 'svelte';

	export let data: {
		availableManagers: Array<{
			managerKey: number;
			managerName: string;
			displayName: string | null;
		}>;
	};

	let selectedManagerKey = '';
	let mounted = false;

	onMount(() => {
		mounted = true;
	});

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
	<title>Register - The League</title>
	<meta name="description" content="Create your fantasy league account" />
</svelte:head>

<div class="register-container">
	<div class="register-card">
		<div class="register-header">
			<a href="/login" class="back-link">
				<ArrowLeft class="h-4 w-4" />
				Back to Login
			</a>
			<div class="header-content">
				<div class="flex items-center justify-center mb-4">
					<Trophy class="h-12 w-12 text-amber-400 mr-3" />
					<h1 class="text-3xl font-bold text-white">Join The League</h1>
				</div>
				<p class="text-slate-400">Create your fantasy league account</p>
			</div>
		</div>

		<form method="post" action="/login?/register" use:enhance>
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

		<div class="mt-6 text-center">
			<p class="text-slate-400 text-sm">
				Already have an account?
				<a href="/login" class="text-blue-400 hover:text-blue-300 ml-1">
					Sign in
				</a>
			</p>
		</div>
	</div>
</div>

<style>
	.register-container {
		min-height: 100vh;
		background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 2rem;
	}

	.register-card {
		background: rgba(30, 41, 59, 0.8);
		backdrop-filter: blur(10px);
		border: 1px solid rgba(148, 163, 184, 0.1);
		border-radius: 16px;
		padding: 2.5rem;
		width: 100%;
		max-width: 500px;
		box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
	}

	.register-header {
		text-align: center;
		margin-bottom: 2rem;
	}

	.back-link {
		display: inline-flex;
		align-items: center;
		gap: 0.5rem;
		color: #94a3b8;
		text-decoration: none;
		font-size: 0.875rem;
		margin-bottom: 1rem;
		transition: color 0.2s;
	}

	.back-link:hover {
		color: #cbd5e1;
	}

	.header-content h1 {
		color: white;
		font-size: 1.875rem;
		font-weight: 700;
		margin: 0 0 0.5rem 0;
	}

	.header-content p {
		color: #94a3b8;
		margin: 0;
	}

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

	@media (max-width: 640px) {
		.register-container {
			padding: 1rem;
		}

		.register-card {
			padding: 2rem;
		}
	}
</style> 