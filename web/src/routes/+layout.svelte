<script lang="ts">
	import '../app.css';
	import { page } from '$app/stores';
	import { enhance } from '$app/forms';
	import { Trophy, BarChart3, Calendar, Users, Crown, BookOpen, Target, Settings, LogIn, LogOut, User } from 'lucide-svelte';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	import type { LayoutData } from './$types';
	import type { SubmitFunction } from '@sveltejs/kit';

	let { data, children }: { data: LayoutData; children: any } = $props();

	const navigation = [
		{ name: 'This Season', href: '/this-season', icon: Calendar },
		{ name: 'Historical Deep Dive', href: '/historical', icon: BarChart3 },
		{ name: 'Hall of Fame', href: '/hall-of-fame', icon: Crown },
		{ name: 'Manager Profiles', href: '/managers', icon: Users },
		{ name: 'Trade Center', href: '/trades', icon: Target },
		{ name: 'Draft Central', href: '/draft', icon: Trophy },
		{ name: 'Constitution', href: '/constitution', icon: BookOpen },
	];

	let showUserMenu = $state(false);

	function toggleUserMenu() {
		showUserMenu = !showUserMenu;
		console.log('🔍 User menu toggled:', showUserMenu);
		console.log('🔍 User data:', data.user);
		console.log('🔍 Authenticated manager:', data.authenticatedManager);
	}

	function closeUserMenu() {
		showUserMenu = false;
	}

	const handleLogout: SubmitFunction = ({ formData, cancel }) => {
		console.log('🚪 Logout form submitted');
		// Close menu immediately
		closeUserMenu();
		// Let SvelteKit handle the redirect naturally
	};
</script>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900">
	<!-- Header -->
	<header class="border-b border-slate-700/50 bg-slate-900/80 backdrop-blur-md">
		<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
			<div class="flex h-16 items-center justify-between">
				<div class="flex items-center">
					<a href="/" class="flex items-center">
						<Trophy class="h-8 w-8 text-amber-400" />
						<h1 class="ml-2 text-xl font-bold text-white">The League</h1>
					</a>
				</div>

				<!-- Navigation -->
				<nav class="hidden md:flex space-x-8">
					{#each navigation as item}
						<a
							href={item.href}
							class="flex items-center px-3 py-2 text-sm font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
							class:bg-slate-800={$page.url.pathname === item.href}
							class:text-white={$page.url.pathname === item.href}
						>
							<svelte:component this={item.icon} class="h-4 w-4 mr-2" />
							{item.name}
						</a>
					{/each}
				</nav>

				<!-- User menu -->
				<div class="flex items-center space-x-4">
					{#if data.user && data.authenticatedManager}
						<!-- Authenticated User Menu -->
						<div class="relative">
							<button
								type="button"
								onclick={toggleUserMenu}
								class="flex items-center space-x-3 text-sm rounded-full bg-slate-800 p-2 text-white hover:bg-slate-700 transition-colors"
							>
								<ManagerProfilePicture 
									managerName={data.authenticatedManager.managerName}
									size="small"
									className="ring-2 ring-slate-600"
								/>
								<span class="hidden sm:block font-medium">{data.authenticatedManager.displayName}</span>
								<Settings class="h-4 w-4" />
							</button>

							{#if showUserMenu}
								<!-- User dropdown menu - Dark theme styling -->
								<div class="absolute right-0 z-50 mt-2 w-48 origin-top-right rounded-md bg-slate-800 py-1 shadow-lg ring-1 ring-slate-700 border border-slate-600">
									<div class="px-4 py-3 border-b border-slate-700">
										<p class="text-sm text-slate-300">Signed in as</p>
										<p class="text-sm font-medium text-white truncate">{data.authenticatedManager.displayName}</p>
									</div>
									<a href="/managers/{data.authenticatedManager.managerName}" class="flex items-center px-4 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700 transition-colors">
										<User class="h-4 w-4 mr-2" />
										Your Profile
									</a>
									<form method="post" action="/logout" use:enhance={handleLogout}>
										<button type="submit" class="w-full text-left flex items-center px-4 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700 transition-colors">
											<LogOut class="h-4 w-4 mr-2" />
											Sign Out
										</button>
									</form>
								</div>
							{/if}
						</div>
					{:else}
						<!-- Not authenticated -->
						<a 
							href="/login"
							class="flex items-center px-4 py-2 text-sm font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
						>
							<LogIn class="h-4 w-4 mr-2" />
							Sign In
						</a>
					{/if}

					<!-- Mobile menu button -->
					<div class="md:hidden">
						<button type="button" class="text-slate-300 hover:text-white">
							<Settings class="h-6 w-6" />
						</button>
					</div>
				</div>
			</div>
		</div>
	</header>

	<!-- Main content -->
	<main class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
		{@render children()}
	</main>

	<!-- Footer -->
	<footer class="border-t border-slate-700/50 bg-slate-900/50 mt-20">
		<div class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
			<div class="text-center text-slate-400">
				<p>&copy; 2024 The League. Where legends are made and dreams are crushed.</p>
			</div>
		</div>
	</footer>
</div>

<!-- Click outside to close user menu -->
<!-- TEMPORARILY DISABLED FOR TESTING -->
<!--
{#if showUserMenu}
	<div 
		class="fixed inset-0 z-40" 
		role="button" 
		tabindex="-1"
		onclick={(e) => {
			// Only close if clicking outside the dropdown area
			if (e.target === e.currentTarget) {
				closeUserMenu();
			}
		}}
		onkeydown={(e) => { if (e.key === 'Escape') closeUserMenu(); }}
		aria-label="Close user menu"
	></div>
{/if}
-->
