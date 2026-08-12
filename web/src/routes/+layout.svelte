<script lang="ts">
	import '../app.css';
	import { page } from '$app/stores';
	import { enhance } from '$app/forms';
	import { Trophy, BarChart3, Calendar, Users, Crown, BookOpen, Target, TrendingUp, MessageSquare, Database, Shield, ChevronDown, Settings, LogIn, LogOut, User, Menu, X } from 'lucide-svelte';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	import NavDropdown, { type NavItem } from '$lib/components/NavDropdown.svelte';
	import type { LayoutData } from './$types';
	import type { SubmitFunction } from '@sveltejs/kit';

	let { data, children }: { data: LayoutData; children: any } = $props();

	/**
	 * The bar is two direct links plus three subject dropdowns.
	 *
	 * There is deliberately no "More". A catch-all bucket is where the pages
	 * nobody owns go to be forgotten, and the last version put Hall of Fame,
	 * Draft Central and the whole 20-season archive in it. Every page now sits
	 * under a heading that says what it is, one click deep at most.
	 */
	const directLinks = [
		{ name: 'This Season', href: '/this-season', icon: Calendar },
		{ name: 'Chat', href: '/chat', icon: MessageSquare }
	];

	const navGroups: { label: string; icon: typeof Calendar; items: NavItem[] }[] = [
		{
			label: 'Rankings',
			icon: TrendingUp,
			items: [
				{
					name: 'Power Rankings',
					href: '/power-rankings',
					icon: TrendingUp,
					blurb: "Who's actually good right now"
				},
				{
					name: 'Hall of Fame',
					href: '/hall-of-fame',
					icon: Crown,
					blurb: 'The all-time leaderboard'
				},
				{
					name: 'Historical Deep Dive',
					href: '/historical',
					icon: BarChart3,
					blurb: 'Twenty seasons of analysis'
				}
			]
		},
		{
			label: 'Managers',
			icon: Users,
			items: [
				{
					name: 'Manager Profiles',
					href: '/managers',
					icon: User,
					blurb: 'Career stats and head-to-head'
				},
				{
					name: 'Trade Center',
					href: '/trades',
					icon: Target,
					blurb: 'Every trade ever made'
				},
				{
					name: 'Draft Central',
					href: '/draft',
					icon: Trophy,
					blurb: 'Draft boards and grades'
				}
			]
		},
		{
			label: 'League',
			icon: BookOpen,
			items: [
				{
					name: 'Constitution',
					href: '/constitution',
					icon: BookOpen,
					blurb: 'Rules, proposals and voting'
				},
				{
					name: 'Data Dictionary',
					href: '/data-dictionary',
					icon: Database,
					blurb: 'What every metric means'
				}
			]
		}
	];

	let showUserMenu = $state(false);
	let showMobileMenu = $state(false);
	// One open menu at a time, held here rather than in each dropdown — otherwise
	// clicking a second trigger leaves two panels overlapping.
	let openNavMenu = $state<string | null>(null);

	const isCommissioner = $derived(data.member?.role === 'commissioner');
	// The dropdown links to the manager's public stats page. Encoded because every
	// name in the roster has a space in it ("Gabe the Younger", "Troy Colvin").
	const profileHref = $derived(
		data.authenticatedManager
			? `/managers/${encodeURIComponent(data.authenticatedManager.managerName)}`
			: '/managers'
	);

	function toggleUserMenu() {
		showUserMenu = !showUserMenu;
		if (showUserMenu) openNavMenu = null;
	}

	function closeUserMenu() {
		showUserMenu = false;
	}

	function setNavMenu(label: string | null) {
		openNavMenu = label;
		if (label !== null) showUserMenu = false;
	}

	function toggleMobileMenu() {
		showMobileMenu = !showMobileMenu;
	}

	function closeMobileMenu() {
		showMobileMenu = false;
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key !== 'Escape') return;
		if (showMobileMenu) closeMobileMenu();
		if (openNavMenu) openNavMenu = null;
		if (showUserMenu) closeUserMenu();
	}

	const handleLogout: SubmitFunction = ({ formData, cancel }) => {
		// Close menu immediately
		closeUserMenu();
		// Let SvelteKit handle the redirect naturally
	};
</script>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900" onkeydown={handleKeydown} role="main">
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

				<!-- Navigation. `lg` rather than `md`: at 768px even the old seven-item
				     bar overflowed into the user menu. -->
				<nav class="hidden lg:flex items-center space-x-1">
					{#each directLinks as item (item.href)}
						{@const Icon = item.icon}
						<a
							href={item.href}
							class="flex items-center px-3 py-2 text-sm font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors whitespace-nowrap"
							class:bg-slate-800={$page.url.pathname === item.href}
							class:text-white={$page.url.pathname === item.href}
						>
							<Icon class="h-4 w-4 mr-2" />
							{item.name}
						</a>
					{/each}

					{#each navGroups as group (group.label)}
						<NavDropdown
							label={group.label}
							icon={group.icon}
							items={group.items}
							open={openNavMenu}
							onToggle={setNavMenu}
						/>
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
								aria-expanded={showUserMenu}
								aria-haspopup="true"
								class="flex items-center space-x-3 text-sm rounded-full bg-slate-800 p-2 text-white hover:bg-slate-700 transition-colors"
							>
								<ManagerProfilePicture 
									managerName={data.authenticatedManager.managerName}
									size="small"
									className="ring-2 ring-slate-600"
								/>
								<span class="hidden sm:block font-medium">{data.authenticatedManager.displayName}</span>
								<!-- A chevron, not a cog. The cog that used to sit here read as a
								     link to /settings and wasn't one. -->
								<ChevronDown class="h-4 w-4" />
							</button>

							{#if showUserMenu}
								<!-- User dropdown menu - Dark theme styling -->
								<div class="absolute right-0 z-50 mt-2 w-48 origin-top-right rounded-md bg-slate-800 py-1 shadow-lg ring-1 ring-slate-700 border border-slate-600">
									<div class="px-4 py-3 border-b border-slate-700">
										<p class="text-sm text-slate-300">Signed in as</p>
										<p class="text-sm font-medium text-white truncate">{data.authenticatedManager.displayName}</p>
									</div>
									<a href={profileHref} onclick={closeUserMenu} class="flex items-center px-4 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700 transition-colors">
										<User class="h-4 w-4 mr-2" />
										Your Profile
									</a>
									<a href="/settings" onclick={closeUserMenu} class="flex items-center px-4 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700 transition-colors">
										<Settings class="h-4 w-4 mr-2" />
										Settings
									</a>
									{#if isCommissioner}
										<a href="/admin/members" onclick={closeUserMenu} class="flex items-center px-4 py-2 text-sm text-amber-300 hover:text-amber-200 hover:bg-slate-700 transition-colors">
											<Shield class="h-4 w-4 mr-2" />
											Commissioner
										</a>
									{/if}
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
					<div class="lg:hidden">
						<button 
							type="button" 
							onclick={toggleMobileMenu}
							class="text-slate-300 hover:text-white p-2 rounded-md transition-colors"
							aria-label={showMobileMenu ? 'Close menu' : 'Open menu'}
						>
							{#if showMobileMenu}
								<X class="h-6 w-6" />
							{:else}
								<Menu class="h-6 w-6" />
							{/if}
						</button>
					</div>
				</div>
			</div>
		</div>
	</header>

	<!-- Mobile Navigation Menu -->
	{#if showMobileMenu}
		<div class="lg:hidden relative z-50">
			<!-- Backdrop -->
			<div 
				class="fixed inset-0 bg-black/50 backdrop-blur-sm" 
				onclick={closeMobileMenu}
				onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') closeMobileMenu(); }}
				role="button"
				tabindex="-1"
				aria-label="Close mobile menu"
			></div>
			
			<!-- Mobile Menu Panel -->
			<div class="fixed top-0 left-0 w-full bg-slate-900 border-b border-slate-700 shadow-xl">
				<!-- Mobile Navigation Links -->
				<nav class="max-h-screen overflow-y-auto px-4 py-6 space-y-2">
					<!-- The drawer has the vertical room the bar doesn't, so the groups
					     open flat under their headings rather than as nested menus. -->
					{#each directLinks as item (item.href)}
						{@const Icon = item.icon}
						<a
							href={item.href}
							onclick={closeMobileMenu}
							class="flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
							class:bg-slate-800={$page.url.pathname === item.href}
							class:text-white={$page.url.pathname === item.href}
						>
							<Icon class="h-5 w-5 mr-3" />
							{item.name}
						</a>
					{/each}

					{#each navGroups as group (group.label)}
						<div class="pt-3">
							<p class="px-4 pb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
								{group.label}
							</p>
							{#each group.items as item (item.href)}
								{@const Icon = item.icon}
								<a
									href={item.href}
									onclick={closeMobileMenu}
									class="flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
									class:bg-slate-800={$page.url.pathname === item.href}
									class:text-white={$page.url.pathname === item.href}
								>
									<Icon class="h-5 w-5 mr-3" />
									{item.name}
								</a>
							{/each}
						</div>
					{/each}

					<!-- Auth links for mobile -->
					{#if !data.user || !data.authenticatedManager}
						<div class="border-t border-slate-700 pt-4 mt-4">
							<a 
								href="/login"
								onclick={closeMobileMenu}
								class="flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
							>
								<LogIn class="h-5 w-5 mr-3" />
								Sign In
							</a>
						</div>
					{:else}
						<div class="border-t border-slate-700 pt-4 mt-4">
							<a
								href={profileHref}
								onclick={closeMobileMenu}
								class="flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
							>
								<User class="h-5 w-5 mr-3" />
								Your Profile
							</a>
							<a
								href="/settings"
								onclick={closeMobileMenu}
								class="flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
							>
								<Settings class="h-5 w-5 mr-3" />
								Settings
							</a>
							{#if isCommissioner}
								<a
									href="/admin/members"
									onclick={closeMobileMenu}
									class="flex items-center px-4 py-3 text-base font-medium text-amber-300 hover:text-amber-200 hover:bg-slate-800 rounded-md transition-colors"
								>
									<Shield class="h-5 w-5 mr-3" />
									Commissioner
								</a>
							{/if}
							<form method="post" action="/logout" use:enhance={handleLogout}>
								<button 
									type="submit" 
									onclick={closeMobileMenu}
									class="w-full text-left flex items-center px-4 py-3 text-base font-medium text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
								>
									<LogOut class="h-5 w-5 mr-3" />
									Sign Out
								</button>
							</form>
						</div>
					{/if}
				</nav>
			</div>
		</div>
	{/if}

	<!-- Main content -->
	<main class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
		{@render children()}
	</main>

	<!-- Footer -->
	<footer class="border-t border-slate-700/50 bg-slate-900/50 mt-20">
		<div class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
			<div class="text-center text-slate-400">
				<p>&copy; {new Date().getFullYear()} The League. Where legends are made and dreams are crushed.</p>
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
