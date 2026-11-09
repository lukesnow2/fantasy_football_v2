<script lang="ts">
	import '../app.css';
	import { tick } from 'svelte';
	import { page } from '$app/state';
	import { afterNavigate } from '$app/navigation';
	import { enhance } from '$app/forms';
	import { dev } from '$app/environment';
	import { injectAnalytics } from '@vercel/analytics/sveltekit';
	import { ChevronDown, Menu, X } from 'lucide-svelte';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { directLinks, navGroups, isActivePath, nextMenuIndex } from '$lib/nav';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	import NavDropdown from '$lib/components/NavDropdown.svelte';
	import type { LayoutData } from './$types';
	import type { SubmitFunction } from '@sveltejs/kit';

	let { data, children }: { data: LayoutData; children: any } = $props();

	// Vercel Web Analytics. In dev it logs page views to the console instead of sending them.
	injectAnalytics({ mode: dev ? 'development' : 'production' });

	let showUserMenu = $state(false);
	let showMobileMenu = $state(false);
	// One open menu at a time, held here rather than in each dropdown, otherwise
	// clicking a second trigger leaves two panels overlapping.
	let openNavMenu = $state<string | null>(null);

	let userButton = $state<HTMLButtonElement | null>(null);
	let userMenu = $state<HTMLDivElement | null>(null);

	const pathname = $derived(page.url.pathname);
	const signedIn = $derived(Boolean(data.user && data.authenticatedManager));
	const isCommissioner = $derived(data.member?.role === 'commissioner');
	// The dropdown links to the manager's public stats page. Encoded because every
	// name in the roster has a space in it ("Gabe the Younger", "Troy Colvin").
	const profileHref = $derived(
		data.authenticatedManager
			? `/managers/${encodeURIComponent(data.authenticatedManager.managerName)}`
			: '/managers'
	);

	// Any navigation closes every menu, including one a link didn't close itself
	// (the back button, a link in the page behind an open menu).
	afterNavigate(() => {
		openNavMenu = null;
		showUserMenu = false;
		showMobileMenu = false;
	});

	function userMenuItems(): HTMLElement[] {
		return [...(userMenu?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? [])];
	}

	async function toggleUserMenu(event: MouseEvent) {
		showUserMenu = !showUserMenu;
		if (!showUserMenu) return;
		openNavMenu = null;
		// From the keyboard (a click with no pointer behind it), focus the first
		// item, as the nav menus do.
		if (event.detail === 0) {
			await tick();
			userMenuItems()[0]?.focus();
		}
	}

	function closeUserMenu() {
		showUserMenu = false;
	}

	function onUserMenuKeydown(event: KeyboardEvent) {
		const items = userMenuItems();
		const next = nextMenuIndex(
			items.indexOf(document.activeElement as HTMLElement),
			items.length,
			event.key
		);
		if (next === null) return;
		event.preventDefault();
		items[next].focus();
	}

	function onUserMenuFocusout(event: FocusEvent) {
		const to = event.relatedTarget as Node | null;
		if (to && !userMenu?.contains(to) && !userButton?.contains(to)) closeUserMenu();
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
		if (showUserMenu) {
			closeUserMenu();
			userButton?.focus();
		}
	}

	const handleLogout: SubmitFunction = () => {
		// Close the menu at once and let SvelteKit handle the redirect.
		closeUserMenu();
	};
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900">
	<!-- Header -->
	<header class="border-b border-slate-700/50 bg-slate-900/80 backdrop-blur-md">
		<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
			<div class="flex h-16 items-center justify-between">
				<div class="flex items-center">
					<a href="/" class="flex items-center">
						<h1 class="ml-2 text-xl font-bold text-white">The League</h1>
					</a>
				</div>

				<!-- Navigation. `lg` rather than `md`: at 768px even the old seven-item
				     bar overflowed into the user menu. -->
				<nav class="hidden items-center space-x-1 lg:flex">
					{#each directLinks as item (item.href)}
						<a
							href={item.href}
							aria-current={isActivePath(pathname, item.href) ? 'page' : undefined}
							class="flex items-center rounded-md px-3 py-2 text-sm font-medium whitespace-nowrap text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
							class:bg-slate-800={isActivePath(pathname, item.href)}
							class:text-white={isActivePath(pathname, item.href)}
						>
							{item.name}
						</a>
					{/each}

					{#each navGroups as group (group.label)}
						<NavDropdown
							label={group.label}
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
								bind:this={userButton}
								onclick={toggleUserMenu}
								aria-expanded={showUserMenu}
								aria-haspopup="menu"
								class="flex items-center space-x-3 rounded-full bg-slate-800 p-2 text-sm text-white transition-colors hover:bg-slate-700"
							>
								<ManagerProfilePicture
									managerName={data.authenticatedManager.managerName}
									size="small"
									className="ring-2 ring-slate-600"
								/>
								<span class="hidden font-medium sm:block"
									>{data.authenticatedManager.displayName}</span
								>
								<!-- A chevron, not a cog. The cog that used to sit here read as a
								     link to /settings and wasn't one. -->
								<ChevronDown class="h-4 w-4" />
							</button>

							{#if showUserMenu}
								<div
									bind:this={userMenu}
									role="menu"
									aria-label="Account"
									tabindex="-1"
									use:clickOutside={{ onOutside: closeUserMenu, ignore: [userButton] }}
									onkeydown={onUserMenuKeydown}
									onfocusout={onUserMenuFocusout}
									class="absolute right-0 z-50 mt-2 w-48 origin-top-right rounded-md border border-slate-600 bg-slate-800 py-1 shadow-lg ring-1 ring-slate-700"
								>
									<div role="none" class="border-b border-slate-700 px-4 py-3">
										<p class="text-sm text-slate-300">Signed in as</p>
										<p class="truncate text-sm font-medium text-white">
											{data.authenticatedManager.displayName}
										</p>
									</div>
									<a
										href={profileHref}
										role="menuitem"
										aria-current={isActivePath(pathname, profileHref) ? 'page' : undefined}
										onclick={closeUserMenu}
										class="flex items-center px-4 py-2 text-sm text-slate-300 transition-colors hover:bg-slate-700 hover:text-white"
									>
										Your Profile
									</a>
									<a
										href="/settings"
										role="menuitem"
										aria-current={isActivePath(pathname, '/settings') ? 'page' : undefined}
										onclick={closeUserMenu}
										class="flex items-center px-4 py-2 text-sm text-slate-300 transition-colors hover:bg-slate-700 hover:text-white"
									>
										Settings
									</a>
									{#if isCommissioner}
										<a
											href="/admin/members"
											role="menuitem"
											aria-current={isActivePath(pathname, '/admin/members') ? 'page' : undefined}
											onclick={closeUserMenu}
											class="flex items-center px-4 py-2 text-sm text-amber-300 transition-colors hover:bg-slate-700 hover:text-amber-200"
										>
											Commissioner
										</a>
									{/if}
									<form method="post" action="/logout" use:enhance={handleLogout} role="none">
										<button
											type="submit"
											role="menuitem"
											class="flex w-full items-center px-4 py-2 text-left text-sm text-slate-300 transition-colors hover:bg-slate-700 hover:text-white"
										>
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
							class="flex items-center rounded-md px-4 py-2 text-sm font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
						>
							Sign In
						</a>
					{/if}

					<!-- Mobile menu button -->
					<div class="lg:hidden">
						<button
							type="button"
							onclick={toggleMobileMenu}
							class="rounded-md p-2 text-slate-300 transition-colors hover:text-white"
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
		<div class="relative z-50 lg:hidden">
			<!-- Backdrop -->
			<div
				class="fixed inset-0 bg-black/50 backdrop-blur-sm"
				onclick={closeMobileMenu}
				onkeydown={(e) => {
					if (e.key === 'Enter' || e.key === ' ') closeMobileMenu();
				}}
				role="button"
				tabindex="-1"
				aria-label="Close mobile menu"
			></div>

			<!-- Mobile Menu Panel -->
			<div class="fixed top-0 left-0 w-full border-b border-slate-700 bg-slate-900 shadow-xl">
				<!-- Mobile Navigation Links -->
				<nav class="max-h-screen space-y-2 overflow-y-auto px-4 py-6">
					<!-- The drawer has the vertical room the bar doesn't, so the groups
					     open flat under their headings rather than as nested menus. -->
					{#each directLinks as item (item.href)}
						<a
							href={item.href}
							onclick={closeMobileMenu}
							class="flex items-center rounded-md px-4 py-3 text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
							class:bg-slate-800={isActivePath(pathname, item.href)}
							class:text-white={isActivePath(pathname, item.href)}
						>
							{item.name}
						</a>
					{/each}

					{#each navGroups as group (group.label)}
						<div class="pt-3">
							<p class="px-4 pb-1 text-xs font-semibold tracking-wide text-slate-500 uppercase">
								{group.label}
							</p>
							{#each group.items as item (item.href)}
								<a
									href={item.href}
									onclick={closeMobileMenu}
									class="flex items-center rounded-md px-4 py-3 text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
									class:bg-slate-800={isActivePath(pathname, item.href)}
									class:text-white={isActivePath(pathname, item.href)}
								>
									{item.name}
								</a>
							{/each}
						</div>
					{/each}

					<!-- Auth links for mobile -->
					{#if !signedIn}
						<div class="mt-4 border-t border-slate-700 pt-4">
							<a
								href="/login"
								onclick={closeMobileMenu}
								class="flex items-center rounded-md px-4 py-3 text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
							>
								Sign In
							</a>
						</div>
					{:else}
						<div class="mt-4 border-t border-slate-700 pt-4">
							<a
								href={profileHref}
								onclick={closeMobileMenu}
								class="flex items-center rounded-md px-4 py-3 text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
							>
								Your Profile
							</a>
							<a
								href="/settings"
								onclick={closeMobileMenu}
								class="flex items-center rounded-md px-4 py-3 text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
							>
								Settings
							</a>
							{#if isCommissioner}
								<a
									href="/admin/members"
									onclick={closeMobileMenu}
									class="flex items-center rounded-md px-4 py-3 text-base font-medium text-amber-300 transition-colors hover:bg-slate-800 hover:text-amber-200"
								>
									Commissioner
								</a>
							{/if}
							<form method="post" action="/logout" use:enhance={handleLogout}>
								<button
									type="submit"
									onclick={closeMobileMenu}
									class="flex w-full items-center rounded-md px-4 py-3 text-left text-base font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
								>
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
	<footer class="mt-20 border-t border-slate-700/50 bg-slate-900/50">
		<div class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
			<div class="text-center text-slate-400">
				<p>
					&copy; {new Date().getFullYear()} The League. Where legends are made and dreams are crushed.
				</p>
			</div>
		</div>
	</footer>
</div>
