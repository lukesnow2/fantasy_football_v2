<script lang="ts">
	import '../app.css';
	import { tick } from 'svelte';
	import { page } from '$app/state';
	import { afterNavigate } from '$app/navigation';
	import { enhance } from '$app/forms';
	import { dev } from '$app/environment';
	import { injectAnalytics } from '@vercel/analytics/sveltekit';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { directLinks, navGroups, isActivePath } from '$lib/nav';
	import { focusFirstMenuItem, moveMenuFocus } from '$lib/menu-focus';
	import { Avatar } from '$lib/components/ui';
	import NavDropdown from '$lib/components/NavDropdown.svelte';
	import type { LayoutData } from './$types';
	import type { SubmitFunction } from '@sveltejs/kit';

	let { data, children }: { data: LayoutData; children: any } = $props();

	// Vercel Web Analytics. In dev it logs page views to the console instead of sending them.
	injectAnalytics({ mode: dev ? 'development' : 'production' });

	/**
	 * Routes that draw their own full-bleed band and so take no content column
	 * from <main> (WP2, S2.4). Home's hero runs edge to edge (WP3).
	 */
	const FULL_BLEED_ROUTES = new Set(['/']);

	/**
	 * Routes whose boards draw <main> 28px from the band, not 40px: the manager
	 * profile, on every tab (WP6 S6.4; build spec F1). Matched on the route id,
	 * so every manager's URL counts. Phones keep the 20px every page has.
	 */
	const COMPACT_TOP_ROUTES = new Set(['/managers/[slug]']);

	let showUserMenu = $state(false);
	let showMobileMenu = $state(false);
	// One open menu at a time, held here rather than in each dropdown, otherwise
	// clicking a second trigger leaves two panels overlapping.
	let openNavMenu = $state<string | null>(null);

	let userButton = $state<HTMLButtonElement | null>(null);
	let userMenu = $state<HTMLDivElement | null>(null);
	let menuButton = $state<HTMLButtonElement | null>(null);

	const pathname = $derived(page.url.pathname);
	const fullBleed = $derived(FULL_BLEED_ROUTES.has(pathname));
	const compactTop = $derived(COMPACT_TOP_ROUTES.has(page.route?.id ?? ''));
	const isCommissioner = $derived(data.member?.role === 'commissioner');
	// The dropdown links to the manager's public stats page. Encoded because every
	// name in the roster has a space in it ("Gabe the Younger", "Troy Colvin").
	const profileHref = $derived(
		data.authenticatedManager
			? `/managers/${encodeURIComponent(data.authenticatedManager.managerName)}`
			: '/managers'
	);
	// Which account item is the page you're on, worked out once per item.
	const profileCurrent = $derived(isActivePath(pathname, profileHref));
	const settingsCurrent = $derived(isActivePath(pathname, '/settings'));
	const adminCurrent = $derived(isActivePath(pathname, '/admin/members'));

	// Any navigation closes every menu, including one a link didn't close itself
	// (the back button, a link in the page behind an open menu).
	afterNavigate(() => {
		openNavMenu = null;
		showUserMenu = false;
		showMobileMenu = false;
	});

	// The phone panel covers the page, so the page behind it doesn't scroll.
	$effect(() => {
		if (!showMobileMenu) return;
		const root = document.documentElement;
		const before = root.style.overflow;
		root.style.overflow = 'hidden';
		return () => {
			root.style.overflow = before;
		};
	});

	// The panel only exists below 1024px. Widen the window past that with it open
	// and it closes, rather than leaving the page behind it inert.
	$effect(() => {
		const wide = window.matchMedia('(min-width: 1024px)');
		const onChange = () => {
			if (wide.matches) showMobileMenu = false;
		};
		wide.addEventListener('change', onChange);
		return () => wide.removeEventListener('change', onChange);
	});

	async function toggleUserMenu(event: MouseEvent) {
		showUserMenu = !showUserMenu;
		if (!showUserMenu) return;
		openNavMenu = null;
		// From the keyboard (a click with no pointer behind it), focus the first
		// item, as the nav menus do.
		if (event.detail === 0) {
			await tick();
			focusFirstMenuItem(userMenu);
		}
	}

	function closeUserMenu() {
		showUserMenu = false;
	}

	function onUserMenuKeydown(event: KeyboardEvent) {
		moveMenuFocus(userMenu, event);
	}

	function onUserMenuFocusout(event: FocusEvent) {
		const to = event.relatedTarget as Node | null;
		if (to && !userMenu?.contains(to) && !userButton?.contains(to)) closeUserMenu();
	}

	function setNavMenu(label: string | null) {
		openNavMenu = label;
		if (label !== null) showUserMenu = false;
	}

	async function openMobileMenu() {
		openNavMenu = null;
		showUserMenu = false;
		showMobileMenu = true;
		// The band's row stays on top as the panel's first row, so its button,
		// now "Close menu", is the close button. Safari doesn't focus a clicked
		// button, so focus it here.
		await tick();
		menuButton?.focus();
	}

	function closeMobileMenu() {
		showMobileMenu = false;
	}

	/** Close the panel and hand focus back to the band's menu button. */
	async function dismissMobileMenu() {
		showMobileMenu = false;
		await tick();
		menuButton?.focus();
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key !== 'Escape') return;
		if (showMobileMenu) dismissMobileMenu();
		if (openNavMenu) openNavMenu = null;
		if (showUserMenu) {
			closeUserMenu();
			userButton?.focus();
		}
	}

	const handleLogout: SubmitFunction = () => {
		// Close the menus at once and let SvelteKit handle the redirect.
		closeUserMenu();
		closeMobileMenu();
	};
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="shell">
	<header class="band" class:menu-open={showMobileMenu}>
		<div class="band-row">
			<!-- With the phone panel open this row is the panel's top row, so the
			     brand closes it too, even when you're already on Home. -->
			<a href="/" class="brand" onclick={closeMobileMenu}>
				<span class="brand-name">The League</span>
				<span class="brand-est">Est. 2005</span>
			</a>

			<nav aria-label="Main" class="band-nav">
				{#each directLinks as item (item.href)}
					{@const active = isActivePath(pathname, item.href)}
					<a
						href={item.href}
						class="band-link"
						class:on={active}
						aria-current={active ? 'page' : undefined}>{item.name}</a
					>
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

			<div class="band-account">
				{#if data.user && data.authenticatedManager}
					<button
						bind:this={userButton}
						type="button"
						class="pill"
						class:open={showUserMenu}
						onclick={toggleUserMenu}
						aria-expanded={showUserMenu}
						aria-haspopup="menu"
					>
						<Avatar name={data.authenticatedManager.managerName} size={30} />
						<span
							>{data.authenticatedManager.displayName}<span aria-hidden="true"
								>&nbsp;{showUserMenu ? '▴' : '▾'}</span
							></span
						>
					</button>

					{#if showUserMenu}
						<div
							bind:this={userMenu}
							class="account-menu"
							role="menu"
							aria-label="Account"
							tabindex="-1"
							use:clickOutside={{ onOutside: closeUserMenu, ignore: [userButton] }}
							onkeydown={onUserMenuKeydown}
							onfocusout={onUserMenuFocusout}
						>
							<div class="signed-in" role="none">
								<div class="signed-in-label">Signed in as</div>
								<div class="signed-in-name">{data.authenticatedManager.displayName}</div>
							</div>
							<a
								href={profileHref}
								class="account-item"
								class:current={profileCurrent}
								role="menuitem"
								aria-current={profileCurrent ? 'page' : undefined}
								onclick={closeUserMenu}>Your profile</a
							>
							<a
								href="/settings"
								class="account-item"
								class:current={settingsCurrent}
								role="menuitem"
								aria-current={settingsCurrent ? 'page' : undefined}
								onclick={closeUserMenu}>Settings</a
							>
							{#if isCommissioner}
								<a
									href="/admin/members"
									class="account-item"
									class:current={adminCurrent}
									role="menuitem"
									aria-current={adminCurrent ? 'page' : undefined}
									onclick={closeUserMenu}>Commissioner</a
								>
							{/if}
							<form method="post" action="/logout" use:enhance={handleLogout} role="none">
								<button type="submit" class="account-item sign-out" role="menuitem">Sign out</button
								>
							</form>
						</div>
					{/if}
				{:else}
					<a href="/login" class="sign-in">Sign in</a>
				{/if}
			</div>

			<button
				bind:this={menuButton}
				type="button"
				class="menu-button"
				aria-label={showMobileMenu ? 'Close menu' : 'Open menu'}
				aria-expanded={showMobileMenu}
				aria-controls="site-menu"
				onclick={() => (showMobileMenu ? dismissMobileMenu() : openMobileMenu())}
			>
				{#if showMobileMenu}
					<svg
						width="20"
						height="20"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
						stroke-linecap="round"
						aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"></path></svg
					>
				{:else}
					<svg
						width="20"
						height="20"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
						stroke-linecap="round"
						aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16"></path></svg
					>
				{/if}
			</button>
		</div>
	</header>

	<!-- The phone menu: a full-screen panel over the page, so no backdrop. The
	     band's row stays above it as its top row (see .menu-open). -->
	<div id="site-menu" class="phone-panel" hidden={!showMobileMenu}>
		<nav aria-label="Main" class="phone-nav">
			<!-- The panel has the vertical room the bar doesn't, so the groups
			     open flat under their headings rather than as nested menus. -->
			{#each directLinks as item (item.href)}
				{@const active = isActivePath(pathname, item.href)}
				<a
					href={item.href}
					class="phone-link"
					class:on={active}
					aria-current={active ? 'page' : undefined}
					onclick={closeMobileMenu}>{item.name}</a
				>
			{/each}

			{#each navGroups as group (group.label)}
				<div class="phone-group">{group.label}</div>
				{#each group.items as item (item.href)}
					{@const active = isActivePath(pathname, item.href)}
					<a
						href={item.href}
						class="phone-item"
						class:on={active}
						aria-current={active ? 'page' : undefined}
						onclick={closeMobileMenu}>{item.name}</a
					>
				{/each}
			{/each}

			<div class="phone-account">
				{#if data.user && data.authenticatedManager}
					<Avatar name={data.authenticatedManager.managerName} size={28} />
					<span>
						<a href={profileHref} class="phone-account-link" onclick={closeMobileMenu}
							>Your profile</a
						>
						·
						<a href="/settings" class="phone-account-link" onclick={closeMobileMenu}>Settings</a>
						·
						{#if isCommissioner}
							<a href="/admin/members" class="phone-account-link" onclick={closeMobileMenu}
								>Commissioner</a
							>
							·
						{/if}
						<form
							method="post"
							action="/logout"
							use:enhance={handleLogout}
							class="phone-account-form"
						>
							<button type="submit" class="phone-account-link">Sign out</button>
						</form>
					</span>
				{:else}
					<a href="/login" class="phone-account-link" onclick={closeMobileMenu}>Sign in</a>
				{/if}
			</div>
		</nav>
	</div>

	<main
		class="content"
		class:full-bleed={fullBleed}
		class:compact-top={compactTop}
		inert={showMobileMenu}
	>
		{@render children()}
	</main>

	<footer class="foot" inert={showMobileMenu}>
		<div class="foot-row">
			<span class="foot-name">The League</span>
			<span>Est. 2005 · Data from Yahoo Fantasy</span>
		</div>
	</footer>
</div>

<style>
	/*
	 * The shell, from the Record Book boards (WP2): the header band on every page
	 * (This Season is the reference, Chat and Bet Board the signed-in pill,
	 * Settings the open account menu), the Navigation and mobile board for the
	 * menus and the phone, and the footer. Every value is the boards' own.
	 */

	.shell {
		display: flex;
		flex-direction: column;
		min-height: 100vh;
	}

	/* ---- The band ---- */

	.band {
		background: var(--color-clubhouse);
		color: var(--color-white);
	}

	.band-row {
		box-sizing: border-box;
		max-width: 1200px;
		margin: 0 auto;
		padding: 0 24px;
		min-height: 68px;
		display: flex;
		flex-wrap: wrap;
		gap: 8px 28px;
		align-items: center;
		justify-content: space-between;
	}

	.brand {
		display: inline-flex;
		align-items: baseline;
		gap: 10px;
		color: var(--color-white);
	}

	.brand:hover {
		color: var(--color-white);
	}

	.brand-name {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.brand-est {
		font-size: 12px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-on-clubhouse-2);
	}

	.band-nav {
		display: flex;
		flex-wrap: wrap;
		gap: 2px 4px;
		font-size: 15px;
		font-weight: 500;
	}

	/* Global so NavDropdown's trigger reads exactly like a direct link. */
	:global(.band-link) {
		display: block;
		padding: 22px 12px;
		border: 0;
		background: transparent;
		color: var(--color-on-clubhouse);
		font: inherit;
		cursor: pointer;
		white-space: nowrap;
	}

	:global(.band-link:hover) {
		color: var(--color-white);
		background: var(--color-clubhouse-3);
	}

	:global(.band-link.on) {
		padding-bottom: 19px;
		border-bottom: 3px solid var(--color-pennant-light);
		color: var(--color-white);
		font-weight: 600;
	}

	:global(.band-link:focus-visible) {
		outline: 2px solid var(--color-white);
		outline-offset: -2px;
	}

	.band-account {
		position: relative;
	}

	.sign-in {
		display: inline-block;
		padding: 10px 16px;
		border-radius: var(--radius-control);
		background: var(--color-card);
		color: var(--color-clubhouse);
		font-size: 15px;
		font-weight: 600;
	}

	.sign-in:hover {
		color: var(--color-clubhouse);
	}

	.pill {
		display: inline-flex;
		align-items: center;
		gap: 10px;
		padding: 6px 12px 6px 6px;
		border: 0;
		border-radius: 999px;
		background: var(--color-clubhouse-2);
		color: var(--color-white);
		font: inherit;
		font-size: 15px;
		font-weight: 600;
		cursor: pointer;
	}

	.pill:hover {
		background: var(--color-clubhouse-3);
	}

	.pill.open {
		box-shadow: inset 0 0 0 2px var(--color-pennant-light);
	}

	.pill:focus-visible,
	.sign-in:focus-visible,
	.brand:focus-visible,
	.menu-button:focus-visible {
		outline: 2px solid var(--color-white);
		outline-offset: 2px;
	}

	/* ---- The account menu ---- */

	.account-menu {
		position: absolute;
		/* The pill is centered in the 68px band; the board hangs the menu 8px
		   below the band's bottom edge, its right edge in line with the pill. */
		top: calc(50% + 34px + 8px);
		right: 0;
		z-index: 50;
		/* Content-box, as the Settings board draws it: 220px inside the padding. */
		box-sizing: content-box;
		width: 220px;
		padding: 6px;
		border-radius: var(--radius-card);
		background: var(--color-card);
		border: 1px solid var(--color-line);
		box-shadow: var(--shadow-popover);
		color: var(--color-ink);
	}

	.account-menu:focus {
		outline: none;
	}

	.signed-in {
		padding: 10px 12px 12px;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.signed-in-label {
		font-size: 13px;
		color: var(--color-muted);
	}

	.signed-in-name {
		font-weight: 700;
	}

	.account-item {
		display: block;
		box-sizing: border-box;
		width: 100%;
		padding: 11px 12px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		color: var(--color-ink);
		font: inherit;
		font-size: 15px;
		font-weight: 500;
		text-align: left;
		cursor: pointer;
	}

	/* Before the hover rule, so hover reads the same on every item. */
	.account-item.current {
		background: var(--color-field-tint);
		font-weight: 700;
	}

	.account-item:hover,
	.account-item:focus-visible {
		background: var(--color-field-wash);
		color: var(--color-ink);
		outline: none;
	}

	.sign-out {
		border-top: 1px solid var(--color-line-soft);
	}

	/* Just above the phone width, the longest name on the roster ("Gabe the
	   Younger") pushes the pill onto a second row. The tagline goes first; the
	   footer still says "Est. 2005". */
	@media (min-width: 1024px) and (max-width: 1079.98px) {
		.brand-est {
			display: none;
		}
	}

	/* ---- The phone ---- */

	.menu-button {
		display: none;
		flex: none;
		width: 44px;
		height: 44px;
		border: 0;
		border-radius: 10px;
		background: var(--color-clubhouse-2);
		color: var(--color-white);
		cursor: pointer;
		align-items: center;
		justify-content: center;
	}

	.phone-panel {
		display: none;
	}

	@media (max-width: 1023.98px) {
		.band-row {
			padding: 14px 16px;
		}

		/* A 44px tap target, as the phone boards draw the brand (WP3 S3.8). */
		.brand {
			align-items: center;
			min-height: 44px;
		}

		.brand-name {
			font-size: 22px;
		}

		.brand-est,
		.band-nav,
		.band-account {
			display: none;
		}

		.menu-button {
			display: inline-flex;
		}

		/* Open, the band's row is pinned over the panel as its first row, with
		   the board's 1px rule under it, and its button reads "Close menu". */
		.band.menu-open .band-row {
			position: fixed;
			top: 0;
			left: 0;
			right: 0;
			z-index: 61;
			background: var(--color-clubhouse);
			border-bottom: 1px solid var(--color-clubhouse-3);
		}

		.phone-panel {
			display: block;
			position: fixed;
			inset: 0;
			z-index: 60;
			/* Clear the pinned row: 14px + 44px + 14px, and its 1px rule. */
			padding-top: 73px;
			box-sizing: border-box;
			overflow-y: auto;
			overscroll-behavior: contain;
			background: var(--color-clubhouse);
			color: var(--color-white);
		}
	}

	.phone-nav {
		padding: 8px 16px;
	}

	.phone-link,
	.phone-item {
		display: block;
		color: var(--color-on-clubhouse);
		font-weight: 600;
	}

	.phone-link {
		padding: 13px 12px;
		font-size: 17px;
	}

	.phone-item {
		padding: 9px 12px;
		font-size: 16px;
	}

	.phone-link:hover,
	.phone-item:hover {
		color: var(--color-white);
	}

	.phone-link.on,
	.phone-item.on {
		border-radius: var(--radius-control);
		background: var(--color-clubhouse-2);
		color: var(--color-white);
		font-weight: 700;
		box-shadow: inset 3px 0 0 var(--color-pennant-light);
	}

	.phone-group {
		padding: 12px 12px 2px;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-on-clubhouse-2);
	}

	.phone-account {
		margin-top: 8px;
		padding: 10px 0 0 12px;
		border-top: 1px solid var(--color-clubhouse-3);
		display: flex;
		align-items: center;
		gap: 10px;
		color: var(--color-white);
		font-weight: 600;
	}

	.phone-account-form {
		display: contents;
	}

	/* Each word is its own target, at least 44px tall. */
	.phone-account-link {
		display: inline-flex;
		align-items: center;
		min-height: 44px;
		padding: 0;
		border: 0;
		background: transparent;
		color: var(--color-white);
		font: inherit;
		cursor: pointer;
	}

	.phone-account-link:hover {
		color: var(--color-white);
		text-decoration: underline;
	}

	.phone-link:focus-visible,
	.phone-item:focus-visible,
	.phone-account-link:focus-visible {
		outline: 2px solid var(--color-white);
		outline-offset: -2px;
	}

	/* ---- The page frame ---- */

	.content {
		flex: 1 0 auto;
		box-sizing: border-box;
		width: 100%;
		max-width: 1200px;
		margin: 0 auto;
		padding: 40px 24px 64px;
	}

	.content.full-bleed {
		max-width: none;
		padding: 0;
	}

	.content.compact-top {
		padding-top: 28px;
	}

	.foot {
		border-top: 1px solid var(--color-line);
		background: var(--color-card);
	}

	/* Content-box, unlike the band row: the boards' footer row has no
	   box-sizing, so its 1200px is the text column and the padding sits
	   outside it. */
	.foot-row {
		box-sizing: content-box;
		max-width: 1200px;
		margin: 0 auto;
		padding: 24px;
		display: flex;
		flex-wrap: wrap;
		gap: 8px 24px;
		justify-content: space-between;
		font-size: 14px;
		color: var(--color-muted);
	}

	.foot-name {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
		color: var(--color-clubhouse);
	}

	@media (max-width: 639.98px) {
		.content,
		.content.compact-top {
			padding: 20px 16px 48px;
		}

		.content.full-bleed {
			padding: 0;
		}

		.foot-row {
			padding: 24px 16px;
			flex-direction: column;
		}
	}
</style>
