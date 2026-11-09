<script lang="ts">
	/**
	 * One group in the header band ("Rankings ▾") and the menu it opens, as the
	 * Navigation board draws them (WP2, S2.2). The layout holds which menu is
	 * open, so only one is open at a time.
	 */
	import { tick } from 'svelte';
	import { page } from '$app/state';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { isActivePath, type NavItem } from '$lib/nav';
	import { focusFirstMenuItem, moveMenuFocus } from '$lib/menu-focus';

	interface Props {
		label: string;
		items: NavItem[];
		/** Which menu the bar currently has open, null for none. */
		open: string | null;
		onToggle: (label: string | null) => void;
	}

	let { label, items, open, onToggle }: Props = $props();

	let root = $state<HTMLDivElement | null>(null);
	let trigger = $state<HTMLButtonElement | null>(null);
	let panel = $state<HTMLDivElement | null>(null);

	const isOpen = $derived(open === label);
	// A group reads as active when you are on any page inside it, so the bar still
	// tells you where you are once the page itself is a click deep.
	const isActive = $derived(items.some((item) => isActivePath(page.url.pathname, item.href)));

	async function onTriggerClick(event: MouseEvent) {
		if (isOpen) {
			onToggle(null);
			return;
		}
		onToggle(label);
		// Enter and Space fire a click with no pointer behind it (detail 0). From
		// the keyboard, focus goes to the first item so ↓ and ↑ work at once; a
		// mouse open leaves focus on the trigger.
		if (event.detail === 0) {
			await tick();
			focusFirstMenuItem(panel);
		}
	}

	function close() {
		onToggle(null);
		trigger?.focus();
	}

	function onKeydown(event: KeyboardEvent) {
		if (!isOpen) return;
		if (event.key === 'Escape') {
			event.stopPropagation();
			close();
			return;
		}
		moveMenuFocus(panel, event);
	}

	// Tabbing out of the menu closes it. A null relatedTarget is a pointer press
	// somewhere that takes no focus, which clickOutside already handles.
	function onFocusout(event: FocusEvent) {
		if (!isOpen) return;
		const to = event.relatedTarget as Node | null;
		if (to && !root?.contains(to)) onToggle(null);
	}
</script>

<div class="group" bind:this={root} onkeydown={onKeydown} onfocusout={onFocusout} role="none">
	<button
		bind:this={trigger}
		type="button"
		class="band-link"
		class:on={isActive || isOpen}
		aria-expanded={isOpen}
		aria-haspopup="menu"
		onclick={onTriggerClick}
	>
		{label}<span aria-hidden="true">&nbsp;{isOpen ? '▴' : '▾'}</span>
	</button>

	{#if isOpen}
		<div
			bind:this={panel}
			class="menu"
			role="menu"
			aria-label={label}
			use:clickOutside={{ onOutside: () => onToggle(null), ignore: [trigger] }}
		>
			{#each items as item (item.href)}
				{@const current = isActivePath(page.url.pathname, item.href)}
				<a
					href={item.href}
					class="item"
					class:current
					role="menuitem"
					aria-current={current ? 'page' : undefined}
					onclick={() => onToggle(null)}
				>
					<span class="bar" aria-hidden="true"></span>
					<span>
						<span class="name">{item.name}</span>
						<span class="blurb">{item.blurb}</span>
					</span>
				</a>
			{/each}
			<div class="hint" aria-hidden="true">↑ ↓ to move · Esc to close</div>
		</div>
	{/if}
</div>

<style>
	.group {
		position: relative;
	}

	/* The trigger shares the band link's look with the direct links; the
	   layout's .band-link rule is global so both read the same. */

	.menu {
		position: absolute;
		/* The group sits centered in the 68px band, so its middle is 34px above
		   the band's bottom edge. The board puts the menu 4px below that edge. */
		top: calc(50% + 34px + 4px);
		left: 0;
		z-index: 50;
		/* Content-box, as the board draws it: 340px of items inside the padding. */
		box-sizing: content-box;
		width: 340px;
		padding: 8px;
		border-radius: var(--radius-card);
		background: var(--color-card);
		border: 1px solid var(--color-line);
		box-shadow: var(--shadow-popover);
		color: var(--color-ink);
		/* The menu sits inside the nav, which sets 15px weight 500 for the band
		   links; the board's menu is outside it and reads at the page's 16px 400. */
		font-size: 16px;
		font-weight: 400;
	}

	.item {
		display: flex;
		gap: 12px;
		padding: 12px;
		border-radius: var(--radius-control);
		color: var(--color-ink);
	}

	/* Before the hover rule, so hovering the current page's item turns it
	   field-wash with a clear bar like every other item. */
	.item.current {
		background: var(--color-field-tint);
	}

	.item:hover,
	.item:focus-visible {
		background: var(--color-field-wash);
		color: var(--color-ink);
		outline: none;
	}

	.bar {
		flex: none;
		width: 4px;
		border-radius: 2px;
		background: transparent;
	}

	.item.current .bar {
		background: var(--color-field);
	}

	.item.current:hover .bar,
	.item.current:focus-visible .bar {
		background: transparent;
	}

	.name {
		display: block;
		font-size: 15px;
		font-weight: 700;
	}

	.blurb {
		display: block;
		margin-top: 2px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.hint {
		padding: 8px 12px 4px;
		font-size: 12px;
		color: var(--color-muted);
	}
</style>
