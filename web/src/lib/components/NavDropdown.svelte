<script lang="ts">
	import { tick } from 'svelte';
	import { page } from '$app/state';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { isActivePath, nextMenuIndex, type NavItem } from '$lib/nav';

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

	function menuItems(): HTMLElement[] {
		return [...(panel?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? [])];
	}

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
			menuItems()[0]?.focus();
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
		const links = menuItems();
		const next = nextMenuIndex(
			links.indexOf(document.activeElement as HTMLElement),
			links.length,
			event.key
		);
		if (next === null) return;
		event.preventDefault();
		links[next].focus();
	}

	// Tabbing out of the menu closes it. A null relatedTarget is a pointer press
	// somewhere that takes no focus, which clickOutside already handles.
	function onFocusout(event: FocusEvent) {
		if (!isOpen) return;
		const to = event.relatedTarget as Node | null;
		if (to && !root?.contains(to)) onToggle(null);
	}
</script>

<div class="relative" bind:this={root} onkeydown={onKeydown} onfocusout={onFocusout} role="none">
	<button
		bind:this={trigger}
		type="button"
		aria-expanded={isOpen}
		aria-haspopup="menu"
		onclick={onTriggerClick}
		class="flex items-center whitespace-nowrap rounded-md px-3 py-2 text-sm font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
		class:bg-slate-800={isActive || isOpen}
		class:text-white={isActive || isOpen}
	>
		{label}<span aria-hidden="true">&nbsp;{isOpen ? '▴' : '▾'}</span>
	</button>

	{#if isOpen}
		<div
			bind:this={panel}
			role="menu"
			aria-label={label}
			use:clickOutside={{ onOutside: () => onToggle(null), ignore: [trigger] }}
			class="absolute left-0 z-50 mt-2 w-72 origin-top-left rounded-lg border border-slate-600 bg-slate-800 py-1.5 shadow-xl ring-1 ring-slate-700"
		>
			{#each items as item (item.href)}
				{@const current = isActivePath(page.url.pathname, item.href)}
				<a
					href={item.href}
					role="menuitem"
					aria-current={current ? 'page' : undefined}
					onclick={() => onToggle(null)}
					class="flex items-start gap-3 px-3 py-2 transition-colors hover:bg-slate-700"
					class:bg-slate-700={current}
				>
					<span class="min-w-0">
						<span class="block text-sm font-medium text-white">{item.name}</span>
						<span class="block text-xs text-slate-400">{item.blurb}</span>
					</span>
				</a>
			{/each}
		</div>
	{/if}
</div>
