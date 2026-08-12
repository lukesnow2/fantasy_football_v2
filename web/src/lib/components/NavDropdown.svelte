<script lang="ts">
	import { page } from '$app/state';
	import { ChevronDown, type Icon as IconType } from 'lucide-svelte';
	import { clickOutside } from '$lib/actions/clickOutside';

	export interface NavItem {
		name: string;
		href: string;
		icon: typeof IconType;
		/** One line on what the page is for. The whole point of grouping is that
		 *  the group name plus this line answers "is what I want in here?". */
		blurb: string;
	}

	interface Props {
		label: string;
		icon: typeof IconType;
		items: NavItem[];
		/** Which menu the bar currently has open — null for none. */
		open: string | null;
		onToggle: (label: string | null) => void;
	}

	let { label, icon: Icon, items, open, onToggle }: Props = $props();

	let trigger = $state<HTMLButtonElement | null>(null);
	let panel = $state<HTMLDivElement | null>(null);

	const isOpen = $derived(open === label);
	// A group reads as active when you are on any page inside it, so the bar still
	// tells you where you are once the page itself is a click deep.
	const isActive = $derived(items.some((item) => page.url.pathname === item.href));

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
		if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
			event.preventDefault();
			const links = [...(panel?.querySelectorAll('a') ?? [])];
			if (links.length === 0) return;
			const current = links.indexOf(document.activeElement as HTMLAnchorElement);
			const next =
				event.key === 'ArrowDown'
					? (current + 1) % links.length
					: (current - 1 + links.length) % links.length;
			links[next].focus();
		}
	}
</script>

<div class="relative" onkeydown={onKeydown} role="none">
	<button
		bind:this={trigger}
		type="button"
		aria-expanded={isOpen}
		aria-haspopup="true"
		onclick={() => onToggle(isOpen ? null : label)}
		class="flex items-center whitespace-nowrap rounded-md px-3 py-2 text-sm font-medium text-slate-300 transition-colors hover:bg-slate-800 hover:text-white"
		class:bg-slate-800={isActive || isOpen}
		class:text-white={isActive || isOpen}
	>
		<Icon class="mr-2 h-4 w-4" />
		{label}
		<!-- Interpolated rather than a `class:` directive — that directive is only
		     valid on elements, and ChevronDown is a component. -->
		<ChevronDown class="ml-1 h-4 w-4 transition-transform {isOpen ? 'rotate-180' : ''}" />
	</button>

	{#if isOpen}
		<div
			bind:this={panel}
			use:clickOutside={{ onOutside: () => onToggle(null), ignore: [trigger] }}
			class="absolute left-0 z-50 mt-2 w-72 origin-top-left rounded-lg border border-slate-600 bg-slate-800 py-1.5 shadow-xl ring-1 ring-slate-700"
		>
			{#each items as item (item.href)}
				{@const ItemIcon = item.icon}
				<a
					href={item.href}
					onclick={() => onToggle(null)}
					class="flex items-start gap-3 px-3 py-2 transition-colors hover:bg-slate-700"
					class:bg-slate-700={page.url.pathname === item.href}
				>
					<ItemIcon class="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
					<span class="min-w-0">
						<span class="block text-sm font-medium text-white">{item.name}</span>
						<span class="block text-xs text-slate-400">{item.blurb}</span>
					</span>
				</a>
			{/each}
		</div>
	{/if}
</div>
