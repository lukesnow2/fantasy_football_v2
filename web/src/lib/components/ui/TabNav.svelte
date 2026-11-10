<script lang="ts" module>
	export interface TabItem {
		id: string;
		label: string;
		href: string;
	}
</script>

<script lang="ts">
	/**
	 * A row of tabs that are links, so the tab lives in the URL (Manager
	 * profile boards; WP10's League History uses it too). A 1px line rule runs
	 * along the bottom, tabs sit 4px apart, each 12px 14px of padding in 15px
	 * weight 500 ink-2. The current tab has `aria-current="page"`, a 3px field
	 * bottom border, weight 700 and ink.
	 *
	 * `onselect` runs on a plain click (not one with a modifier key, which
	 * still opens the link where the reader asked), so a page can switch in
	 * place with shallow routing:
	 *
	 *   <TabNav label="Profile sections" tabs={tabs} current={tab} onselect={(id) => ...} />
	 *
	 * Below 640px the row never wraps and scrolls sideways with its scrollbar
	 * hidden, each tab 44px tall, and the current tab scrolls into view when
	 * the row first draws.
	 */
	import { onMount } from 'svelte';

	interface Props {
		tabs: TabItem[];
		current: string;
		label: string;
		onselect?: (id: string, event: MouseEvent) => void;
	}

	let { tabs, current, label, onselect }: Props = $props();

	let row: HTMLElement | undefined = $state();

	function onclick(id: string, event: MouseEvent) {
		if (!onselect) return;
		if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
			return;
		}
		event.preventDefault();
		onselect(id, event);
	}

	onMount(() => {
		// Only the row scrolls: `nearest` on the block axis leaves the page put.
		const active = row?.querySelector<HTMLElement>('[aria-current="page"]');
		if (active && row && row.scrollWidth > row.clientWidth) {
			active.scrollIntoView({ inline: 'nearest', block: 'nearest' });
		}
	});
</script>

<nav class="tabs" aria-label={label} bind:this={row}>
	{#each tabs as tab (tab.id)}
		<a
			href={tab.href}
			class="tab"
			class:current={tab.id === current}
			aria-current={tab.id === current ? 'page' : undefined}
			data-sveltekit-noscroll
			onclick={(event) => onclick(tab.id, event)}>{tab.label}</a
		>
	{/each}
</nav>

<style>
	.tabs {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		border-bottom: 1px solid var(--color-line);
	}

	.tab {
		padding: 12px 14px;
		font-size: 15px;
		font-weight: 500;
		color: var(--color-ink-2);
		text-decoration: none;
	}

	.tab:hover {
		color: var(--color-field);
	}

	.tab.current {
		padding-bottom: 10px;
		border-bottom: 3px solid var(--color-field);
		font-weight: 700;
		color: var(--color-ink);
	}

	.tab:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: -2px;
	}

	@media (max-width: 639.98px) {
		.tabs {
			flex-wrap: nowrap;
			overflow-x: auto;
			scrollbar-width: none;
		}

		.tabs::-webkit-scrollbar {
			display: none;
		}

		.tab {
			flex: none;
			line-height: 20px;
			white-space: nowrap;
		}

		.tab.current {
			padding-bottom: 9px;
		}
	}
</style>
