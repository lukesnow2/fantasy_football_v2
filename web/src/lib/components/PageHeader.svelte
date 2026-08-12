<script lang="ts">
	/**
	 * The standard page header for every top-level route.
	 *
	 * The convention is the Trade Center's: a centred accent icon, a 4xl white
	 * title, and one xl slate line of context capped at max-w-3xl. Before this
	 * existed each page hand-rolled its own header and they drifted into four
	 * different treatments — left-aligned rows, 5xl titles, missing icons, and
	 * on /managers no header at all.
	 *
	 * Put the header in a component rather than in a style guide because a
	 * convention nobody can accidentally deviate from is the only kind that
	 * holds.
	 *
	 *   <PageHeader icon={Target} title="Trade Center" accent="amber">
	 *       Complete trade analysis and history.
	 *   </PageHeader>
	 */
	import type { ComponentType } from 'svelte';

	export let title: string;
	/** A lucide-svelte icon component. Omit for a title-only header. */
	export let icon: ComponentType | null = null;
	export let accent: 'amber' | 'blue' | 'green' | 'purple' | 'cyan' | 'red' = 'amber';

	// Written out in full because Tailwind scans source text: an interpolated
	// `text-${accent}-400` produces a class that never gets generated.
	const accentClass: Record<string, string> = {
		amber: 'text-amber-400',
		blue: 'text-blue-400',
		green: 'text-green-400',
		purple: 'text-purple-400',
		cyan: 'text-cyan-400',
		red: 'text-red-400'
	};
</script>

<div class="text-center py-8">
	{#if icon}
		<svelte:component this={icon} class="h-16 w-16 {accentClass[accent]} mx-auto mb-4" />
	{/if}
	<h1 class="text-4xl font-bold text-white mb-4">{title}</h1>
	{#if $$slots.default}
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			<slot />
		</p>
	{/if}
</div>
