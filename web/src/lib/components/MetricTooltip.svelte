<script lang="ts">
	/**
	 * The old metric tooltip, kept so existing callers keep working. It draws the
	 * one StatTip card now (WP1, S1.6), with the same data path: definitions from
	 * /api/meta-data, cached in metricDefinitionsStore.
	 *
	 * `position`, `includeRelated`, `maxWidth` and `theme` belonged to the old
	 * card and do nothing. New code uses StatTip directly; the housekeeping PR
	 * after WP12 removes this file.
	 */
	import type { Snippet } from 'svelte';
	import StatTip from './ui/StatTip.svelte';

	interface Props {
		metricId: string;
		position?: 'top' | 'bottom' | 'left' | 'right';
		includeRelated?: boolean;
		maxWidth?: string;
		theme?: 'light' | 'dark';
		children?: Snippet;
	}

	let { metricId, children }: Props = $props();
</script>

<StatTip {metricId}>{@render children?.()}</StatTip>
