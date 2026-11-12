<script lang="ts">
	/**
	 * The old way to put a definition on a label, kept so existing callers keep
	 * working. It draws the one StatTip card now (WP1, S1.6).
	 *
	 *   <MetricHelp metricId="hall_of_fame_index">Hall of Fame Index</MetricHelp>
	 *
	 * `position`, `includeRelated`, `maxWidth` and `theme` belonged to the old
	 * card and do nothing: StatTip places itself and has one look. `showIcon`
	 * and `iconOnly` still work. New code uses StatTip directly.
	 */
	import type { Snippet } from 'svelte';
	import StatTip from './ui/StatTip.svelte';

	interface Props {
		metricId: string;
		position?: 'top' | 'bottom' | 'left' | 'right';
		includeRelated?: boolean;
		maxWidth?: string;
		theme?: 'light' | 'dark';
		showIcon?: boolean;
		iconOnly?: boolean;
		className?: string;
		children?: Snippet;
	}

	let { metricId, showIcon = true, iconOnly = false, className = '', children }: Props = $props();
</script>

<span class={className}
	><StatTip {metricId} icon={showIcon} {iconOnly}>{@render children?.()}</StatTip></span
>
