<script lang="ts">
	/**
	 * MetricHelp Component - Simple wrapper for adding metric tooltips
	 * 
	 * Usage:
	 * <MetricHelp metricId="hall_of_fame_index">Hall of Fame Index</MetricHelp>
	 * <MetricHelp metricId="career_win_percentage" position="bottom">Win %</MetricHelp>
	 */
	
	import MetricTooltip from './MetricTooltip.svelte';
	
	export let metricId: string;
	export let position: 'top' | 'bottom' | 'left' | 'right' = 'top';
	export let includeRelated: boolean = true;
	export let maxWidth: string = '400px';
	export let theme: 'light' | 'dark' = 'light';
	export let showIcon: boolean = true;
	export let iconOnly: boolean = false;
	export let className: string = '';
</script>

<MetricTooltip 
	{metricId} 
	{position} 
	{includeRelated} 
	{maxWidth} 
	{theme}
>
	<span class="inline-flex items-center gap-1 {className}">
		{#if !iconOnly}
			<slot />
		{/if}
		{#if showIcon}
			<svg 
				class="w-4 h-4 text-gray-400 hover:text-blue-500 transition-colors cursor-help" 
				fill="none" 
				stroke="currentColor" 
				viewBox="0 0 24 24"
				aria-hidden="true"
			>
				<path 
					stroke-linecap="round" 
					stroke-linejoin="round" 
					stroke-width="2" 
					d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
				/>
			</svg>
		{/if}
	</span>
</MetricTooltip>

<style>
	/* Ensure the help icon doesn't interfere with layout */
	:global(.metric-help-inline) {
		display: inline-flex;
		align-items: center;
		vertical-align: baseline;
	}
</style> 