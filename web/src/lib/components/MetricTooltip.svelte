<script lang="ts">
	/**
	 * MetricTooltip Component
	 * 
	 * Displays comprehensive metric definitions and related information as a tooltip
	 * Fetches data from the meta-data API and caches results for performance
	 */
	
	import { onMount, createEventDispatcher } from 'svelte';
	import { fade, scale } from 'svelte/transition';
	import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
	
	export let metricId: string;
	export let position: 'top' | 'bottom' | 'left' | 'right' = 'top';
	export let includeRelated: boolean = true;
	export let maxWidth: string = '400px';
	export let theme: 'light' | 'dark' = 'light';
	
	const dispatch = createEventDispatcher();
	
	let visible = false;
	let tooltipElement: HTMLElement;
	let triggerElement: HTMLElement;
	let metricDefinition: any = null;
	let relatedMetrics: any[] = [];
	let loading = false;
	let error: string | null = null;
	
	// Reactive statement to fetch data when metricId changes
	$: if (metricId && visible) {
		fetchMetricDefinition();
	}
	
	async function fetchMetricDefinition() {
		if (!metricId) return;
		
		// Check cache first
		const cached = metricDefinitionsStore.getMetric(metricId);
		if (cached) {
			metricDefinition = cached.metric;
			relatedMetrics = cached.related_metrics || [];
			return;
		}
		
		loading = true;
		error = null;
		
		try {
			const response = await fetch(`/api/meta-data?metric_id=${encodeURIComponent(metricId)}&include_related=${includeRelated}`);
			
			if (!response.ok) {
				throw new Error(`Failed to fetch metric definition: ${response.status}`);
			}
			
			const data = await response.json();
			
			if (data.error) {
				throw new Error(data.error);
			}
			
			metricDefinition = data.metric;
			relatedMetrics = data.related_metrics || [];
			
			// Cache the result
			metricDefinitionsStore.setMetric(metricId, data);
			
		} catch (err) {
			console.error('Error fetching metric definition:', err);
			error = err instanceof Error ? err.message : 'Unknown error occurred';
		} finally {
			loading = false;
		}
	}
	
	function showTooltip() {
		visible = true;
		dispatch('show', { metricId });
	}
	
	function hideTooltip() {
		visible = false;
		dispatch('hide', { metricId });
	}
	
	function formatValue(value: any, format: string): string {
		if (value === null || value === undefined) return 'N/A';
		
		// Convert to number if it's not already
		const numValue = typeof value === 'number' ? value : parseFloat(value);
		
		// If conversion fails, return the string representation
		if (isNaN(numValue)) {
			return value.toString();
		}
		
		switch (format) {
			case 'percentage_1':
				return `${(numValue * 100).toFixed(1)}%`;
			case 'percentage_2':
				return `${(numValue * 100).toFixed(2)}%`;
			case 'decimal_1':
				return numValue.toFixed(1);
			case 'decimal_2':
				return numValue.toFixed(2);
			case 'decimal_3':
				return numValue.toFixed(3);
			case 'integer':
				return Math.round(numValue).toString();
			case 'currency':
				return `$${numValue.toFixed(2)}`;
			default:
				return numValue.toString();
		}
	}
	
	function getRelationshipIcon(type: string): string {
		switch (type) {
			case 'component_of':
				return '🧩';
			case 'derived_from':
				return '📊';
			case 'related_to':
				return '🔗';
			case 'inverse_of':
				return '↔️';
			default:
				return '•';
		}
	}
	
	function getPositionClasses(pos: string): string {
		const base = 'absolute z-50 ';
		switch (pos) {
			case 'top':
				return base + 'bottom-full left-1/2 transform -translate-x-1/2 mb-2';
			case 'bottom':
				return base + 'top-full left-1/2 transform -translate-x-1/2 mt-2';
			case 'left':
				return base + 'right-full top-1/2 transform -translate-y-1/2 mr-2';
			case 'right':
				return base + 'left-full top-1/2 transform -translate-y-1/2 ml-2';
			default:
				return base + 'bottom-full left-1/2 transform -translate-x-1/2 mb-2';
		}
	}
	
	// Position arrow based on tooltip position
	function getArrowClasses(pos: string): string {
		const base = 'absolute w-0 h-0 ';
		const borderSize = 'border-8 ';
		const darkBorder = theme === 'dark' ? 'border-slate-900' : 'border-white';
		
		switch (pos) {
			case 'top':
				return base + borderSize + `border-transparent border-t-8 ${darkBorder.replace('border-', 'border-t-')} top-full left-1/2 transform -translate-x-1/2`;
			case 'bottom':
				return base + borderSize + `border-transparent border-b-8 ${darkBorder.replace('border-', 'border-b-')} bottom-full left-1/2 transform -translate-x-1/2`;
			case 'left':
				return base + borderSize + `border-transparent border-l-8 ${darkBorder.replace('border-', 'border-l-')} left-full top-1/2 transform -translate-y-1/2`;
			case 'right':
				return base + borderSize + `border-transparent border-r-8 ${darkBorder.replace('border-', 'border-r-')} right-full top-1/2 transform -translate-y-1/2`;
			default:
				return base + borderSize + `border-transparent border-t-8 ${darkBorder.replace('border-', 'border-t-')} top-full left-1/2 transform -translate-x-1/2`;
		}
	}
</script>

<!-- Trigger element (slot content) -->
<span 
	bind:this={triggerElement}
	on:mouseenter={showTooltip}
	on:mouseleave={hideTooltip}
	on:focus={showTooltip}
	on:blur={hideTooltip}
	class="relative inline-block cursor-help"
	role="button"
	tabindex="0"
	aria-describedby={`tooltip-${metricId}`}
>
	<slot />
	
	<!-- Tooltip content -->
	{#if visible}
		<div
			bind:this={tooltipElement}
			id="tooltip-{metricId}"
			class="{getPositionClasses(position)} {theme === 'dark' ? 'bg-slate-900 text-white border-slate-600' : 'bg-white text-gray-900 border-gray-300'} rounded-lg shadow-xl border-2 p-4 backdrop-blur-sm"
			style="max-width: {maxWidth}; min-width: 250px;"
			transition:fade={{ duration: 200 }}
			role="tooltip"
			aria-live="polite"
		>
			<!-- Arrow -->
			<div class={getArrowClasses(position)}></div>
			
			{#if loading}
				<div class="flex items-center space-x-2">
					<div class="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
					<span class="text-sm">Loading definition...</span>
				</div>
			{:else if error}
				<div class="text-red-500 text-sm">
					<strong>Error:</strong> {error}
				</div>
			{:else if metricDefinition}
				<!-- Main definition -->
				<div class="space-y-3">
					<!-- Header -->
					<div class="border-b {theme === 'dark' ? 'border-slate-600' : 'border-gray-200'} pb-2">
						<h3 class="font-semibold text-lg {theme === 'dark' ? 'text-white' : 'text-gray-900'}">
							{metricDefinition.metricName}
						</h3>
						{#if metricDefinition.categoryName}
							<span class="inline-block {theme === 'dark' ? 'bg-blue-900/50 text-blue-200' : 'bg-blue-100 text-blue-800'} text-xs px-2 py-1 rounded-full mt-1">
								{metricDefinition.categoryName}
							</span>
						{/if}
					</div>
					
					<!-- Short description -->
					<div>
						<p class="text-sm {theme === 'dark' ? 'text-gray-300' : 'text-gray-700'}">
							{metricDefinition.shortDescription}
						</p>
					</div>

					<!-- Detailed description (if available) -->
					{#if metricDefinition.detailedDescription}
						<div class="border-t {theme === 'dark' ? 'border-slate-600' : 'border-gray-200'} pt-2">
							<p class="text-xs {theme === 'dark' ? 'text-slate-400' : 'text-gray-600'}">
								{metricDefinition.detailedDescription}
							</p>
						</div>
					{/if}
					
					<!-- Calculation formula (if available) -->
					{#if metricDefinition.calculationFormula}
						<div class="{theme === 'dark' ? 'bg-slate-800' : 'bg-gray-50'} rounded p-2">
							<h4 class="text-xs font-medium {theme === 'dark' ? 'text-slate-300' : 'text-gray-700'} mb-1">
								Calculation:
							</h4>
							<code class="text-xs {theme === 'dark' ? 'text-slate-300' : 'text-gray-600'} font-mono">
								{metricDefinition.calculationFormula}
							</code>
						</div>
					{/if}
					
					<!-- Example calculation (if available) -->
					{#if metricDefinition.exampleCalculation}
						<div class="{theme === 'dark' ? 'bg-blue-900/30' : 'bg-blue-50'} rounded p-2">
							<h4 class="text-xs font-medium {theme === 'dark' ? 'text-blue-300' : 'text-blue-700'} mb-1">
								Example:
							</h4>
							<p class="text-xs {theme === 'dark' ? 'text-blue-200' : 'text-blue-600'}">
								{metricDefinition.exampleCalculation}
							</p>
						</div>
					{/if}
					
					<!-- Interpretation guide (if available) -->
					{#if metricDefinition.interpretationGuide}
						<div class="{theme === 'dark' ? 'bg-green-900/30' : 'bg-green-50'} rounded p-2">
							<h4 class="text-xs font-medium {theme === 'dark' ? 'text-green-300' : 'text-green-700'} mb-1">
								How to interpret:
							</h4>
							<p class="text-xs {theme === 'dark' ? 'text-green-200' : 'text-green-600'}">
								{metricDefinition.interpretationGuide}
							</p>
						</div>
					{/if}
					
					<!-- Value ranges and thresholds -->
					{#if metricDefinition.typicalRange || metricDefinition.goodValueThreshold || metricDefinition.excellentValueThreshold}
						<div class="border-t {theme === 'dark' ? 'border-slate-600' : 'border-gray-200'} pt-2">
							<h4 class="text-xs font-medium {theme === 'dark' ? 'text-slate-300' : 'text-gray-700'} mb-1">
								Value Ranges:
							</h4>
							<div class="text-xs {theme === 'dark' ? 'text-slate-400' : 'text-gray-600'} space-y-1">
								{#if metricDefinition.typicalRange}
									<div>Typical: {metricDefinition.typicalRange}</div>
								{/if}
								{#if metricDefinition.goodValueThreshold}
									<div>Good: {formatValue(metricDefinition.goodValueThreshold, metricDefinition.displayFormat)}+</div>
								{/if}
								{#if metricDefinition.excellentValueThreshold}
									<div>Excellent: {formatValue(metricDefinition.excellentValueThreshold, metricDefinition.displayFormat)}+</div>
								{/if}
							</div>
						</div>
					{/if}
					
					<!-- Related metrics -->
					{#if includeRelated && relatedMetrics.length > 0}
						<div class="border-t {theme === 'dark' ? 'border-slate-600' : 'border-gray-200'} pt-2">
							<h4 class="text-xs font-medium {theme === 'dark' ? 'text-slate-300' : 'text-gray-700'} mb-2">
								Related Metrics:
							</h4>
							<div class="space-y-1">
								{#each relatedMetrics.slice(0, 3) as related}
									<div class="flex items-center space-x-2 text-xs {theme === 'dark' ? 'text-slate-400' : 'text-gray-600'}">
										<span class="text-sm">{getRelationshipIcon(related.relationshipType)}</span>
										<span class="font-medium">{related.relatedMetricName}</span>
										{#if related.relationshipDescription}
											<span class="{theme === 'dark' ? 'text-slate-500' : 'text-gray-500'}">- {related.relationshipDescription}</span>
										{/if}
									</div>
								{/each}
								{#if relatedMetrics.length > 3}
									<div class="text-xs {theme === 'dark' ? 'text-slate-500' : 'text-gray-500'} italic">
										+{relatedMetrics.length - 3} more related metrics
									</div>
								{/if}
							</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="text-gray-500 text-sm">
					No definition available for: {metricId}
				</div>
			{/if}
		</div>
	{/if}
</span>

<style>
	/* Custom styles for better tooltip appearance */
	:global(.metric-tooltip-container) {
		position: relative;
		display: inline-block;
	}
	
	/* Ensure tooltip appears above other content */
	:global(.metric-tooltip) {
		z-index: 9999;
	}
	
	/* Animation improvements */
	:global(.tooltip-fade-in) {
		animation: tooltipFadeIn 0.2s ease-in-out;
	}
	
	@keyframes tooltipFadeIn {
		from {
			opacity: 0;
			transform: scale(0.95);
		}
		to {
			opacity: 1;
			transform: scale(1);
		}
	}
</style> 