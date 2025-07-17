<script lang="ts">
	/**
	 * Data Dictionary Page
	 * 
	 * Comprehensive glossary of all metrics and definitions
	 * Demonstrates the tooltip system and provides searchable reference
	 */
	
	import { onMount } from 'svelte';
	import { metricDefinitionsStore } from '$lib/stores/metricDefinitions.js';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import MetricTooltip from '$lib/components/MetricTooltip.svelte';
	
	let categories: any[] = [];
	let groupedMetrics: Record<string, any> = {};
	let searchTerm = '';
	let selectedCategory = 'all';
	let loading = true;
	let error: string | null = null;
	
	// Reactive filtering
	$: filteredCategories = categories.filter((category: any) => {
		if (selectedCategory !== 'all' && category.category_id !== selectedCategory) {
			return false;
		}
		
		if (searchTerm) {
			const searchLower = searchTerm.toLowerCase();
			return category.metrics.some((metric: any) => 
				metric.metric_name.toLowerCase().includes(searchLower) ||
				metric.short_description.toLowerCase().includes(searchLower) ||
				metric.metric_category.toLowerCase().includes(searchLower)
			);
		}
		
		return true;
	});
	
	onMount(async () => {
		try {
			// Fetch all categories and metrics
			const response = await fetch('/api/meta-data');
			
			if (!response.ok) {
				throw new Error(`Failed to fetch data dictionary: ${response.status}`);
			}
			
			const data = await response.json();
			
			if (data.error) {
				throw new Error(data.error);
			}
			
			categories = data.categories || [];
			
			// Also cache in store for tooltip usage
			categories.forEach((category: any) => {
				metricDefinitionsStore.setCategories(categories);
				category.metrics?.forEach((metric: any) => {
					metricDefinitionsStore.setMetric(metric.metric_id, { metric });
				});
			});
			
		} catch (err: any) {
			console.error('Error loading data dictionary:', err);
			error = err.message;
		} finally {
			loading = false;
		}
	});
	
	function getFilteredMetrics(categoryMetrics: any[]) {
		if (!searchTerm) return categoryMetrics;
		
		const searchLower = searchTerm.toLowerCase();
		return categoryMetrics.filter((metric: any) => 
			metric.metric_name.toLowerCase().includes(searchLower) ||
			metric.short_description.toLowerCase().includes(searchLower)
		);
	}
	
	function getCategoryIcon(iconName: string) {
		const icons: Record<string, string> = {
			'user': '👤',
			'users': '👥',
			'target': '🎯',
			'sword': '⚔️',
			'exchange': '🔄',
			'clipboard': '📋',
			'star': '⭐',
			'trophy': '🏆',
			'chart-line': '📈',
			'zap': '⚡',
			'book': '📚',
			'globe': '🌍'
		};
		return icons[iconName] || '📊';
	}
	
	function formatValue(value: any, format: string) {
		if (value === null || value === undefined) return 'N/A';
		
		switch (format) {
			case 'percentage_1':
				return `${(value * 100).toFixed(1)}%`;
			case 'percentage_2':
				return `${(value * 100).toFixed(2)}%`;
			case 'decimal_1':
				return value.toFixed(1);
			case 'decimal_2':
				return value.toFixed(2);
			case 'decimal_3':
				return value.toFixed(3);
			case 'integer':
				return Math.round(value).toString();
			case 'currency':
				return `$${value.toFixed(2)}`;
			default:
				return value.toString();
		}
	}
</script>

<svelte:head>
	<title>Data Dictionary | Fantasy Football Analytics</title>
	<meta name="description" content="Comprehensive glossary of all fantasy football metrics and their definitions" />
</svelte:head>

<div class="container mx-auto px-4 py-8 max-w-6xl">
	<!-- Header -->
	<div class="mb-8">
		<h1 class="text-4xl font-bold text-gray-900 mb-4">Data Dictionary</h1>
		<p class="text-lg text-gray-600 mb-6">
			Comprehensive definitions for all metrics and data points used in our fantasy football analytics.
			Hover over any metric name to see detailed information including calculations and interpretation guides.
		</p>
		
		<!-- Quick demonstration -->
		<div class="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
			<h3 class="font-semibold text-blue-900 mb-2">💡 How to Use Tooltips</h3>
			<p class="text-blue-800 mb-2">
				Throughout the application, you'll see metrics with help icons like this: 
				<MetricHelp metricId="hall_of_fame_index" position="right">Hall of Fame Index</MetricHelp>
			</p>
			<p class="text-blue-700 text-sm">
				Hover over the help icon or the metric name to see detailed definitions, calculations, and related metrics.
			</p>
		</div>
	</div>
	
	<!-- Search and Filter Controls -->
	<div class="mb-8 bg-white p-6 rounded-lg shadow-sm border border-gray-200">
		<div class="flex flex-col md:flex-row gap-4">
			<!-- Search -->
			<div class="flex-1">
				<label for="search" class="block text-sm font-medium text-gray-700 mb-2">
					Search Metrics
				</label>
				<input
					id="search"
					type="text"
					bind:value={searchTerm}
					placeholder="Search by name or description..."
					class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
				/>
			</div>
			
			<!-- Category Filter -->
			<div class="md:w-64">
				<label for="category" class="block text-sm font-medium text-gray-700 mb-2">
					Filter by Category
				</label>
				<select
					id="category"
					bind:value={selectedCategory}
					class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
				>
					<option value="all">All Categories</option>
					{#each categories as category}
						<option value={category.category_id}>
							{getCategoryIcon(category.icon_name)} {category.category_name}
						</option>
					{/each}
				</select>
			</div>
		</div>
		
		<!-- Results summary -->
		{#if !loading}
			<div class="mt-4 text-sm text-gray-600">
				{#if searchTerm || selectedCategory !== 'all'}
					Showing {filteredCategories.reduce((total, cat) => total + getFilteredMetrics(cat.metrics || []).length, 0)} metrics
				{:else}
					{categories.reduce((total, cat) => total + (cat.metrics?.length || 0), 0)} total metrics across {categories.length} categories
				{/if}
			</div>
		{/if}
	</div>
	
	<!-- Content -->
	{#if loading}
		<div class="flex items-center justify-center py-12">
			<div class="flex items-center space-x-3">
				<div class="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
				<span class="text-gray-600">Loading data dictionary...</span>
			</div>
		</div>
	{:else if error}
		<div class="bg-red-50 border border-red-200 rounded-lg p-6">
			<h3 class="font-semibold text-red-900 mb-2">Error Loading Data Dictionary</h3>
			<p class="text-red-700">{error}</p>
		</div>
	{:else if filteredCategories.length === 0}
		<div class="text-center py-12">
			<div class="text-gray-500 mb-4">
				<svg class="w-16 h-16 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
					<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.172 16.172a4 4 0 015.656 0M9 12h6m-6-4h6m2 5.291A7.962 7.962 0 0112 15c-2.34 0-4.5-.98-6.084-2.709M16.146 9.146a4.002 4.002 0 010 5.708m-10.292 0A4.002 4.002 0 015.146 9.146"/>
				</svg>
			</div>
			<h3 class="text-lg font-medium text-gray-900 mb-2">No metrics found</h3>
			<p class="text-gray-600">Try adjusting your search terms or category filter.</p>
		</div>
	{:else}
		<!-- Metrics by Category -->
		<div class="space-y-8">
			{#each filteredCategories as category}
				{@const filteredMetrics = getFilteredMetrics(category.metrics || [])}
				{#if filteredMetrics.length > 0}
					<div class="bg-white rounded-lg shadow-sm border border-gray-200">
						<!-- Category Header -->
						<div class="px-6 py-4 border-b border-gray-200">
							<div class="flex items-center space-x-3">
								<span class="text-2xl">{getCategoryIcon(category.icon_name)}</span>
								<div>
									<h2 class="text-xl font-bold text-gray-900">{category.category_name}</h2>
									{#if category.category_description}
										<p class="text-sm text-gray-600">{category.category_description}</p>
									{/if}
								</div>
								<div class="ml-auto">
									<span class="bg-gray-100 text-gray-700 px-2 py-1 rounded-full text-sm">
										{filteredMetrics.length} metric{filteredMetrics.length !== 1 ? 's' : ''}
									</span>
								</div>
							</div>
						</div>
						
						<!-- Metrics List -->
						<div class="divide-y divide-gray-100">
							{#each filteredMetrics as metric}
								<div class="px-6 py-4 hover:bg-gray-50 transition-colors">
									<div class="flex items-start justify-between">
										<div class="flex-1">
											<!-- Metric Name with Tooltip -->
											<div class="flex items-center space-x-2 mb-2">
												<MetricTooltip metricId={metric.metric_id} position="right" maxWidth="500px">
													<h3 class="text-lg font-semibold text-gray-900 cursor-help hover:text-blue-600 transition-colors">
														{metric.metric_name}
													</h3>
												</MetricTooltip>
											</div>
											
											<!-- Short Description -->
											<p class="text-gray-700 mb-3">{metric.short_description}</p>
											
											<!-- Metadata -->
											<div class="flex flex-wrap gap-4 text-sm text-gray-600">
												<div class="flex items-center space-x-1">
													<span class="font-medium">Type:</span>
													<span class="capitalize">{metric.data_type}</span>
												</div>
												
												{#if metric.unit_of_measure}
													<div class="flex items-center space-x-1">
														<span class="font-medium">Unit:</span>
														<span class="capitalize">{metric.unit_of_measure}</span>
													</div>
												{/if}
												
												{#if metric.typical_range}
													<div class="flex items-center space-x-1">
														<span class="font-medium">Range:</span>
														<span>{metric.typical_range}</span>
													</div>
												{/if}
												
												{#if metric.good_value_threshold}
													<div class="flex items-center space-x-1">
														<span class="font-medium">Good:</span>
														<span class="text-green-600">
															{formatValue(metric.good_value_threshold, metric.display_format)}+
														</span>
													</div>
												{/if}
												
												{#if metric.excellent_value_threshold}
													<div class="flex items-center space-x-1">
														<span class="font-medium">Excellent:</span>
														<span class="text-blue-600">
															{formatValue(metric.excellent_value_threshold, metric.display_format)}+
														</span>
													</div>
												{/if}
											</div>
										</div>
										
										<!-- Metric ID for developers -->
										<div class="ml-4 text-right">
											<code class="text-xs bg-gray-100 px-2 py-1 rounded text-gray-500">
												{metric.metric_id}
											</code>
										</div>
									</div>
								</div>
							{/each}
						</div>
					</div>
				{/if}
			{/each}
		</div>
	{/if}
</div>

<style>
	/* Ensure tooltips work properly in this page */
	:global(.container) {
		position: relative;
	}
</style> 