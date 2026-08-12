<script lang="ts">
	/**
	 * Data Dictionary Page
	 *
	 * Comprehensive glossary of all metrics and definitions.
	 *
	 * Each row expands to the full record the meta_data schema holds: what the
	 * metric measures, the formula the ETL actually runs, a worked example, how
	 * to read the number, and where it misleads. The short description alone was
	 * not enough to tell you whether a number meant anything.
	 */

	import { onMount } from 'svelte';
	import { BookOpen, ChevronDown } from 'lucide-svelte';
	import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
	import MetricHelp from '$lib/components/MetricHelp.svelte';

	let categories: any[] = [];
	let searchTerm = '';
	let selectedCategory = 'all';
	let loading = true;
	let error: string | null = null;
	let expanded = new Set<string>();

	// Reactive filtering
	$: filteredCategories = categories.filter((category: any) => {
		if (selectedCategory !== 'all' && category.categoryId !== selectedCategory) {
			return false;
		}

		if (searchTerm) {
			const searchLower = searchTerm.toLowerCase();
			return (category.metrics ?? []).some((metric: any) => matchesSearch(metric, searchLower));
		}

		return true;
	});

	// `?? ''` rather than a bare `.toLowerCase()`: a metric missing a field should
	// drop out of the results, not throw on the first keystroke.
	function matchesSearch(metric: any, searchLower: string) {
		return (
			(metric.metricName ?? '').toLowerCase().includes(searchLower) ||
			(metric.shortDescription ?? '').toLowerCase().includes(searchLower) ||
			(metric.metricCategory ?? '').toLowerCase().includes(searchLower)
		);
	}

	function toggle(metricId: string) {
		// Reassigned rather than mutated so Svelte sees the change.
		const next = new Set(expanded);
		next.has(metricId) ? next.delete(metricId) : next.add(metricId);
		expanded = next;
	}

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
			metricDefinitionsStore.setCategories(categories);
			categories.forEach((category: any) => {
				category.metrics?.forEach((metric: any) => {
					metricDefinitionsStore.setMetric(metric.metricId, { metric });
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
		return categoryMetrics.filter((metric: any) => matchesSearch(metric, searchLower));
	}

	function getCategoryIcon(iconName: string) {
		const icons: Record<string, string> = {
			user: '👤',
			users: '👥',
			target: '🎯',
			sword: '⚔️',
			exchange: '🔄',
			clipboard: '📋',
			star: '⭐',
			trophy: '🏆',
			'chart-line': '📈',
			zap: '⚡',
			book: '📚',
			globe: '🌍'
		};
		return icons[iconName] || '📊';
	}

	/**
	 * Standard deviations are stored with the excellent threshold *below* the good
	 * one, which is the only signal in the data that says lower is better. Reading
	 * it off the thresholds beats hardcoding a list of metric ids here.
	 */
	function lowerIsBetter(metric: any) {
		return (
			metric.goodValueThreshold != null &&
			metric.excellentValueThreshold != null &&
			metric.excellentValueThreshold < metric.goodValueThreshold
		);
	}

	function formatThreshold(value: any, format: string, lower: boolean) {
		return lower ? `≤${formatValue(value, format)}` : `${formatValue(value, format)}+`;
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
	<title>Data Dictionary | The League</title>
	<meta
		name="description"
		content="Comprehensive glossary of all fantasy football metrics and their definitions"
	/>
</svelte:head>

<div class="space-y-8">
	<!-- Hero -->
	<section class="text-center py-8">
		<div class="max-w-4xl mx-auto">
			<BookOpen class="h-16 w-16 text-blue-400 mx-auto mb-4" />
			<h1 class="text-4xl font-bold text-white mb-4">
				Data <span class="text-blue-400">Dictionary</span>
			</h1>
			<p class="text-xl text-slate-300 leading-relaxed">
				Every metric in this app: what it measures, the formula behind it, and where it misleads.
				Expand any metric for the full definition.
			</p>
		</div>
	</section>

	<!-- Tooltip demo -->
	<section class="bg-blue-500/10 border border-blue-500/30 rounded-xl p-5">
		<h2 class="font-semibold text-blue-200 mb-2">💡 Metrics are explained everywhere</h2>
		<p class="text-slate-300 mb-1">
			Throughout the app you'll see metrics with help icons like this:
			<MetricHelp metricId="hall_of_fame_index" position="right" theme="dark" className="text-blue-300"
				>Hall of Fame Index</MetricHelp
			>
		</p>
		<p class="text-slate-400 text-sm">
			Hover one to get the same definition you'll find on this page, without leaving what you were
			reading.
		</p>
	</section>

	<!-- Search and filter -->
	<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
		<div class="flex flex-col md:flex-row gap-4">
			<div class="flex-1">
				<label for="search" class="block text-sm font-medium text-slate-300 mb-2">
					Search Metrics
				</label>
				<input
					id="search"
					type="text"
					bind:value={searchTerm}
					placeholder="Search by name, description or category..."
					class="w-full bg-slate-700/50 border border-slate-600 rounded-lg px-3 py-2 text-white placeholder-slate-400 focus:outline-none focus:border-blue-400"
				/>
			</div>

			<div class="md:w-72">
				<label for="category" class="block text-sm font-medium text-slate-300 mb-2">
					Filter by Category
				</label>
				<select
					id="category"
					bind:value={selectedCategory}
					class="w-full bg-slate-700/50 border border-slate-600 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-blue-400"
				>
					<option value="all">All Categories</option>
					{#each categories as category}
						<option value={category.categoryId}>
							{getCategoryIcon(category.iconName)}
							{category.categoryName}
						</option>
					{/each}
				</select>
			</div>
		</div>

		{#if !loading}
			<div class="mt-4 text-sm text-slate-400">
				{#if searchTerm || selectedCategory !== 'all'}
					Showing {filteredCategories.reduce(
						(total, cat) => total + getFilteredMetrics(cat.metrics || []).length,
						0
					)} metrics
				{:else}
					{categories.reduce((total, cat) => total + (cat.metrics?.length || 0), 0)} total metrics across
					{categories.length} categories
				{/if}
			</div>
		{/if}
	</section>

	<!-- Content -->
	{#if loading}
		<div class="flex items-center justify-center h-64">
			<div class="text-slate-400">Loading data dictionary...</div>
		</div>
	{:else if error}
		<section class="bg-red-500/10 border border-red-500/30 rounded-xl p-6">
			<h2 class="font-semibold text-red-300 mb-2">Error Loading Data Dictionary</h2>
			<p class="text-red-200">{error}</p>
		</section>
	{:else if filteredCategories.length === 0}
		<div class="text-center py-12">
			<h2 class="text-lg font-medium text-white mb-2">No metrics found</h2>
			<p class="text-slate-400">Try adjusting your search terms or category filter.</p>
		</div>
	{:else}
		<div class="space-y-6">
			{#each filteredCategories as category}
				{@const filteredMetrics = getFilteredMetrics(category.metrics || [])}
				{#if filteredMetrics.length > 0}
					<section class="bg-slate-800/50 rounded-xl border border-slate-700/50 overflow-hidden">
						<!-- Category header -->
						<div class="px-6 py-4 border-b border-slate-700/50 flex items-center gap-3">
							<span class="text-2xl">{getCategoryIcon(category.iconName)}</span>
							<div>
								<h2 class="text-xl font-bold text-white">{category.categoryName}</h2>
								{#if category.categoryDescription}
									<p class="text-sm text-slate-400">{category.categoryDescription}</p>
								{/if}
							</div>
							<span
								class="ml-auto bg-slate-700/70 text-slate-300 px-2.5 py-1 rounded-full text-sm whitespace-nowrap"
							>
								{filteredMetrics.length} metric{filteredMetrics.length !== 1 ? 's' : ''}
							</span>
						</div>

						<!-- Metrics -->
						<div class="divide-y divide-slate-700/50">
							{#each filteredMetrics as metric}
								{@const isOpen = expanded.has(metric.metricId)}
								{@const lower = lowerIsBetter(metric)}
								<div>
									<button
										type="button"
										class="w-full text-left px-6 py-4 hover:bg-slate-700/30 transition-colors"
										aria-expanded={isOpen}
										on:click={() => toggle(metric.metricId)}
									>
										<div class="flex items-start gap-4">
											<ChevronDown
												class="h-5 w-5 text-slate-500 mt-1 shrink-0 transition-transform {isOpen
													? 'rotate-180'
													: ''}"
											/>
											<div class="flex-1 min-w-0">
												<div class="flex items-baseline gap-3 flex-wrap">
													<h3 class="text-lg font-semibold text-white">{metric.metricName}</h3>
													<code class="text-xs bg-slate-900/70 px-2 py-0.5 rounded text-slate-400">
														{metric.metricId}
													</code>
												</div>
												<p class="text-slate-300 mt-1">{metric.shortDescription}</p>

												<div class="flex flex-wrap gap-x-5 gap-y-1 text-sm text-slate-400 mt-2">
													<span><span class="text-slate-500">Type:</span> {metric.dataType}</span>
													{#if metric.unitOfMeasure}
														<span
															><span class="text-slate-500">Unit:</span>
															{metric.unitOfMeasure}</span
														>
													{/if}
													{#if metric.typicalRange}
														<span
															><span class="text-slate-500">Range:</span> {metric.typicalRange}</span
														>
													{/if}
													{#if metric.goodValueThreshold != null}
														<span>
															<span class="text-slate-500">Good:</span>
															<span class="text-green-400"
																>{formatThreshold(
																	metric.goodValueThreshold,
																	metric.displayFormat,
																	lower
																)}</span
															>
														</span>
													{/if}
													{#if metric.excellentValueThreshold != null}
														<span>
															<span class="text-slate-500">Excellent:</span>
															<span class="text-blue-400"
																>{formatThreshold(
																	metric.excellentValueThreshold,
																	metric.displayFormat,
																	lower
																)}</span
															>
														</span>
													{/if}
													{#if lower}
														<span class="text-amber-400/90">Lower is better</span>
													{/if}
												</div>
											</div>
										</div>
									</button>

									{#if isOpen}
										<div class="px-6 pb-5 pl-15 space-y-4">
											{#if metric.detailedDescription}
												<p class="text-slate-300 leading-relaxed">{metric.detailedDescription}</p>
											{/if}

											{#if metric.calculationFormula}
												<div class="bg-slate-900/60 border border-slate-700/50 rounded-lg p-4">
													<h4 class="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-2">
														Calculation
													</h4>
													<code class="text-sm text-slate-200 font-mono break-words">
														{metric.calculationFormula}
													</code>
													{#if metric.exampleCalculation}
														<p class="text-sm text-slate-400 mt-3 pt-3 border-t border-slate-700/50">
															<span class="text-slate-500">Worked example: </span>
															{metric.exampleCalculation}
														</p>
													{/if}
												</div>
											{/if}

											{#if metric.interpretationGuide}
												<div class="bg-green-500/5 border border-green-500/20 rounded-lg p-4">
													<h4 class="text-xs font-semibold uppercase tracking-wide text-green-400/80 mb-2">
														How to read it
													</h4>
													<p class="text-sm text-slate-300">{metric.interpretationGuide}</p>
												</div>
											{/if}

											{#if metric.limitations}
												<div class="bg-amber-500/5 border border-amber-500/20 rounded-lg p-4">
													<h4 class="text-xs font-semibold uppercase tracking-wide text-amber-400/80 mb-2">
														Limitations
													</h4>
													<p class="text-sm text-slate-300">{metric.limitations}</p>
												</div>
											{/if}
										</div>
									{/if}
								</div>
							{/each}
						</div>
					</section>
				{/if}
			{/each}
		</div>
	{/if}
</div>
