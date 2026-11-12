<script lang="ts">
	/**
	 * Data Dictionary Page
	 *
	 * Every metric the app shows, grouped by category. Each entry opens to the
	 * full record the meta_data schema holds: its type and thresholds, the
	 * formula the ETL actually runs, a worked example, how to read the number,
	 * and where it misleads. The short description alone was not enough to tell
	 * you whether a number meant anything.
	 *
	 * /data-dictionary#{metricId} opens that entry, which is where every
	 * StatTip's "Full definition in the Data Dictionary →" lands.
	 */
	import { onMount, tick } from 'svelte';
	import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { Button, Card, StatTip, TextField } from '$lib/components/ui';
	import {
		VISIBLE_CATEGORY_CHIPS,
		categoryBlurb,
		categoryChips,
		categoryLabel,
		dictionarySummary,
		factTiles,
		filterMetrics,
		type DictionaryCategory
	} from '$lib/metrics/dictionary';

	let categories: DictionaryCategory[] = [];
	let searchTerm = '';
	let selectedCategory = 'all';
	let loading = true;
	let error: string | null = null;
	let expanded = new Set<string>();
	let showAllChips = false;

	$: filtered = filterMetrics(categories, searchTerm, selectedCategory);
	$: chips = categoryChips(categories);
	// A category picked from the select that sits past the seventh chip shows
	// every chip, so the pressed one is on screen.
	$: chipsOpen =
		showAllChips ||
		chips.findIndex((c) => c.value === selectedCategory) > VISIBLE_CATEGORY_CHIPS;
	$: visibleChips = chipsOpen ? chips : chips.slice(0, VISIBLE_CATEGORY_CHIPS + 1);
	$: hiddenChips = chips.length - visibleChips.length;
	$: categoryOptions = [
		{ value: 'all', label: 'All categories' },
		...categories.map((c) => ({ value: c.categoryId, label: categoryLabel(c.categoryId, c.categoryName) }))
	];

	let chipRow: HTMLDivElement | undefined;

	/** "+ n more" goes away when pressed, so focus moves to the first chip it revealed. */
	async function revealChips() {
		showAllChips = true;
		await tick();
		chipRow?.querySelectorAll<HTMLElement>('.btn')[VISIBLE_CATEGORY_CHIPS + 1]?.focus();
	}

	function toggle(metricId: string) {
		// Reassigned rather than mutated so Svelte sees the change.
		const next = new Set(expanded);
		next.has(metricId) ? next.delete(metricId) : next.add(metricId);
		expanded = next;
	}

	/** A hash naming a metric clears the filters, opens it and moves to it. */
	async function openFromHash() {
		let id = '';
		try {
			id = decodeURIComponent(window.location.hash.slice(1));
		} catch {
			return; // A malformed escape names no metric.
		}
		if (!id) return;
		const known = categories.some((c) => (c.metrics ?? []).some((m) => m.metricId === id));
		if (!known) return;
		searchTerm = '';
		selectedCategory = 'all';
		expanded = new Set([...expanded, id]);
		await tick();
		const entry = document.getElementById(id);
		entry?.scrollIntoView({ block: 'start' });
		entry?.querySelector<HTMLElement>('h3')?.focus({ preventScroll: true });
	}

	async function load() {
		loading = true;
		error = null;
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
			metricDefinitionsStore.setCategories(categories as any);
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
		if (!error) await openFromHash();
	}

	onMount(() => {
		load();
		const onHash = () => openFromHash();
		window.addEventListener('hashchange', onHash);
		return () => window.removeEventListener('hashchange', onHash);
	});
</script>

<svelte:head>
	<title>Data Dictionary | The League</title>
	<meta
		name="description"
		content="Comprehensive glossary of all fantasy football metrics and their definitions"
	/>
</svelte:head>

<PageHeader
	kicker={loading ? 'League' : dictionarySummary(categories)}
	title="Data Dictionary"
	introWidth={680}
	phoneKickerLineHeight={1.4}
>
	Every metric in this app: what it measures, the formula behind it, and where it misleads. The
	little ⓘ next to a stat anywhere on the site opens the same definition.
</PageHeader>

<div class="tip">
	<span class="tip-lead">Metrics are explained everywhere.</span>
	<span class="tip-text"
		><span class="hover-copy">Hover</span><span class="touch-copy">Tap</span> the ⓘ next to any stat, like
		<StatTip metricId="hall_of_fame_index" label="Hall of Fame Index" chip />, to get the same
		definition you'll find on this page.</span
	>
</div>

<div class="filters">
	<TextField
		label="Search"
		type="search"
		size="lg"
		placeholder="Search by name, description or category"
		bind:value={searchTerm}
		class="search"
	/>
	<TextField
		kind="select"
		label="Category"
		size="lg"
		options={categoryOptions}
		bind:value={selectedCategory}
		class="category"
	/>
</div>

{#if !loading && !error && chips.length > 1}
	<div class="chips" bind:this={chipRow}>
		{#each visibleChips as chip (chip.value)}
			<Button
				variant="filter"
				aria-pressed={selectedCategory === chip.value}
				onclick={() => (selectedCategory = chip.value)}
				>{chip.label} <span class="chip-count">{chip.count}</span></Button
			>
		{/each}
		{#if hiddenChips > 0}
			<Button variant="filter" tone="muted" onclick={revealChips}>+ {hiddenChips} more</Button>
		{/if}
	</div>
{/if}

<div class="groups" aria-busy={loading || undefined}>
	{#if loading}
		{#each [0, 1, 2] as i (i)}
			<Card label="Loading">
				<div class="bar bar-title"></div>
				<div class="bar"></div>
				<div class="bar bar-short"></div>
			</Card>
		{/each}
	{:else if error}
		<Card title="Couldn't load the Data Dictionary" titleSize={26}>
			<p class="state-line">{error}</p>
			<div class="state-action"><Button variant="secondary" onclick={load}>Try again</Button></div>
		</Card>
	{:else if filtered.length === 0}
		<Card title="No metrics found" titleSize={26}>
			<p class="state-line">Try adjusting your search terms or category filter.</p>
		</Card>
	{:else}
		{#each filtered as category (category.categoryId)}
			<Card
				title={categoryLabel(category.categoryId, category.categoryName)}
				titleSize={26}
				phoneTitleLineHeight={1.2}
				headRule
				headSpacing="6px 16px"
				phoneHeadSpacing="4px 16px"
			>
				{#snippet action()}
					<span class="blurb">{categoryBlurb(category.categoryId, category.categoryDescription)}</span>
				{/snippet}
				{#each category.metrics as metric (metric.metricId)}
					{@const isOpen = expanded.has(metric.metricId)}
					{@const hasDetail =
						!!metric.calculationFormula || !!metric.interpretationGuide || !!metric.limitations}
					<div class="metric" id={metric.metricId}>
						<div class="line">
							<div class="name-block">
								<h3 tabindex="-1">{metric.metricName ?? metric.metricId}</h3>
								<code class="id">{metric.metricId}</code>
							</div>
							<span class="toggle">
								<Button
									variant="link"
									size="xs"
									aria-expanded={isOpen}
									aria-controls="{metric.metricId}-details"
									onclick={() => toggle(metric.metricId)}
								>
									{isOpen ? 'Hide details' : 'Show details'}
								</Button>
							</span>
						</div>
						{#if metric.shortDescription}<p class="desc">{metric.shortDescription}</p>{/if}
						<div id="{metric.metricId}-details" hidden={!isOpen}>
							{#if isOpen}
								{@const tiles = factTiles(metric)}
								{#if tiles.length > 0}
									<div class="tiles">
										{#each tiles as tile (tile.key)}
											<div class="tile" class:excellent={tile.key === 'Excellent'}>
												<div class="tile-key">{tile.key}</div>
												<div class="tile-value">{tile.value}</div>
											</div>
										{/each}
									</div>
								{/if}
								{#if hasDetail}
									<div class="details">
										{#if metric.calculationFormula}
											<div>
												<div class="detail-label">Calculation</div>
												<code class="formula">{metric.calculationFormula}</code>
												{#if metric.exampleCalculation}
													<div class="example">
														<b>Worked example:</b>
														{metric.exampleCalculation}
													</div>
												{/if}
											</div>
										{/if}
										{#if metric.interpretationGuide}
											<div>
												<div class="detail-label">How to read it</div>
												<div class="detail-text">{metric.interpretationGuide}</div>
											</div>
										{/if}
										{#if metric.limitations}
											<div class="limits">
												<div class="detail-label">Limitations</div>
												<div class="detail-text">{metric.limitations}</div>
											</div>
										{/if}
									</div>
								{/if}
							{/if}
						</div>
					</div>
				{/each}
			</Card>
		{/each}
	{/if}
</div>

<style>
	.tip {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px 14px;
		margin-top: 24px;
		padding: 14px 18px;
		border-radius: 12px;
		background: var(--color-field-tint);
	}

	.tip-lead {
		font-weight: 700;
		color: var(--color-clubhouse);
	}

	.tip-text {
		font-size: 14px;
		color: var(--color-ink);
	}

	.touch-copy {
		display: none;
	}

	@media (hover: none) {
		.hover-copy {
			display: none;
		}

		.touch-copy {
			display: inline;
		}
	}

	.filters {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-end;
		gap: 12px;
		margin-top: 20px;
	}

	.filters :global(.search) {
		flex: 999 1 360px;
	}

	.filters :global(.category) {
		flex: 1 1 220px;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		margin-top: 14px;
	}

	.chip-count {
		opacity: 0.7;
	}

	.groups {
		display: flex;
		flex-direction: column;
		gap: 20px;
		margin-top: 24px;
	}

	.blurb {
		font-size: 14px;
		color: var(--color-muted);
	}

	.metric {
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
		scroll-margin-top: 16px;
	}

	.line {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 4px 12px;
	}

	/* One row on a wide screen; the name over the id on a phone. */
	.name-block {
		display: contents;
	}

	h3 {
		margin: 0;
		font-size: 17px;
		font-weight: 700;
	}

	h3:focus {
		outline: none;
	}

	h3:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.id {
		font-family: var(--font-mono);
		font-size: 13px;
		color: var(--color-muted);
	}

	.toggle {
		margin-left: auto;
	}

	.desc {
		margin: 4px 0 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.tiles {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
		gap: 10px;
		margin-top: 12px;
	}

	.tile {
		padding: 10px 12px;
		border-radius: var(--radius-control);
		background: var(--color-ground);
	}

	.tile.excellent {
		background: var(--color-field-tint);
	}

	.tile-key,
	.detail-label {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.tile-value {
		margin-top: 2px;
		font-size: 15px;
		font-weight: 700;
	}

	.details {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		gap: 14px 24px;
		margin-top: 14px;
		font-size: 14px;
		line-height: 1.55;
		color: var(--color-ink);
	}

	.detail-label {
		color: var(--color-field);
	}

	.limits {
		grid-column: 1 / -1;
	}

	.limits .detail-label {
		color: var(--color-pennant-dark);
	}

	.formula {
		display: block;
		margin-top: 4px;
		padding: 8px 10px;
		border-radius: var(--radius-tag);
		background: var(--color-ground);
		font-family: var(--font-mono);
		font-size: 13px;
	}

	.example {
		margin-top: 6px;
		color: var(--color-ink-2);
	}

	.detail-text {
		margin-top: 4px;
	}

	.state-line {
		margin: 0;
		font-size: 15px;
		color: var(--color-muted);
	}

	.state-action {
		margin-top: 16px;
	}

	.bar {
		height: 14px;
		margin-top: 12px;
		border-radius: 4px;
		background: var(--color-wash);
	}

	.bar-title {
		width: 40%;
		height: 24px;
		margin-top: 0;
	}

	.bar-short {
		width: 60%;
	}

	@media (max-width: 639.98px) {
		.tip {
			padding: 14px 16px;
		}

		.tip-text {
			line-height: 1.6;
		}

		.filters {
			flex-direction: column;
			align-items: stretch;
		}

		.filters :global(.search),
		.filters :global(.category) {
			flex: none;
		}

		.filters :global(.control) {
			box-sizing: border-box;
			width: 100%;
		}

		.groups {
			gap: 16px;
		}

		.line {
			flex-wrap: nowrap;
			align-items: center;
		}

		.name-block {
			display: block;
			flex: 1;
			min-width: 0;
		}

		h3 {
			line-height: 1.3;
		}

		.id {
			display: block;
			margin-top: 2px;
			overflow-wrap: anywhere;
		}

		.toggle {
			flex: none;
			margin-left: 0;
			padding-left: 8px;
		}

		.desc {
			margin-top: 6px;
		}

		.details {
			grid-template-columns: minmax(0, 1fr);
		}

		.formula {
			overflow-wrap: anywhere;
		}
	}
</style>
