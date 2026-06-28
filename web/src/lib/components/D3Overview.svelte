<script lang="ts">
	import { onMount, afterUpdate } from 'svelte';
	import { browser } from '$app/environment';
	
	let d3: any = null;

	export let data: any = null;
	
	let container: HTMLDivElement;
	let selectedMetric = 'scoring';
	let selectedView = 'line';
	
	// Available metrics for visualization
	const metrics = [
		{ 
			id: 'scoring', 
			name: 'Scoring Evolution', 
			description: 'Average weekly scores and volatility over time',
			yKey: 'avgWeeklyScore',
			secondaryKey: 'scoreVolatility',
			color: '#3b82f6'
		},
		{ 
			id: 'competitiveness', 
			name: 'League Competitiveness', 
			description: 'Parity and close games metrics',
			yKey: 'winParityScore',
			secondaryKey: 'closeGamesScore',
			color: '#ef4444'
		},
		{ 
			id: 'activity', 
			name: 'League Activity', 
			description: 'Transactions and trade activity',
			yKey: 'totalTransactions',
			secondaryKey: 'totalTrades',
			color: '#10b981'
		},
		{ 
			id: 'players', 
			name: 'Player Value Trends', 
			description: 'Average fantasy points and draft positions',
			yKey: 'avgFantasyPoints',
			secondaryKey: 'avgDraftPosition',
			color: '#f59e0b'
		},
		{ 
			id: 'spread', 
			name: 'Scoring Distribution', 
			description: 'High scores vs scoring spread',
			yKey: 'highestSingleWeekScore',
			secondaryKey: 'pointSpreadScore',
			color: '#8b5cf6'
		}
	];

	const viewTypes = [
		{ id: 'line', name: 'Line Chart', icon: '📈' },
		{ id: 'area', name: 'Area Chart', icon: '📊' },
		{ id: 'bar', name: 'Bar Chart', icon: '📋' },
		{ id: 'scatter', name: 'Scatter Plot', icon: '⚪' }
	];

	// Chart dimensions
	const margin = { top: 20, right: 80, bottom: 60, left: 60 };
	const width = 900 - margin.left - margin.right;
	const height = 500 - margin.top - margin.bottom;

	function getDataForMetric(metricId: string) {
		if (!data) return [];
		
		let rawData = [];
		switch (metricId) {
			case 'scoring':
				rawData = data.scoringPatterns || data.scoring_patterns || [];
				break;
			case 'competitiveness':
				rawData = data.competitiveness || [];
				break;
			case 'activity':
				const leagueEvolution = data.leagueEvolution || data.league_evolution || [];
				const tradeActivity = data.tradeActivity || data.trade_activity || [];
				rawData = leagueEvolution.map((d: any, i: number) => ({
					...d,
					totalTrades: (tradeActivity && tradeActivity[i]) ? (tradeActivity[i].totalTrades || tradeActivity[i].total_trades) : 0
				}));
				break;
			case 'players':
				rawData = data.playerTrends || data.player_trends || [];
				break;
			case 'spread':
				const leagueEvolutionSpread = data.leagueEvolution || data.league_evolution || [];
				const competitivenessSpread = data.competitiveness || [];
				rawData = leagueEvolutionSpread.map((d: any, i: number) => ({
					...d,
					point_spread_score: (competitivenessSpread && competitivenessSpread[i]) ? (competitivenessSpread[i].pointSpreadScore || competitivenessSpread[i].point_spread_score) : 0
				}));
				break;
			default:
				return [];
		}
		// Convert string values to numbers and ensure data integrity
		const processedData = rawData.map((d: any) => {
			const processed = { ...d };
			
			// Convert common string fields to numbers with camelCase fallback
			if (processed.avgWeeklyScore || processed.avg_weekly_score) processed.avgWeeklyScore = parseFloat(processed.avgWeeklyScore || processed.avg_weekly_score);
			if (processed.scoreVolatility || processed.score_volatility) processed.scoreVolatility = parseFloat(processed.scoreVolatility || processed.score_volatility);
			if (processed.highestSingleWeekScore || processed.highest_single_week_score) processed.highestSingleWeekScore = parseFloat(processed.highestSingleWeekScore || processed.highest_single_week_score);
			if (processed.averageWeeklyScore || processed.average_weekly_score) processed.averageWeeklyScore = parseFloat(processed.averageWeeklyScore || processed.average_weekly_score);
			if (processed.totalTransactions || processed.total_transactions) processed.totalTransactions = parseInt(processed.totalTransactions || processed.total_transactions);
			if (processed.totalTrades || processed.total_trades) processed.totalTrades = parseInt(processed.totalTrades || processed.total_trades);
			if (processed.winParityScore || processed.win_parity_score) processed.winParityScore = parseFloat(processed.winParityScore || processed.win_parity_score);
			if (processed.closeGamesScore || processed.close_games_score) processed.closeGamesScore = parseFloat(processed.closeGamesScore || processed.close_games_score);
			if (processed.avgFantasyPoints || processed.avg_fantasy_points) processed.avgFantasyPoints = parseFloat(processed.avgFantasyPoints || processed.avg_fantasy_points);
			if (processed.avgDraftPosition || processed.avg_draft_position) processed.avgDraftPosition = parseFloat(processed.avgDraftPosition || processed.avg_draft_position);
			if (processed.pointSpreadScore || processed.point_spread_score) processed.pointSpreadScore = parseFloat(processed.pointSpreadScore || processed.point_spread_score);
			
			return processed;
		});
		return processedData;
	}

	// Insight metadata per metric, for data-derived summaries shown under the chart.
	const insightMeta: Record<string, { label: string; unit: string; decimals: number }> = {
		scoring: { label: 'Average weekly scoring', unit: ' pts', decimals: 1 },
		competitiveness: { label: 'Win-parity score', unit: '', decimals: 1 },
		activity: { label: 'Total transactions', unit: '', decimals: 0 },
		players: { label: 'Average fantasy points', unit: ' pts', decimals: 1 },
		spread: { label: 'Highest weekly score', unit: ' pts', decimals: 1 }
	};

	// Compute real insights from the selected metric's own per-season series:
	// first→last change, peak/low season, and the multi-season average.
	function computeInsights(metricId: string): string[] {
		const meta = insightMeta[metricId];
		const yKey = metrics.find((m) => m.id === metricId)?.yKey;
		if (!meta || !yKey) return [];

		const series = getDataForMetric(metricId)
			.map((d: any) => ({ year: parseInt(d.seasonYear ?? d.season_year), val: parseFloat(d[yKey]) }))
			.filter((p: any) => Number.isFinite(p.year) && Number.isFinite(p.val))
			.sort((a: any, b: any) => a.year - b.year);
		if (series.length < 2) return [];

		const fmt = (n: number) => n.toFixed(meta.decimals) + meta.unit;
		const first = series[0];
		const last = series[series.length - 1];
		const peak = series.reduce((m: any, p: any) => (p.val > m.val ? p : m), series[0]);
		const low = series.reduce((m: any, p: any) => (p.val < m.val ? p : m), series[0]);
		const avg = series.reduce((s: number, p: any) => s + p.val, 0) / series.length;

		const delta = last.val - first.val;
		const pct = first.val !== 0 ? Math.round((delta / Math.abs(first.val)) * 100) : null;
		const dir = delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';

		return [
			`${meta.label}: ${fmt(first.val)} (${first.year}) → ${fmt(last.val)} (${last.year})` +
				(pct !== null && dir !== 'flat' ? `, ${dir} ${Math.abs(pct)}%` : ''),
			`Peak: ${fmt(peak.val)} in ${peak.year} · Low: ${fmt(low.val)} in ${low.year}`,
			`${series.length}-season average: ${fmt(avg)}`
		];
	}

	$: insights = data ? computeInsights(selectedMetric) : [];

	function createVisualization() {
		if (!container || !data || !d3 || !browser) return;

		// Clear previous chart
		d3.select(container).selectAll("*").remove();

		const currentMetric = metrics.find(m => m.id === selectedMetric);
		if (!currentMetric) return;

		const chartData = getDataForMetric(selectedMetric);
		if (!chartData.length) return;

		// Create SVG
		const svg = d3.select(container)
			.append('svg')
			.attr('width', width + margin.left + margin.right)
			.attr('height', height + margin.top + margin.bottom);

		const g = svg.append('g')
			.attr('transform', `translate(${margin.left},${margin.top})`);

		// Scales
		const xDomain = d3.extent(chartData, (d: any) => getNumeric(d, 'seasonYear', 'int')) as [number, number];
		const yDomain = d3.extent(chartData, (d: any) => getNumeric(d, currentMetric.yKey)) as [number, number];
		const secondaryDomain = currentMetric.secondaryKey
			? d3.extent(chartData, (d: any) => getNumeric(d, currentMetric.secondaryKey))
			: null;
		
		
		const xScale = d3.scaleLinear()
			.domain(xDomain)
			.range([0, width]);

		const yScale = d3.scaleLinear()
			.domain(yDomain)
			.nice()
			.range([height, 0]);

		const secondaryScale = currentMetric.secondaryKey ? d3.scaleLinear()
			.domain(secondaryDomain as [number, number])
			.nice()
			.range([height, 0]) : null;

		// Create visualization based on view type
		if (selectedView === 'line') {
			createLineChart(g, chartData, xScale, yScale, secondaryScale, currentMetric);
		} else if (selectedView === 'area') {
			createAreaChart(g, chartData, xScale, yScale, currentMetric);
		} else if (selectedView === 'bar') {
			createBarChart(g, chartData, xScale, yScale, currentMetric);
		} else if (selectedView === 'scatter') {
			createScatterPlot(g, chartData, xScale, yScale, secondaryScale, currentMetric);
		}

		// Add axes
		g.append('g')
			.attr('transform', `translate(0,${height})`)
			.call(d3.axisBottom(xScale).tickFormat(d3.format('d')))
			.append('text')
			.attr('x', width / 2)
			.attr('y', 40)
			.style('text-anchor', 'middle')
			.style('fill', '#e2e8f0')
			.text('Season');

		g.append('g')
			.call(d3.axisLeft(yScale))
			.append('text')
			.attr('transform', 'rotate(-90)')
			.attr('y', -40)
			.attr('x', -height / 2)
			.style('text-anchor', 'middle')
			.style('fill', '#e2e8f0')
			.text(currentMetric.yKey.replace(/_/g, ' ').toUpperCase());

		// Add secondary axis if applicable
		if (secondaryScale && currentMetric.secondaryKey) {
			g.append('g')
				.attr('transform', `translate(${width}, 0)`)
				.call(d3.axisRight(secondaryScale))
				.append('text')
				.attr('transform', 'rotate(-90)')
				.attr('y', 40)
				.attr('x', -height / 2)
				.style('text-anchor', 'middle')
				.style('fill', '#f59e0b')
				.text(currentMetric.secondaryKey.replace(/_/g, ' ').toUpperCase());
		}

		// Add title
		svg.append('text')
			.attr('x', (width + margin.left + margin.right) / 2)
			.attr('y', 20)
			.style('text-anchor', 'middle')
			.style('font-size', '18px')
			.style('font-weight', 'bold')
			.style('fill', '#f1f5f9')
			.text(currentMetric.name);
	}

	function createLineChart(g: any, data: any[], xScale: any, yScale: any, secondaryScale: any, metric: any) {
		const line = d3.line()
			.x((d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
			.y((d: any) => yScale(getNumeric(d, metric.yKey)))
			.curve(d3.curveMonotoneX);

		// Main line
		g.append('path')
			.datum(data)
			.attr('fill', 'none')
			.attr('stroke', metric.color)
			.attr('stroke-width', 3)
			.attr('d', line);

		// Data points
		g.selectAll('.dot')
			.data(data)
			.enter().append('circle')
			.attr('class', 'dot')
			.attr('cx', (d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
			.attr('cy', (d: any) => yScale(getNumeric(d, metric.yKey)))
			.attr('r', 4)
			.attr('fill', metric.color)
			.on('mouseover', function(event: any, d: any) {
				// Tooltip
				const tooltip = d3.select('body').append('div')
					.attr('class', 'tooltip')
					.style('position', 'absolute')
					.style('background', '#1e293b')
					.style('color', '#f1f5f9')
					.style('padding', '8px')
					.style('border-radius', '4px')
					.style('font-size', '12px')
					.style('pointer-events', 'none')
					.style('opacity', 0);

				tooltip.transition().duration(200).style('opacity', 1);
				const seasonYear = getNumeric(d, 'seasonYear', 'int');
				const yVal = getNumeric(d, metric.yKey);
				const secondaryVal = metric.secondaryKey ? getNumeric(d, metric.secondaryKey) : null;
				tooltip.html(`
					<strong>${seasonYear}</strong><br/>
					${metric.yKey.replace(/_/g, ' ')}: ${yVal.toFixed(1)}
					${metric.secondaryKey ? '<br/>' + metric.secondaryKey.replace(/_/g, ' ') + ': ' + (secondaryVal !== null ? secondaryVal.toFixed(1) : '') : ''}
				`)
				.style('left', (event.pageX + 10) + 'px')
				.style('top', (event.pageY - 10) + 'px');
			})
			.on('mouseout', function() {
				d3.selectAll('.tooltip').remove();
			});

		// Secondary line if available
		if (secondaryScale && metric.secondaryKey) {
			const secondaryLine = d3.line()
				.x((d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
				.y((d: any) => secondaryScale(getNumeric(d, metric.secondaryKey)))
				.curve(d3.curveMonotoneX);

			g.append('path')
				.datum(data)
				.attr('fill', 'none')
				.attr('stroke', '#f59e0b')
				.attr('stroke-width', 2)
				.attr('stroke-dasharray', '5,5')
				.attr('d', secondaryLine);
		}
	}

	function createAreaChart(g: any, data: any[], xScale: any, yScale: any, metric: any) {
		const area = d3.area()
			.x((d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
			.y0(height)
			.y1((d: any) => yScale(getNumeric(d, metric.yKey)))
			.curve(d3.curveMonotoneX);

		g.append('path')
			.datum(data)
			.attr('fill', metric.color)
			.attr('fill-opacity', 0.3)
			.attr('d', area);

		// Add line on top
		const line = d3.line()
			.x((d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
			.y((d: any) => yScale(getNumeric(d, metric.yKey)))
			.curve(d3.curveMonotoneX);

		g.append('path')
			.datum(data)
			.attr('fill', 'none')
			.attr('stroke', metric.color)
			.attr('stroke-width', 2)
			.attr('d', line);
	}

	function createBarChart(g: any, data: any[], xScale: any, yScale: any, metric: any) {
		const barWidth = width / data.length * 0.8;

		g.selectAll('.bar')
			.data(data)
			.enter().append('rect')
			.attr('class', 'bar')
			.attr('x', (d: any) => xScale(getNumeric(d, 'seasonYear', 'int')) - barWidth / 2)
			.attr('y', (d: any) => yScale(getNumeric(d, metric.yKey)))
			.attr('width', barWidth)
			.attr('height', (d: any) => height - yScale(getNumeric(d, metric.yKey)))
			.attr('fill', metric.color)
			.attr('fill-opacity', 0.7)
			.on('mouseover', function(event: any, d: any) {
				d3.select(this).attr('fill-opacity', 1);
			})
			.on('mouseout', function() {
				d3.select(this).attr('fill-opacity', 0.7);
			});
	}

	function createScatterPlot(g: any, data: any[], xScale: any, yScale: any, secondaryScale: any, metric: any) {
		if (!metric.secondaryKey || !secondaryScale) {
			// Fallback to simple scatter
			g.selectAll('.dot')
				.data(data)
				.enter().append('circle')
				.attr('class', 'dot')
				.attr('cx', (d: any) => xScale(getNumeric(d, 'seasonYear', 'int')))
				.attr('cy', (d: any) => yScale(getNumeric(d, metric.yKey)))
				.attr('r', 6)
				.attr('fill', metric.color)
				.attr('fill-opacity', 0.7);
			return;
		}

		// Scatter plot with two metrics
		g.selectAll('.dot')
			.data(data)
			.enter().append('circle')
			.attr('class', 'dot')
			.attr('cx', (d: any) => yScale(getNumeric(d, metric.yKey)))
			.attr('cy', (d: any) => secondaryScale(getNumeric(d, metric.secondaryKey)))
			.attr('r', 6)
			.attr('fill', metric.color)
			.attr('fill-opacity', 0.7);
	}

	// Helper to convert camelCase to snake_case
	function camelToSnake(str: string) {
		return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
	}
	// Helper to robustly get a numeric value from both camelCase and snake_case
	function getNumeric(obj: any, key: string, type: 'float' | 'int' = 'float') {
		const camel = obj[key];
		const snake = obj[camelToSnake(key)];
		let val = camel !== undefined ? camel : snake;
		if (val === undefined || val === null || val === '') return 0;
		if (typeof val === 'string' && val.trim() === '') return 0;
		if (type === 'int') return parseInt(val, 10) || 0;
		return parseFloat(val) || 0;
	}

	onMount(async () => {
		if (browser) {
			d3 = await import('d3');
			createVisualization();
		}
	});

	afterUpdate(() => {
		if (browser && d3) {
			createVisualization();
		}
	});

	$: if ((selectedMetric || selectedView) && browser && d3) {
		createVisualization();
	}
</script>

<div class="d3-overview-container">
	<!-- Controls -->
	<div class="controls mb-6">
		<div class="flex flex-wrap gap-4 mb-4">
			<div class="flex-1 min-w-0">
				<label class="block text-sm font-medium text-slate-300 mb-2">Metric</label>
				<select bind:value={selectedMetric} class="w-full bg-slate-700 text-white rounded-lg px-3 py-2 border border-slate-600">
					{#each metrics as metric}
						<option value={metric.id}>{metric.name}</option>
					{/each}
				</select>
			</div>
			
			<div class="flex-1 min-w-0">
				<label class="block text-sm font-medium text-slate-300 mb-2">View Type</label>
				<select bind:value={selectedView} class="w-full bg-slate-700 text-white rounded-lg px-3 py-2 border border-slate-600">
					{#each viewTypes as view}
						<option value={view.id}>{view.icon} {view.name}</option>
					{/each}
				</select>
			</div>
		</div>
		
		<!-- Metric description -->
		{#if selectedMetric}
			{@const currentMetric = metrics.find(m => m.id === selectedMetric)}
			{#if currentMetric}
				<div class="text-sm text-slate-400 bg-slate-800/50 rounded-lg p-3">
					{currentMetric.description}
				</div>
			{/if}
		{/if}
	</div>

	<!-- Visualization container -->
	<div class="chart-container bg-slate-800/30 rounded-xl p-6 border border-slate-700/50">
		<div bind:this={container} class="w-full"></div>
		
		{#if !data}
			<div class="flex items-center justify-center h-64 text-slate-400">
				Loading league data...
			</div>
		{/if}
	</div>

	<!-- Legend and insights -->
	{#if data && selectedMetric}
		{@const currentMetric = metrics.find(m => m.id === selectedMetric)}
		{#if currentMetric}
		<div class="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
			<div class="bg-slate-800/50 rounded-lg p-4">
				<h3 class="font-bold text-white mb-2">Key Insights</h3>
				<div class="text-sm text-slate-300 space-y-1">
					{#each insights as insight}
						<div>• {insight}</div>
					{:else}
						<div class="text-slate-400">Insights appear once season data loads.</div>
					{/each}
				</div>
			</div>
			
			<div class="bg-slate-800/50 rounded-lg p-4">
				<h3 class="font-bold text-white mb-2">Legend</h3>
				<div class="text-sm text-slate-300 space-y-2">
					<div class="flex items-center">
						<div class="w-4 h-0.5 mr-2" style="background-color: {currentMetric.color}"></div>
						<span>{currentMetric.yKey.replace(/_/g, ' ').toUpperCase()}</span>
					</div>
					{#if currentMetric.secondaryKey}
						<div class="flex items-center">
							<div class="w-4 h-0.5 mr-2 border-dashed border-t-2" style="border-color: #f59e0b"></div>
							<span>{currentMetric.secondaryKey.replace(/_/g, ' ').toUpperCase()}</span>
						</div>
					{/if}
				</div>
			</div>
		</div>
		{/if}
	{/if}
</div>

<style>
	.d3-overview-container {
		width: 100%;
		max-width: 1200px;
		margin: 0 auto;
	}
	
	:global(.tooltip) {
		box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
		z-index: 1000;
	}
</style> 