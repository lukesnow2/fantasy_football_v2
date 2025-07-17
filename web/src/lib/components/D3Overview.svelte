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
			yKey: 'avg_weekly_score',
			secondaryKey: 'score_volatility',
			color: '#3b82f6'
		},
		{ 
			id: 'competitiveness', 
			name: 'League Competitiveness', 
			description: 'Parity and close games metrics',
			yKey: 'win_parity_score',
			secondaryKey: 'close_games_score',
			color: '#ef4444'
		},
		{ 
			id: 'activity', 
			name: 'League Activity', 
			description: 'Transactions and trade activity',
			yKey: 'total_transactions',
			secondaryKey: 'total_trades',
			color: '#10b981'
		},
		{ 
			id: 'players', 
			name: 'Player Value Trends', 
			description: 'Average fantasy points and draft positions',
			yKey: 'avg_fantasy_points',
			secondaryKey: 'avg_draft_position',
			color: '#f59e0b'
		},
		{ 
			id: 'spread', 
			name: 'Scoring Distribution', 
			description: 'High scores vs scoring spread',
			yKey: 'highest_single_week_score',
			secondaryKey: 'point_spread_score',
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
		console.log('Getting data for metric:', metricId, 'data keys:', data ? Object.keys(data) : 'no data');
		if (!data) return [];
		
		let rawData = [];
		switch (metricId) {
			case 'scoring':
				rawData = data.scoring_patterns || [];
				break;
			case 'competitiveness':
				rawData = data.competitiveness || [];
				break;
			case 'activity':
				rawData = (data.league_evolution || []).map((d: any, i: number) => ({
					...d,
					total_trades: (data.trade_activity && data.trade_activity[i]) ? data.trade_activity[i].total_trades : 0
				}));
				break;
			case 'players':
				rawData = data.player_trends || [];
				break;
			case 'spread':
				rawData = (data.league_evolution || []).map((d: any, i: number) => ({
					...d,
					point_spread_score: (data.competitiveness && data.competitiveness[i]) ? data.competitiveness[i].point_spread_score : 0
				}));
				break;
			default:
				return [];
		}
		
		// Convert string values to numbers and ensure data integrity
		const processedData = rawData.map((d: any) => {
			const processed = { ...d };
			
			// Convert common string fields to numbers
			if (processed.avg_weekly_score) processed.avg_weekly_score = parseFloat(processed.avg_weekly_score);
			if (processed.score_volatility) processed.score_volatility = parseFloat(processed.score_volatility);
			if (processed.highest_single_week_score) processed.highest_single_week_score = parseFloat(processed.highest_single_week_score);
			if (processed.average_weekly_score) processed.average_weekly_score = parseFloat(processed.average_weekly_score);
			if (processed.total_transactions) processed.total_transactions = parseInt(processed.total_transactions);
			if (processed.total_trades) processed.total_trades = parseInt(processed.total_trades);
			if (processed.win_parity_score) processed.win_parity_score = parseFloat(processed.win_parity_score);
			if (processed.close_games_score) processed.close_games_score = parseFloat(processed.close_games_score);
			if (processed.avg_fantasy_points) processed.avg_fantasy_points = parseFloat(processed.avg_fantasy_points);
			if (processed.avg_draft_position) processed.avg_draft_position = parseFloat(processed.avg_draft_position);
			if (processed.point_spread_score) processed.point_spread_score = parseFloat(processed.point_spread_score);
			
			return processed;
		});
		
		console.log('Processed data for', metricId, ':', processedData.slice(0, 3));
		return processedData;
	}

	function createVisualization() {
		console.log('Creating visualization:', { container: !!container, data: !!data, d3: !!d3, browser });
		if (!container || !data || !d3 || !browser) return;

		// Clear previous chart
		d3.select(container).selectAll("*").remove();

		const currentMetric = metrics.find(m => m.id === selectedMetric);
		if (!currentMetric) return;

		const chartData = getDataForMetric(selectedMetric);
		console.log('Chart data length:', chartData.length);
		console.log('Sample chart data:', chartData.slice(0, 3));
		console.log('Y-key values:', chartData.map(d => d[currentMetric.yKey]).slice(0, 5));
		if (!chartData.length) return;

		// Create SVG
		const svg = d3.select(container)
			.append('svg')
			.attr('width', width + margin.left + margin.right)
			.attr('height', height + margin.top + margin.bottom);

		const g = svg.append('g')
			.attr('transform', `translate(${margin.left},${margin.top})`);

		// Scales
		const xDomain = d3.extent(chartData, (d: any) => d.season_year) as [number, number];
		const yDomain = d3.extent(chartData, (d: any) => d[currentMetric.yKey]) as [number, number];
		
		console.log('X domain (years):', xDomain);
		console.log('Y domain (values):', yDomain);
		
		const xScale = d3.scaleLinear()
			.domain(xDomain)
			.range([0, width]);

		const yScale = d3.scaleLinear()
			.domain(yDomain)
			.nice()
			.range([height, 0]);

		const secondaryScale = currentMetric.secondaryKey ? d3.scaleLinear()
			.domain(d3.extent(chartData, (d: any) => d[currentMetric.secondaryKey]) as [number, number])
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
		const line = d3.line<any>()
			.x(d => xScale(d.season_year))
			.y(d => yScale(d[metric.yKey]))
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
			.attr('cx', (d: any) => xScale(d.season_year))
			.attr('cy', (d: any) => yScale(d[metric.yKey]))
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
				tooltip.html(`
					<strong>${d.season_year}</strong><br/>
					${metric.yKey.replace(/_/g, ' ')}: ${parseFloat(d[metric.yKey]).toFixed(1)}
					${metric.secondaryKey ? '<br/>' + metric.secondaryKey.replace(/_/g, ' ') + ': ' + parseFloat(d[metric.secondaryKey]).toFixed(1) : ''}
				`)
				.style('left', (event.pageX + 10) + 'px')
				.style('top', (event.pageY - 10) + 'px');
			})
			.on('mouseout', function() {
				d3.selectAll('.tooltip').remove();
			});

		// Secondary line if available
		if (secondaryScale && metric.secondaryKey) {
			const secondaryLine = d3.line<any>()
				.x(d => xScale(d.season_year))
				.y(d => secondaryScale(d[metric.secondaryKey]))
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
		const area = d3.area<any>()
			.x(d => xScale(d.season_year))
			.y0(height)
			.y1(d => yScale(d[metric.yKey]))
			.curve(d3.curveMonotoneX);

		g.append('path')
			.datum(data)
			.attr('fill', metric.color)
			.attr('fill-opacity', 0.3)
			.attr('d', area);

		// Add line on top
		const line = d3.line<any>()
			.x(d => xScale(d.season_year))
			.y(d => yScale(d[metric.yKey]))
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
			.attr('x', (d: any) => xScale(d.season_year) - barWidth / 2)
			.attr('y', (d: any) => yScale(d[metric.yKey]))
			.attr('width', barWidth)
			.attr('height', (d: any) => height - yScale(d[metric.yKey]))
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
				.attr('cx', (d: any) => xScale(d.season_year))
				.attr('cy', (d: any) => yScale(d[metric.yKey]))
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
			.attr('cx', (d: any) => yScale(d[metric.yKey]))
			.attr('cy', (d: any) => secondaryScale(d[metric.secondaryKey]))
			.attr('r', 6)
			.attr('fill', metric.color)
			.attr('fill-opacity', 0.7);
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
					{#if selectedMetric === 'scoring'}
						<div>• League scoring has evolved from ~108 pts (2005) to ~122 pts (2024)</div>
						<div>• Peak scoring was in 2007 (~134 pts average)</div>
						<div>• Score volatility has generally decreased over time</div>
					{:else if selectedMetric === 'competitiveness'}
						<div>• Win parity scores show how evenly matched teams are</div>
						<div>• Close games metric indicates competitive balance</div>
						<div>• Higher scores = more competitive seasons</div>
					{:else if selectedMetric === 'activity'}
						<div>• Transaction activity peaked in 2020-2021 (~600+ moves)</div>
						<div>• Trade volume has decreased in recent years</div>
						<div>• Early years had fewer total moves but more impactful trades</div>
					{:else if selectedMetric === 'players'}
						<div>• Player scoring has stabilized around 190-200 pts</div>
						<div>• Draft positions have gotten more accurate over time</div>
						<div>• Value picks are easier to identify in modern seasons</div>
					{:else if selectedMetric === 'spread'}
						<div>• Highest weekly scores have trended upward</div>
						<div>• Point spread indicates scoring consistency</div>
						<div>• Recent seasons show both high ceilings and floors</div>
					{/if}
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