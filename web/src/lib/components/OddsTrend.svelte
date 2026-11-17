<script lang="ts">
	import { formatOdds } from '$lib/home';
	import { chartWeeks, type OddsHistory, type OddsMetric } from '$lib/odds-history';
	import { RANK_LINE_COLORS } from '$lib/utils/power-rankings';
	import Avatar from './ui/Avatar.svelte';

	/**
	 * Every team's odds week by week, one line each: the playoff odds widget's
	 * chart (PlayoffOdds.svelte) and, with `full`, the Playoff Odds page's.
	 * Lines are colored by where they end, top first, like the power rankings
	 * chart. Picking a team in the legend (or passing `highlight`) brings its
	 * line forward. The full chart also ends each line in its manager's picture
	 * and highlights on hover, as Rank through the season does.
	 */

	interface Props {
		history: Extract<OddsHistory, { available: true }>;
		metric: OddsMetric;
		/** The team to bring forward until a reader picks one. */
		highlight?: number | null;
		/** The page-width chart, with pictures at the line ends. */
		full?: boolean;
	}

	let { history, metric, highlight = null, full = false }: Props = $props();

	let picked = $state<number | null | undefined>(undefined);
	let hovered = $state<number | null>(null);
	const focus = $derived(hovered ?? (picked !== undefined ? picked : highlight));

	// About the chart's drawn width, so the axis text stays near its set size.
	const W = $derived(full ? 960 : 400);
	const H = $derived(full ? 460 : 230);
	const PAD = $derived(
		full
			? { top: 22, right: 44, bottom: 34, left: 52 }
			: { top: 14, right: 10, bottom: 30, left: 46 }
	);

	const weeks = $derived(chartWeeks(history, metric));
	const x = $derived.by(() => {
		const first = weeks[0] ?? 1;
		const span = Math.max(1, (weeks[weeks.length - 1] ?? 1) - first);
		return (w: number) => PAD.left + ((w - first) / span) * (W - PAD.left - PAD.right);
	});
	const y = $derived((p: number) => PAD.top + (1 - p) * (H - PAD.top - PAD.bottom));

	const series = $derived.by(() => {
		const inWeeks = new Set(weeks);
		const built = history.teams
			.map((t) => {
				const pts = t.points.filter((p) => inWeeks.has(p.week));
				const end = pts[pts.length - 1];
				return {
					teamKey: t.teamKey,
					name: t.managerName ?? 'Unknown manager',
					last: end?.[metric] ?? 0,
					end: end ? { x: x(end.week), y: y(end[metric]) } : { x: 0, y: 0 },
					path: pts
						.map(
							(p, i) => `${i === 0 ? 'M' : 'L'}${x(p.week).toFixed(1)},${y(p[metric]).toFixed(1)}`
						)
						.join(' ')
				};
			})
			.filter((s) => s.path !== '')
			.sort((a, b) => b.last - a.last);
		return built.map((s, i) => ({ ...s, color: RANK_LINE_COLORS[i % RANK_LINE_COLORS.length] }));
	});

	// Pictures at the line ends. Odds share ends (every team out of it sits at
	// 0%), so pictures that would sit on top of each other step left in a row.
	const ends = $derived.by(() => {
		const byY = [...series].sort((a, b) => a.end.y - b.end.y);
		const out: { teamKey: number; name: string; color: string; x: number; y: number }[] = [];
		let anchor = -Infinity;
		let inRow = 0;
		for (const s of byY) {
			if (s.end.y - anchor < 26) inRow++;
			else {
				anchor = s.end.y;
				inRow = 0;
			}
			out.push({
				teamKey: s.teamKey,
				name: s.name,
				color: s.color,
				x: s.end.x - inRow * 20,
				y: s.end.y
			});
		}
		// Later ones draw on top: the rows' first pictures last.
		return out.reverse();
	});
	const pctX = (v: number) => `${(v / W) * 100}%`;
	const pctY = (v: number) => `${(v / H) * 100}%`;

	// Lines drawn back to front, so the focused one sits on top.
	const drawOrder = $derived(
		[...series].sort((a, b) => Number(a.teamKey === focus) - Number(b.teamKey === focus))
	);

	// Every week fits up to about 16; past that, every other one.
	const ticks = $derived(weeks.filter((_, i) => weeks.length <= 16 || i % 2 === 0));
	const playoffStart = $derived(
		metric === 'titleOdds' && history.playoffWeeks.length > 0
			? (x(history.regularSeasonWeeks) + x(history.playoffWeeks[0])) / 2
			: null
	);

	const LABELS: Record<OddsMetric, string> = {
		playoffOdds: 'Playoff odds',
		byeOdds: 'Bye odds',
		titleOdds: 'Title odds'
	};
	const label = $derived(
		series.length === 0
			? `${LABELS[metric]} by week: none yet.`
			: `${LABELS[metric]} by week, weeks ${weeks[0]} to ${weeks[weeks.length - 1]}. At the end: ${series
					.map((s) => `${s.name} ${formatOdds(s.last, s.last === 0 || s.last === 1)}`)
					.join(', ')}.`
	);
</script>

<div class="trend" class:full>
	<div class="chart-scroll">
		<div class="chart" role="img" aria-label={label}>
			<svg viewBox="0 0 {W} {H}" aria-hidden="true">
				{#each [0, 0.25, 0.5, 0.75, 1] as p (p)}
					<line
						x1={PAD.left}
						x2={W - PAD.right}
						y1={y(p)}
						y2={y(p)}
						style:stroke="var(--color-line-soft)"
						stroke-width="1"
					/>
					<text class="axis" x={PAD.left - 8} y={y(p) + 5} text-anchor="end">{p * 100}%</text>
				{/each}
				{#if playoffStart != null}
					<line
						x1={playoffStart}
						x2={playoffStart}
						y1={PAD.top}
						y2={H - PAD.bottom}
						style:stroke="var(--color-field)"
						stroke-width="1.5"
						stroke-dasharray="4 4"
					/>
					<!-- Left of the line: the bracket weeks are the chart's last few. -->
					<text class="axis playoffs" x={playoffStart - 6} y={PAD.top + 12} text-anchor="end"
						>Playoffs</text
					>
				{/if}
				{#each ticks as w (w)}
					<text class="axis" x={x(w)} y={H - 6} text-anchor="middle">{w}</text>
				{/each}
				{#each drawOrder as s (s.teamKey)}
					<path
						d={s.path}
						fill="none"
						style:stroke={s.color}
						stroke-width={focus === s.teamKey ? 4 : 2.25}
						stroke-opacity={focus != null && focus !== s.teamKey ? 0.2 : 0.9}
						stroke-linejoin="round"
						stroke-linecap="round"
					/>
				{/each}
			</svg>
			{#if full}
				{#each ends as e (e.teamKey)}
					<span
						class="end"
						role="presentation"
						title={e.name}
						style:left={pctX(e.x)}
						style:top={pctY(e.y)}
						style:opacity={focus != null && focus !== e.teamKey ? 0.35 : 1}
						style:z-index={focus === e.teamKey ? 2 : 1}
						onmouseenter={() => (hovered = e.teamKey)}
						onmouseleave={() => (hovered = null)}
					>
						<Avatar name={e.name} size={26} ring={2.5} ringColor={e.color} />
					</span>
				{/each}
			{/if}
		</div>
		<p class="axis-note">Week</p>
	</div>
	<div class="legend" role="group" aria-label="Pick a team to bring its line forward">
		{#each series as s (s.teamKey)}
			<button
				type="button"
				class:on={focus === s.teamKey}
				aria-pressed={focus === s.teamKey}
				onclick={() => (picked = focus === s.teamKey ? null : s.teamKey)}
				onmouseenter={full ? () => (hovered = s.teamKey) : undefined}
				onmouseleave={full ? () => (hovered = null) : undefined}
			>
				<span class="dot" style:background={s.color}></span>
				<span class="name">{s.name}</span>
				<span class="value">{formatOdds(s.last, s.last === 0 || s.last === 1)}</span>
			</button>
		{/each}
	</div>
</div>

<style>
	.trend {
		min-width: 0;
	}

	/* The page-width chart: legend beside it, scrolling sideways on phones. */
	.trend.full {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	.full .chart-scroll {
		flex: 999 1 560px;
		min-width: 0;
		overflow-x: auto;
	}

	.full .chart {
		position: relative;
		min-width: 560px;
	}

	.full .axis {
		font-size: 13px;
	}

	.full .legend {
		flex: 1 1 200px;
		display: flex;
		flex-direction: column;
		gap: 2px;
		margin-top: 0;
	}

	.full .legend button {
		min-height: 34px;
		font-size: 14px;
	}

	.end {
		position: absolute;
		display: inline-flex;
		transform: translate(-50%, -50%);
	}

	.chart svg {
		display: block;
		width: 100%;
		height: auto;
	}

	.axis {
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		fill: var(--color-muted);
	}

	.axis.playoffs {
		fill: var(--color-field);
	}

	.axis-note {
		margin: -2px 0 0;
		text-align: center;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.legend {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(min(150px, 100%), 1fr));
		gap: 2px 8px;
		margin-top: 8px;
	}

	.legend button {
		display: flex;
		align-items: center;
		gap: 8px;
		min-height: 32px;
		padding: 0 8px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		cursor: pointer;
		font-family: var(--font-sans);
		font-size: 13px;
		font-weight: 600;
		color: var(--color-ink);
		text-align: left;
	}

	.legend button.on {
		background: var(--color-field-tint);
	}

	.legend button:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}

	.dot {
		flex: none;
		width: 10px;
		height: 10px;
		border-radius: 50%;
	}

	.name {
		flex: 1;
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.value {
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.legend {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}

		.legend button {
			min-height: 40px;
		}
	}
</style>
