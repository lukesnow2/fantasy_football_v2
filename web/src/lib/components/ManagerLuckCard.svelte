<script lang="ts">
	/**
	 * Expected (Pythagorean) wins and luck for one manager: career, and the
	 * season being played. The numbers are the power rankings' own, carried over
	 * by /api/managers/performance, so the pages always agree. Career counts
	 * finished seasons only; the live season has its own row (G8).
	 *
	 * - `variant="compact"` (Managers board): a pennant-tint card of rows, with
	 *   the playoff line while the season runs.
	 * - `variant="full"` (Manager profile Overview board): a white card with a
	 *   career box and a season box, and no playoff line.
	 *
	 * `stacked` is left over from the old card. It still compiles and means
	 * `variant="full"`, so callers keep working until WP6 moves them.
	 */
	import { Card, EmptyValue, StatTip } from '$lib/components/ui';
	import { formatLuck, luckTone, seasonCount } from '$lib/utils/power-rankings';
	import type { CurrentSeasonLuck } from '$lib/manager-luck';

	interface LuckSummary {
		careerRegularSeasonWins?: number | null;
		careerPythagoreanWins?: number | null;
		careerLuckFactor?: number | null;
		luckSeasons?: number | null;
		latestSeason?: number | null;
		currentSeason?: CurrentSeasonLuck | null;
	}

	interface Props {
		manager: LuckSummary | null | undefined;
		variant?: 'compact' | 'full';
		/** Deprecated: use `variant="full"`. */
		stacked?: boolean;
	}

	let { manager, variant = 'full', stacked = false }: Props = $props();

	const shape = $derived(stacked ? 'full' : variant);
	const current = $derived(manager?.currentSeason ?? null);
	const seasons = $derived(manager?.luckSeasons ?? 0);
	const latest = $derived(manager?.latestSeason ?? null);

	// A manager in their first season has no finished season yet (G8 keeps the
	// live one out of career), so the count drops rather than reading "0 seasons".
	const careerCompact = $derived(seasons > 0 ? `Career, ${seasonCount(seasons)}` : 'Career');
	const careerFull = $derived(seasons > 0 ? `Career · ${seasonCount(seasons)}` : 'Career');

	function oneplace(value: number | null | undefined): string | null {
		return value == null || !Number.isFinite(value) ? null : value.toFixed(1);
	}
</script>

{#snippet luck(value: number | null | undefined)}
	{@const text = formatLuck(value, 1)}
	{#if text === null}<EmptyValue />{:else}<span class="luck {luckTone(value, 1)}">{text} W</span
		>{/if}
{/snippet}

{#snippet number(value: number | null | undefined)}
	{@const text = oneplace(value)}
	{#if text === null}<EmptyValue />{:else}{text}{/if}
{/snippet}

{#if shape === 'compact'}
	<Card
		tone="pennant"
		title="Expected wins & luck"
		level={3}
		titleSize={24}
		phoneTitleSize={18}
		headGap={4}
		class="luck-card compact"
	>
		<p class="intro">Regular season. Luck is the gap between wins and expected wins.</p>
		<div class="row">
			<span>{careerCompact}</span>
			<b>{@render luck(manager?.careerLuckFactor)}</b>
		</div>
		{#if current}
			<div class="row">
				<span
					>{current.seasonYear}, {current.final
						? 'final'
						: `through week ${current.throughWeek}`}</span
				>
				<b>{@render luck(current.luckFactor)}</b>
			</div>
			{#if !current.final && current.inPlayoffs != null}
				<div class="row">
					<span>Playoffs</span>
					<b>{current.inPlayoffs ? 'In the bracket' : 'Missed'}</b>
				</div>
			{:else if !current.final}
				<div class="row">
					<!-- G8 keeps the ⓘ the old card had, though the board doesn't draw it. -->
					<span><StatTip metricId="playoff_probability" label="Playoff odds" /></span>
					<b
						>{#if current.playoffOdds == null}<EmptyValue text="Unknown" />{:else}{Math.round(
								current.playoffOdds * 100
							)}%{/if}</b
					>
				</div>
			{/if}
		{:else if latest != null}
			<!-- Between seasons this is the season just ended; a manager who
			     left the league gets this line, not their own last season. -->
			<div class="row"><span>Didn't play in {latest}.</span></div>
		{/if}
	</Card>
{:else}
	<Card title="Expected wins & luck" headGap={6} class="luck-card full">
		<p class="intro">
			Regular season. Expected wins come from points scored and allowed; luck is the gap.
		</p>
		<div class="boxes">
			<div class="box">
				<div class="span">{careerFull}</div>
				<div class="line first">
					<span>Wins</span><b>{@render number(manager?.careerRegularSeasonWins)}</b>
				</div>
				<div class="line">
					<span><StatTip metricId="pythagorean_wins" label="Expected" phoneTap="inline" /></span><b
						>{@render number(manager?.careerPythagoreanWins)}</b
					>
				</div>
				<div class="big">{@render luck(manager?.careerLuckFactor)}</div>
			</div>
			{#if current}
				<div class="box">
					<div class="span">
						{current.seasonYear} · {current.final ? 'final' : `thru wk ${current.throughWeek}`}
					</div>
					<div class="line first">
						<span>Wins</span><b>{@render number(current.regularSeasonWins)}</b>
					</div>
					<div class="line">
						<span><StatTip metricId="pythagorean_wins" label="Expected" phoneTap="inline" /></span
						><b>{@render number(current.pythagoreanWins)}</b>
					</div>
					<div class="big">{@render luck(current.luckFactor)}</div>
				</div>
			{:else if latest != null}
				<div class="box">
					<div class="span">{latest}</div>
					<p class="sat-out">Didn't play in {latest}.</p>
				</div>
			{/if}
		</div>
	</Card>
{/if}

<style>
	.intro {
		margin: 0;
	}

	/* ---------- Compact (Managers board) ---------- */
	:global(.luck-card.compact) .intro {
		margin-bottom: 10px;
		font-size: 13px;
		color: var(--color-ink-brown);
	}

	.row {
		display: flex;
		justify-content: space-between;
		gap: 12px;
		padding: 8px 0;
		font-size: 15px;
		color: var(--color-ink);
	}

	.row + .row {
		border-top: 1px solid var(--color-pennant-line);
	}

	.row b {
		font-weight: 700;
	}

	:global(.luck-card.compact) .luck.up {
		color: var(--color-field);
	}

	:global(.luck-card.compact) .luck.down {
		color: var(--color-pennant-dark);
	}

	:global(.luck-card.compact) .luck.flat {
		color: var(--color-ink);
	}

	/* ---------- Full (profile Overview board) ---------- */
	:global(.luck-card.full) .intro {
		margin-bottom: 14px;
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.boxes {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 12px;
	}

	.box {
		border: 1px solid var(--color-line);
		border-radius: 10px;
		padding: 14px 16px;
		min-width: 0;
	}

	.span {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.line {
		display: flex;
		justify-content: space-between;
		gap: 8px;
		margin-top: 4px;
		font-size: 14px;
	}

	.line.first {
		margin-top: 8px;
	}

	.line > span {
		color: var(--color-ink-2);
	}

	.line b {
		font-weight: 700;
		color: var(--color-ink);
	}

	.big {
		margin-top: 8px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 28px;
	}

	.sat-out {
		margin: 8px 0 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	:global(.luck-card.full) .luck.up {
		color: var(--color-field);
	}

	:global(.luck-card.full) .luck.down {
		color: var(--color-pennant);
	}

	:global(.luck-card.full) .luck.flat {
		color: var(--color-ink);
	}

	/* ---------- Phones (WP6 S6.9) ---------- */
	@media (max-width: 639.98px) {
		:global(.luck-card.compact) .intro {
			line-height: 1.5;
		}

		:global(.luck-card.full) .intro {
			margin-bottom: 12px;
		}

		.boxes {
			grid-template-columns: minmax(0, 1fr);
			gap: 10px;
		}

		.box {
			padding: 12px;
		}

		.big {
			font-size: 24px;
		}
	}
</style>
