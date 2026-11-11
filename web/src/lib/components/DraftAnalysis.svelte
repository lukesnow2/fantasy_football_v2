<script lang="ts">
	/**
	 * A season's best and worst picks as two mini cards beside the Selected pick
	 * (WP8 S8.7): "Best value" on field tint and "Biggest busts" on pennant
	 * tint, three rows each from the API's bestWorstPicks. Each row is a button
	 * that hands its pick to `onpick`, so the page can show it up close.
	 *
	 * The picks are judged by $lib/draft-value on the server: points over what
	 * that round and position usually score, busts only from picks the draft
	 * counted on, and never a kicker or defense.
	 */
	import { Card } from '$lib/components/ui';
	import { signed } from '$lib/draft-display';

	interface SeasonPick {
		overallPick: number;
		roundNumber: number;
		managerName: string;
		playerName: string;
		primaryPosition: string | null;
		valueOverExpected: number | null;
		pickType: 'best' | 'worst';
	}

	interface Props {
		picks: SeasonPick[];
		onpick: (overallPick: number) => void;
	}

	let { picks, onpick }: Props = $props();

	const lists = $derived([
		{
			key: 'best',
			title: 'Best value',
			tone: 'field' as const,
			rows: picks.filter((p) => p.pickType === 'best').slice(0, 3)
		},
		{
			key: 'worst',
			title: 'Biggest busts',
			tone: 'pennant' as const,
			rows: picks.filter((p) => p.pickType === 'worst').slice(0, 3)
		}
	]);
</script>

<section class="minis" aria-label="Best and worst picks">
	{#each lists as list (list.key)}
		<Card tone={list.tone} padding="12px 12px 8px" phonePadding="12px 12px 8px" class="mini {list.key}">
			<h2 class="mini-title">{list.title}</h2>
			{#each list.rows as p (p.overallPick)}
				<button type="button" class="row" onclick={() => onpick(p.overallPick)}>
					<span class="row-top">
						<span>{p.playerName}</span>
						<span class="value">{signed(Number(p.valueOverExpected ?? 0), 1)}</span>
					</span>
					<span class="row-meta"
						>R{p.roundNumber} · #{p.overallPick} · {p.primaryPosition} · {p.managerName}</span
					>
				</button>
			{/each}
		</Card>
	{/each}
</section>

<style>
	.minis {
		flex: 1 1 300px;
		min-width: 0;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 10px;
	}

	.minis :global(.best) {
		--mini-color: var(--color-field);
		--mini-line: var(--color-field-line);
	}

	.minis :global(.worst) {
		--mini-color: var(--color-pennant-dark);
		--mini-line: var(--color-pennant-line);
	}

	.mini-title {
		margin: 0;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--mini-color);
	}

	.row {
		display: block;
		width: 100%;
		margin-top: 6px;
		padding: 6px 4px;
		border: 0;
		border-top: 1px solid var(--mini-line);
		background: transparent;
		text-align: left;
		cursor: pointer;
		font-family: var(--font-sans);
		color: var(--color-ink);
	}

	.row:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.row-top {
		display: flex;
		justify-content: space-between;
		gap: 6px;
		font-weight: 700;
		font-size: 14px;
	}

	.value {
		color: var(--mini-color);
	}

	.row-meta {
		display: block;
		font-size: 12px;
		color: var(--color-ink-2);
	}

	@media (max-width: 639.98px) {
		.minis {
			display: flex;
			flex-direction: column;
			gap: 10px;
		}

		.row {
			min-height: 44px;
		}
	}
</style>
