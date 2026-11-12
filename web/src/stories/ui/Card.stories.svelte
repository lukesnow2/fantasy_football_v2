<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Card from '$lib/components/ui/Card.svelte';
	import Button from '$lib/components/ui/Button.svelte';
	import DataTable from '$lib/components/ui/DataTable.svelte';
	import Pennant from '$lib/components/ui/Pennant.svelte';
	import { STANDINGS } from './fixtures';

	const { Story } = defineMeta({ title: 'UI/Card', component: Card, tags: ['autodocs'] });

	const columns = [
		{ key: 'rank', label: '#', width: '32px' },
		{ key: 'name', label: 'Manager' },
		{ key: 'wl', label: 'W-L', numeric: true, width: '56px' },
		{ key: 'pf', label: 'PF', numeric: true, width: '72px' }
	];
</script>

<Story name="With title and link">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card title="Power Rankings">
				{#snippet action()}<Button variant="link" href="#">Full rankings →</Button>{/snippet}
				<p style="margin: 0; color: var(--color-ink-2)">Card body.</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="With title and meta">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card title="Standings">
				{#snippet action()}<span style="font-size: 13px; color: var(--color-muted)"
						>Updated 2 hours ago · odds from 10,000 sims</span
					>{/snippet}
				<p style="margin: 0; color: var(--color-ink-2)">Card body.</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="No header">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card><p style="margin: 0; color: var(--color-ink-2)">A card with no title row.</p></Card>
		</div>
	{/snippet}
</Story>

<Story name="Title sizes">
	{#snippet template()}
		<div style="display: grid; gap: 20px; max-width: 560px">
			<Card title="Card title at 28" />
			<Card title="Card title at 26" titleSize={26} />
			<Card title="Card title at 24" titleSize={24} />
		</div>
	{/snippet}
</Story>

<Story name="Flush">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card title="Standings" flush>
				<DataTable label="Standings" {columns} rows={STANDINGS.slice(0, 4)} />
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="On the band, borderless">
	{#snippet template()}
		<div style="padding: 24px; background: var(--color-clubhouse)">
			<div style="max-width: 388px">
				<Card title="This week" titleSize={26} headGap={10} tone="borderless">
					{#snippet action()}<span>Wk 3 of 14</span>{/snippet}
					<p style="margin: 0">Israel 3-0</p>
				</Card>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Pennant">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card tone="pennant" padding="24px">
				<p style="margin: 0">Luke S 21-19 Trevor</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Clubhouse tone">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card tone="clubhouse" padding="26px 28px">
				<p style="margin: 0; font-size: 16px; color: var(--color-on-clubhouse)">A dark card, as This Season's champion draws it.</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Centered header with a control">
	{#snippet template()}
		<div style="max-width: 760px">
			<Card title="Final standings" headAlign="center" headSpacing="10px 16px">
				{#snippet action()}<Button variant="secondary">Record</Button>{/snippet}
				<p style="margin: 0; color: var(--color-ink-2)">Card body.</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Field tone (season draft's Best value)">
	{#snippet template()}
		<div style="max-width: 300px">
			<Card tone="field" padding="12px 12px 8px">Dak Prescott +199.9</Card>
		</div>
	{/snippet}
</Story>

<Story name="Panel (Best drafters)">
	{#snippet template()}
		<div style="max-width: 240px">
			<Card tone="panel" padding="12px 14px">Gabe the Younger +8.2</Card>
		</div>
	{/snippet}
</Story>

<Story name="No title, with a label">
	{#snippet template()}
		<div style="max-width: 460px">
			<Card label="Selected pick" padding="20px 22px">Pick 87 · Round 9</Card>
		</div>
	{/snippet}
</Story>

<Story name="Bet Board stage and bets">
	{#snippet template()}
		<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; max-width: 900px">
			<Card title="Live" titleSize={24} dot="field" stack={12}>
				<Card tone="inset" padding="14px 16px" label="Gabe misses the playoffs">
					<b>Gabe misses the playoffs</b>
				</Card>
				<Card tone="inset-pennant" padding="14px 16px" label="Awaiting a ruling">
					<b>Awaiting a ruling</b>
				</Card>
			</Card>
			<Card title="Settled" titleSize={24} dot="ink" stack={12}>
				<p style="margin: 0; font-size: 14px; color: var(--color-muted)">Ruled on and counted in the ledger.</p>
				<div style="margin-top: auto"><Card tone="dashed" padding="16px">Nothing has been ruled on yet.</Card></div>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Highlight (the open bet form)">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card title="Put it on the board" tone="highlight" padding="24px">
				<p style="margin: 0; color: var(--color-ink-2)">The form.</p>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Tiles (the ledger)">
	{#snippet template()}
		<div style="max-width: 560px">
			<Card title="The ledger" tone="pennant">
				<Card tone="tile" padding="10px 12px">Craig · 0-0</Card>
			</Card>
		</div>
	{/snippet}
</Story>

<Story name="Head rule and icon (League History record book)">
	{#snippet template()}
		<div style="display: grid; gap: 20px; max-width: 760px">
			<Card title="Scoring records" titleSize={26} headRule>
				{#snippet action()}<span style="font-size: 14px; color: var(--color-muted)">13 records</span>{/snippet}
				<p style="margin: 14px 0 0; color: var(--color-ink-2)">Record rows.</p>
			</Card>
			<Card title="Championship records" titleSize={26} tone="clubhouse" padding="24px 28px" headGap={16}>
				{#snippet icon()}<Pennant staff="var(--color-white)" fill="var(--color-pennant-light)" width={14} height={20} />{/snippet}
				<p style="margin: 0">Tiles.</p>
			</Card>
		</div>
	{/snippet}
</Story>
