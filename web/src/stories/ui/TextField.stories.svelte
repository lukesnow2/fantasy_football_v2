<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import TextField from '$lib/components/ui/TextField.svelte';

	const { Story } = defineMeta({ title: 'UI/TextField', component: TextField, tags: ['autodocs'] });

	const against = [
		{ value: '', label: 'Open to anyone' },
		{ value: '9', label: 'Gabe the Younger' },
		{ value: '12', label: 'Trevor' }
	];
</script>

<Story name="The bet form (Bet Board)">
	{#snippet template()}
		<div
			style="display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; max-width: 900px"
		>
			<TextField label="What's the bet?" wide placeholder="Gabe misses the playoffs" />
			<TextField
				kind="textarea"
				label="Terms: what has to happen for you to win"
				wide
				rows={3}
				placeholder="I win if he finishes 7th or worse. Ties go to him."
				helper="Write it out properly now. This is the wording everyone argues over in December."
			/>
			<TextField label="Stake" placeholder="$20, or loser buys wings" />
			<TextField kind="select" label="Against" options={against} value="" />
			<TextField label="Season" type="number" placeholder="2026" />
		</div>
	{/snippet}
</Story>

<Story name="Small, with a hint (What happened?)">
	{#snippet template()}
		<div style="max-width: 320px">
			<TextField size="sm" label="What happened?" hint="Optional" placeholder="He finished 8th." />
		</div>
	{/snippet}
</Story>

<Story name="No visible label (Reasoning)">
	{#snippet template()}
		<div style="max-width: 320px">
			<TextField size="sm" label="Reasoning" hideLabel placeholder="Reasoning (optional)" />
		</div>
	{/snippet}
</Story>

<Story name="Propose a change dialog (WP11)">
	{#snippet template()}
		<div style="display: grid; gap: 16px; max-width: 588px">
			<TextField label="Title" hint="Optional" phoneHintInline focusBorder placeholder="Short name for this proposal" />
			<TextField
				kind="select"
				label="What kind of change is this?"
				focusBorder
				helperWeight={400}
				helper="This sets how many votes it needs under Article VIII. Other changes need a super-majority."
				options={[{ value: 'general', label: 'Other constitutional change' }]}
			/>
			<TextField kind="textarea" label="Proposed wording" rows={3} focusBorder value="The trade deadline starts after week 10." />
			<TextField kind="textarea" size="sm" label="Comment" hideLabel rows={2} placeholder="Add a comment (optional)" />
		</div>
	{/snippet}
</Story>

<Story name="Data Dictionary search (WP11)">
	{#snippet template()}
		<div style="display: flex; gap: 12px; align-items: flex-end">
			<TextField label="Search" type="search" size="lg" placeholder="Search by name, description or category" />
			<TextField kind="select" label="Category" size="lg" options={[{ value: 'all', label: 'All categories' }]} />
		</div>
	{/snippet}
</Story>
