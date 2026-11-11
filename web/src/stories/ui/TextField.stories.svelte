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
