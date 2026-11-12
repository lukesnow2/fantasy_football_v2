<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Dialog from '$lib/components/ui/Dialog.svelte';
	import Button from '$lib/components/ui/Button.svelte';
	import TextField from '$lib/components/ui/TextField.svelte';
	import { fn } from 'storybook/test';

	const { Story } = defineMeta({
		title: 'UI/Dialog',
		component: Dialog,
		tags: ['autodocs'],
		args: { onClose: fn() }
	});
</script>

<Story name="Propose a change (Constitution)">
	{#snippet template(args)}
		<Dialog kicker="Article II · Trades" title="Propose a change" onClose={args.onClose}>
			<div style="display: flex; flex-direction: column; gap: 16px; padding: 22px 26px">
				<TextField label="Title" hint="Optional" phoneHintInline focusBorder placeholder="Short name for this proposal" />
				<TextField kind="textarea" label="Proposed wording" rows={3} focusBorder value="The trade deadline starts after week 10." />
				<div style="display: flex; gap: 10px; padding-top: 6px; border-top: 1px solid var(--color-line-soft)">
					<Button height={46} padX={20} bold>Save as draft</Button>
					<Button variant="secondary" height={46} onclick={args.onClose}>Cancel</Button>
				</div>
			</div>
		</Dialog>
	{/snippet}
</Story>
