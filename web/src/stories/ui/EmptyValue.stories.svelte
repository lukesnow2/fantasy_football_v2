<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import EmptyValue from '$lib/components/ui/EmptyValue.svelte';

	const { Story } = defineMeta({
		title: 'UI/EmptyValue',
		component: EmptyValue,
		tags: ['autodocs']
	});
</script>

<Story name="None" />
<Story name="Custom word" args={{ text: 'No games yet' }} />
