<script lang="ts">
	import Button from './Button.svelte';

	let { onclick }: { onclick?: () => void } = $props();
</script>

<Button class="mt-4" href="/power-rankings">Full rankings</Button>
<Button href="/constitution" disabled {onclick}>Propose a change</Button>
