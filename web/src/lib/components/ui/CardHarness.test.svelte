<script lang="ts">
	import Card from './Card.svelte';
</script>

<div style="width: 600px; font-size: 16px; line-height: 1.5">
	<Card title="Official rankings">
		{#snippet action()}<span style="font-size: 13px">12 managers</span>{/snippet}
	</Card>
</div>
