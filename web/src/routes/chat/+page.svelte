<script lang="ts">
	import { MessageSquare } from 'lucide-svelte';
	import ChatPanel from '$lib/components/chat/ChatPanel.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
</script>

<svelte:head>
	<title>League Chat · The League</title>
</svelte:head>

<!-- Route-level auth comes from hooks.server.ts, which is deny-by-default:
     /chat is not in PUBLIC_PREFIXES, so a signed-out visitor is redirected to
     /login before this ever renders. The panel's signed-out state exists for the
     preview embedded on the public /this-season page. -->
<div class="space-y-8">
	<PageHeader icon={MessageSquare} title="League Chat">
		Trash talk, trade offers and grievances. Reply in a thread to keep an argument in one place.
	</PageHeader>

	<!-- dvh, not vh: on mobile Safari the toolbar makes vh taller than the visible
	     viewport, which pushes the composer off the bottom of the screen.
	     The subtracted height grew with the taller shared page header. -->
	<ChatPanel heightClass="h-[calc(100dvh-26rem)] min-h-[24rem]" title="#general" />
</div>
