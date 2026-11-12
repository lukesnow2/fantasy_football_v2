<script lang="ts">
	import { page } from '$app/state';
	import ChatPanel from '$lib/components/chat/ChatPanel.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { DEFAULT_CHANNEL } from '$lib/chat/constants';
	import { createChatStore } from '$lib/stores/chat.svelte';

	const meManagerKey = $derived(page.data.authenticatedManager?.managerKey ?? null);

	// Created here, not in the panel, so the header can read the roster and the
	// connection. The panel takes it as a prop: one store, one poll loop.
	const store = $derived.by(() => createChatStore({ channelId: DEFAULT_CHANNEL, meManagerKey }));

	$effect(() => store.start());
</script>

<svelte:head>
	<title>League Chat · The League</title>
</svelte:head>

<!-- Route-level auth comes from hooks.server.ts, which is deny-by-default:
     /chat is not in PUBLIC_PREFIXES, so a signed-out visitor is redirected to
     /login before this ever renders. The panel's signed-out state exists for the
     preview embedded on the public /this-season page. -->
<div class="space-y-8">
	<PageHeader kicker="Members only · {store.roster.length} managers" title="Chat">
		Trash talk, trade offers and grievances. Reply in a thread to keep side arguments out of the main feed.
	</PageHeader>

	<!-- dvh, not vh: on mobile Safari the toolbar makes vh taller than the visible
	     viewport, which pushes the composer off the bottom of the screen.
	     The subtracted height grew with the taller shared page header. -->
	<ChatPanel {store} heightClass="h-[calc(100dvh-26rem)] min-h-[24rem]" title="#general" />
</div>
