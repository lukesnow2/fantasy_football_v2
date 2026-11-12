<script lang="ts">
	import { page } from '$app/state';
	import ChatPanel from '$lib/components/chat/ChatPanel.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { DEFAULT_CHANNEL } from '$lib/chat/constants';
	import { connectionPill } from '$lib/chat/format';
	import { createChatStore } from '$lib/stores/chat.svelte';

	const meManagerKey = $derived(page.data.authenticatedManager?.managerKey ?? null);

	// Created here, not in the panel, so the header can read the roster and the
	// connection. The panel takes it as a prop: one store, one poll loop.
	const store = $derived.by(() => createChatStore({ channelId: DEFAULT_CHANNEL, meManagerKey }));

	$effect(() => store.start());

	const managers = $derived(store.roster.length);
	const kicker = $derived(
		managers > 0
			? `Members only · ${managers} ${managers === 1 ? 'manager' : 'managers'}`
			: 'Members only'
	);
	const pill = $derived(meManagerKey === null ? null : connectionPill(store.connectionState));
</script>

<svelte:head>
	<title>League Chat · The League</title>
</svelte:head>

<!-- Route-level auth comes from hooks.server.ts, which is deny-by-default:
     /chat is not in PUBLIC_PREFIXES, so a signed-out visitor is redirected to
     /login before this ever renders. The panel's signed-out state exists for the
     preview embedded on the public /this-season page. -->
<div class="chat-page">
	<PageHeader {kicker} title="Chat" titleSuffix="#general" size={48} tight phoneActionBesideTitle>
		Trash talk, trade offers and grievances. Reply in a thread to keep side arguments out of the
		main feed.
		{#snippet action()}
			{#if pill}
				<span class="status {pill.tone}" role="status">
					<span class="dot" aria-hidden="true"></span>{pill.label}
				</span>
			{/if}
		{/snippet}
	</PageHeader>

	<!-- The page is one screen under the band: the header at its own height
	     and the feed taking the rest, so the composer stays in view however
	     the header wraps. dvh, not vh: on mobile Safari the toolbar makes vh
	     taller than the visible viewport. -->
	<div class="feed">
		<ChatPanel {store} height="auto" />
	</div>
</div>

<style>
	/* 100dvh less the 68px band and <main>'s 28px and 40px; the floor keeps a
	   24rem card under a wrapped header on a short window. */
	.chat-page {
		box-sizing: border-box;
		height: calc(100dvh - 68px - 68px);
		min-height: calc(24rem + 160px);
		display: flex;
		flex-direction: column;
	}

	.feed {
		flex: 1 1 auto;
		min-height: 0;
		margin-top: 20px;
		display: flex;
		flex-direction: column;
	}

	/* Below 1024px the band is the 72px phone band. */
	@media (max-width: 1023.98px) {
		.chat-page {
			height: calc(100dvh - 72px - 68px);
		}
	}

	.status {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		padding: 6px 12px;
		border-radius: 999px;
		font-size: 13px;
		font-weight: 700;
		white-space: nowrap;
	}

	.dot {
		width: 8px;
		height: 8px;
		border-radius: 50%;
	}

	.live {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.live .dot {
		background: var(--color-field);
	}

	.connecting {
		background: var(--color-wash);
		color: var(--color-ink-2);
	}

	.connecting .dot {
		background: var(--color-muted);
	}

	.trouble {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}

	.trouble .dot {
		background: var(--color-pennant);
	}

	/* One screen under the 72px phone band: the header at its own height and
	   the card filling the rest, so the composer is always at your thumb. */
	@media (max-width: 639.98px) {
		.chat-page {
			height: calc(100dvh - 72px - 32px);
		}

		.feed {
			margin-top: 14px;
		}
	}
</style>
