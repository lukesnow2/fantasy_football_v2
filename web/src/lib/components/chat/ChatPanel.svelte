<script lang="ts">
	import { page } from '$app/state';
	import { MessageSquare, RefreshCw } from 'lucide-svelte';
	import Composer from './Composer.svelte';
	import EmojiPickerPopover from './EmojiPickerPopover.svelte';
	import MessageList from './MessageList.svelte';
	import SignedOutPanel from './SignedOutPanel.svelte';
	import { DEFAULT_CHANNEL } from '$lib/chat/constants';
	import { createChatStore } from '$lib/stores/chat.svelte';

	interface Props {
		channelId?: string;
		/** Tailwind height for the panel body. The list needs a bounded height to scroll. */
		heightClass?: string;
		title?: string;
	}

	let {
		channelId = DEFAULT_CHANNEL,
		heightClass = 'h-[32rem] lg:h-[38rem]',
		title = 'League Chat'
	}: Props = $props();

	const meManagerKey = $derived(page.data.authenticatedManager?.managerKey ?? null);
	const canWrite = $derived(meManagerKey !== null);

	// A fresh store when identity or channel changes, not a patched one: `mine` on
	// every reaction is computed server-side against the caller's manager key, so
	// signing in has to refetch rather than reinterpret what's already loaded.
	const store = $derived.by(() => createChatStore({ channelId, meManagerKey }));

	$effect(() => store.start());

	// One picker for the whole panel. Rendering one per message would mount a
	// 288px web component (and its IndexedDB handle) for every row on screen.
	let pickerFor = $state<string | null>(null);
	let pickerAnchor = $state<HTMLElement | null>(null);
	let expandedThreadId = $state<string | null>(null);
	let list = $state<ReturnType<typeof MessageList> | null>(null);
	let composer = $state<ReturnType<typeof Composer> | null>(null);

	function openPicker(messageId: string, anchor: HTMLElement) {
		pickerFor = messageId;
		pickerAnchor = anchor;
	}

	function closePicker() {
		pickerFor = null;
		pickerAnchor = null;
	}

	function toggleThread(messageId: string) {
		if (expandedThreadId === messageId) {
			expandedThreadId = null;
			return;
		}
		expandedThreadId = messageId;
		void store.loadThread(messageId);
	}

	const connectionLabel = $derived(
		store.connectionState === 'live'
			? 'Live'
			: store.connectionState === 'connecting'
				? 'Connecting…'
				: store.connectionState === 'reconnecting'
					? 'Reconnecting…'
					: store.connectionState === 'offline'
						? 'Offline'
						: ''
	);
</script>

<section
	class="flex flex-col overflow-hidden rounded-xl border border-slate-700/50 bg-slate-800/50 {heightClass}"
>
	<header class="flex shrink-0 items-center justify-between border-b border-slate-700/60 px-4 py-3">
		<h2 class="flex items-center gap-2 text-lg font-bold text-white">
			<span class="flex h-7 w-7 items-center justify-center rounded-lg bg-green-500/15">
				<MessageSquare class="h-4 w-4 text-green-400" />
			</span>
			{title}
		</h2>

		{#if canWrite && connectionLabel}
			<span class="flex items-center gap-1.5 text-[11px] text-slate-500">
				<span
					class="h-1.5 w-1.5 rounded-full
						{store.connectionState === 'live'
						? 'bg-green-400'
						: store.connectionState === 'offline'
							? 'bg-red-400'
							: 'bg-amber-400'}"
				></span>
				{connectionLabel}
			</span>
		{/if}
	</header>

	{#if !canWrite}
		<SignedOutPanel redirectTo={page.url.pathname} />
	{:else if store.connectionState === 'unauthorized'}
		<SignedOutPanel reason="not-a-member" />
	{:else if !store.initialLoadDone}
		<div class="flex-1 space-y-3 p-4">
			{#each Array(5) as _, i}
				<div class="flex gap-3">
					<div class="h-9 w-9 shrink-0 animate-pulse rounded-lg bg-slate-700/50"></div>
					<div class="flex-1 space-y-2">
						<div class="h-3 w-24 animate-pulse rounded bg-slate-700/50"></div>
						<div
							class="h-3 animate-pulse rounded bg-slate-700/40"
							style="width: {[80, 55, 70, 45, 62][i]}%"
						></div>
					</div>
				</div>
			{/each}
		</div>
	{:else}
		<MessageList
			bind:this={list}
			{store}
			{canWrite}
			{expandedThreadId}
			onOpenPicker={openPicker}
			onToggleThread={toggleThread}
		/>

		{#if store.connectionState === 'reconnecting' || store.connectionState === 'offline'}
			<!-- A strip, never a full-panel error: the messages already loaded are
			     still worth reading while the connection sorts itself out. -->
			<div
				class="flex shrink-0 items-center justify-between gap-2 border-t border-amber-600/40 bg-amber-900/20 px-4 py-2 text-xs text-amber-200"
			>
				<span>{store.connectionState === 'offline' ? "You're offline." : 'Reconnecting…'}</span>
				<button
					type="button"
					onclick={() => void store.refresh()}
					class="flex items-center gap-1 font-medium underline hover:text-amber-100"
				>
					<RefreshCw class="h-3 w-3" /> Retry now
				</button>
			</div>
		{/if}

		<Composer
			bind:this={composer}
			roster={store.roster}
			onSend={(content) => void store.send(content)}
		/>
	{/if}
</section>

{#if pickerFor}
	<EmojiPickerPopover
		anchor={pickerAnchor}
		onSelect={({ emoji, emojiType }) => {
			const messageId = pickerFor;
			closePicker();
			if (messageId) void store.toggleReaction(messageId, emoji, emojiType);
		}}
		onClose={closePicker}
	/>
{/if}
