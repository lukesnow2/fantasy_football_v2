<script lang="ts">
	import { page } from '$app/state';
	import { RefreshCw } from 'lucide-svelte';
	import Button from '../ui/Button.svelte';
	import Card from '../ui/Card.svelte';
	import Composer from './Composer.svelte';
	import EmojiPickerPopover from './EmojiPickerPopover.svelte';
	import MessageList from './MessageList.svelte';
	import SignedOutPanel from './SignedOutPanel.svelte';
	import ThreadReplies from './ThreadReplies.svelte';
	import { DEFAULT_CHANNEL } from '$lib/chat/constants';
	import { createChatStore, type ChatStore } from '$lib/stores/chat.svelte';

	interface Props {
		channelId?: string;
		/**
		 * The row's height. The list needs a bounded height to scroll; the Chat
		 * page fits it to the viewport. 24rem is the floor either way.
		 */
		height?: string;
		/**
		 * The page's store, when the page also reads it (Chat's header shows the
		 * roster count and the connection). Without one the panel makes its own,
		 * so there's always one store and one poll loop.
		 */
		store?: ChatStore;
	}

	let {
		channelId = DEFAULT_CHANNEL,
		height = '38rem',
		store: providedStore
	}: Props = $props();

	const meManagerKey = $derived(page.data.authenticatedManager?.managerKey ?? null);
	const canWrite = $derived(meManagerKey !== null);

	// A fresh store when identity or channel changes, not a patched one: `mine` on
	// every reaction is computed server-side against the caller's manager key, so
	// signing in has to refetch rather than reinterpret what's already loaded.
	const ownStore = $derived.by(() =>
		providedStore ? null : createChatStore({ channelId, meManagerKey })
	);
	const store = $derived(providedStore ?? (ownStore as ChatStore));

	// The page starts a store it passes in; the panel starts only its own.
	$effect(() => ownStore?.start());

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
			closeThread();
			return;
		}
		expandedThreadId = messageId;
		void store.loadThread(messageId);
	}

	// Closing puts focus back on the chip that opened the thread.
	function closeThread() {
		const id = expandedThreadId;
		expandedThreadId = null;
		if (!id) return;
		queueMicrotask(() =>
			document.querySelector<HTMLElement>(`[data-thread-chip="${CSS.escape(id)}"]`)?.focus()
		);
	}

	const threadRoot = $derived(
		expandedThreadId ? (store.messages.find((m) => m.messageId === expandedThreadId) ?? null) : null
	);

</script>

<div class="chat-row" style:--chat-height={height}>
	<Card flush padding="0" phonePadding="0" label="Messages" class="messages">
		{#if !canWrite}
			<SignedOutPanel redirectTo={page.url.pathname} />
		{:else if store.connectionState === 'unauthorized'}
			<SignedOutPanel reason="not-a-member" />
		{:else if !store.initialLoadDone}
			<div class="loading" aria-busy="true" aria-label="Loading messages">
				{#each [80, 55, 70, 45, 62] as width, i (i)}
					<div class="ghost">
						<span class="ghost-avatar"></span>
						<span class="ghost-lines">
							<span class="bar" style:width="96px"></span>
							<span class="bar" style:width="{width}%"></span>
						</span>
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
				<div class="trouble" role="status">
					<span>{store.connectionState === 'offline' ? "You're offline." : 'Reconnecting…'}</span>
					<Button variant="link" onclick={() => void store.refresh()}>
						<RefreshCw size={14} aria-hidden="true" /> Retry now
					</Button>
				</div>
			{/if}

			<Composer
				bind:this={composer}
				roster={store.roster}
				onSend={(content) => void store.send(content)}
			/>
		{/if}
	</Card>

{#if threadRoot && canWrite && store.connectionState !== 'unauthorized'}
	{@const root = threadRoot}
	<ThreadReplies
		replies={store.repliesFor(root.messageId)}
		loading={store.isThreadLoading(root.messageId)}
		roster={store.roster}
		meManagerKey={store.meManagerKey}
		{canWrite}
		reactionsFor={(id) => store.reactionsFor(id)}
		onSendReply={(content) =>
			void store.send(content, { parentMessageKey: root.messageKey ?? undefined })}
		onToggleReaction={(id, emoji, emojiType) => void store.toggleReaction(id, emoji, emojiType)}
		onOpenPicker={openPicker}
		onEdit={(id, content) => void store.edit(id, content)}
		onDelete={(id) => void store.remove(id)}
		onRetry={(id) => void store.retry(id)}
		onDiscard={(id) => store.discard(id)}
		onCollapse={closeThread}
	/>
{/if}
</div>

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

<style>
	/* The Chat board's row: the messages card and, while a thread is open, the
	   Thread card beside it (WP12 S12.1). */
	.chat-row {
		display: flex;
		flex-wrap: wrap;
		gap: 16px;
		align-items: stretch;
		height: var(--chat-height);
		min-height: 24rem;
	}

	.chat-row :global(.messages) {
		flex: 999 1 600px;
		min-width: 0;
		height: 100%;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.loading {
		flex: 1;
		padding: 18px 20px;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.ghost {
		display: flex;
		gap: 12px;
	}

	.ghost-avatar {
		flex: none;
		width: 40px;
		height: 40px;
		border-radius: 50%;
		background: var(--color-wash);
	}

	.ghost-lines {
		flex: 1;
		display: flex;
		flex-direction: column;
		gap: 10px;
		padding-top: 4px;
	}

	.bar {
		display: block;
		height: 11px;
		border-radius: 6px;
		background: var(--color-wash);
	}

	.trouble {
		flex: none;
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 12px;
		padding: 8px 20px;
		border-top: 1px solid var(--color-pennant-line);
		background: var(--color-pennant-tint);
		font-size: 14px;
		color: var(--color-pennant-dark);
	}

	.trouble :global(.btn) {
		gap: 6px;
		font-size: 14px;
	}

	@media (max-width: 639.98px) {
		.chat-row {
			flex: 1 1 auto;
			height: auto;
			min-height: 24rem;
		}

		.chat-row :global(.messages) {
			height: auto;
		}

		.loading {
			padding: 14px;
		}

		.ghost {
			gap: 10px;
		}

		.ghost-avatar {
			width: 32px;
			height: 32px;
		}

		.trouble {
			padding: 8px 14px;
		}

		.trouble :global(.btn) {
			min-height: 44px;
		}
	}
</style>
