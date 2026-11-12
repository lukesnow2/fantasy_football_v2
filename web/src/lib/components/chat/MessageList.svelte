<script lang="ts">
	import { tick } from 'svelte';
	import { ArrowDown } from 'lucide-svelte';
	import Button from '../ui/Button.svelte';
	import MessageRow from './MessageRow.svelte';
	import { BOTTOM_THRESHOLD_PX } from '$lib/chat/constants';
	import { dayKey, formatDayDivider, newestRepliers, shouldGroupWith } from '$lib/chat/format';
	import type { ChatStore } from '$lib/stores/chat.svelte';
	import type { EmojiType } from '$lib/chat/types';

	interface Props {
		store: ChatStore;
		canWrite: boolean;
		expandedThreadId: string | null;
		onOpenPicker: (messageId: string, anchor: HTMLElement) => void;
		onToggleThread: (messageId: string) => void;
	}

	let { store, canWrite, expandedThreadId, onOpenPicker, onToggleThread }: Props = $props();

	let container = $state<HTMLDivElement | null>(null);
	let lastSeenId = $state<string | null>(null);

	const messages = $derived(store.messages);
	const boundary = $derived(store.unreadBoundaryKey);

	function isAtBottom(el: HTMLElement): boolean {
		return el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_THRESHOLD_PX;
	}

	let scrollFrame = 0;
	function onScroll() {
		if (!container) return;
		// rAF-throttled: a scroll handler that runs per event fires dozens of times
		// per flick and every one of them writes reactive state.
		if (scrollFrame) return;
		scrollFrame = requestAnimationFrame(() => {
			scrollFrame = 0;
			if (!container) return;
			store.setAtBottom(isAtBottom(container));
			const newest = messages[messages.length - 1];
			if (newest?.messageKey) store.markRead(newest.messageKey);
		});
	}

	function scrollToBottom(behavior: ScrollBehavior = 'auto') {
		if (!container) return;
		const reduceMotion =
			typeof matchMedia !== 'undefined' &&
			matchMedia('(prefers-reduced-motion: reduce)').matches;
		container.scrollTo({ top: container.scrollHeight, behavior: reduceMotion ? 'auto' : behavior });
	}

	export function jumpToBottom() {
		store.clearNewMessageCount();
		scrollToBottom('smooth');
	}

	// Autoscroll. Two rules, and the distinction between them is the whole thing:
	// your own message always pulls the view down, someone else's only does when
	// you were already at the bottom.
	$effect(() => {
		const newest = messages[messages.length - 1];
		if (!newest || newest.messageId === lastSeenId) return;

		const isFirstPaint = lastSeenId === null;
		const isMine = newest.authorKey === store.meManagerKey;
		lastSeenId = newest.messageId;

		if (isFirstPaint || isMine || store.atBottom) {
			// After the DOM has the new row, or scrollHeight is still the old value.
			void tick().then(() => scrollToBottom(isFirstPaint ? 'auto' : 'smooth'));
		}
	});

	async function loadOlder() {
		if (!container) return;
		// Preserve the reader's position: measure before, restore after, or the
		// viewport jumps by the height of everything just prepended.
		const before = container.scrollHeight;
		const previousTop = container.scrollTop;
		await store.loadOlder();
		await tick();
		if (container) container.scrollTop = previousTop + (container.scrollHeight - before);
	}

	function handleToggleReaction(messageId: string, emoji: string, emojiType: EmojiType) {
		void store.toggleReaction(messageId, emoji, emojiType);
	}

	function beginEditLast() {
		// Handled by the panel; kept here so MessageRow's contract stays local.
	}
</script>

<div class="list-wrap">
	<div
		bind:this={container}
		onscroll={onScroll}
		class="list"
		style="overflow-anchor: none;"
		role="log"
		aria-live="polite"
		aria-label="League chat messages"
	>
		{#if store.hasMoreHistory}
			<div class="older">
				<Button variant="pill" onclick={loadOlder} disabled={store.loadingOlder}>
					{store.loadingOlder ? 'Loading…' : 'Load earlier messages'}
				</Button>
			</div>
		{/if}

		{#if messages.length === 0}
			<div class="empty">
				<p class="empty-title">No messages yet.</p>
				<p class="empty-line">Be the first to say something.</p>
			</div>
		{/if}

		{#each messages as message, index (message.messageId)}
			{@const previous = index === 0 ? null : messages[index - 1]}
			{@const newDay = !previous || dayKey(previous.createdAt) !== dayKey(message.createdAt)}
			{@const firstUnread =
				boundary !== null &&
				message.messageKey !== null &&
				message.messageKey > boundary &&
				(!previous || previous.messageKey === null || previous.messageKey <= boundary)}
			{@const open = expandedThreadId === message.messageId}

			{#if newDay}
				<div class="day">
					<span class="rule" aria-hidden="true"></span>
					{formatDayDivider(message.createdAt)}
					<span class="rule" aria-hidden="true"></span>
				</div>
			{/if}

			{#if firstUnread}
				<div class="unread">
					<span class="rule" aria-hidden="true"></span>New
				</div>
			{/if}

			<MessageRow
				{message}
				reactions={store.reactionsFor(message.messageId)}
				roster={store.roster}
				meManagerKey={store.meManagerKey}
				{canWrite}
				compact={!newDay && !firstUnread && shouldGroupWith(previous, message)}
				threadExpanded={open}
				repliers={open ? newestRepliers(store.repliesFor(message.messageId)) : []}
				onToggleReaction={handleToggleReaction}
				{onOpenPicker}
				onOpenThread={onToggleThread}
				onEdit={(id, content) => void store.edit(id, content)}
				onDelete={(id) => void store.remove(id)}
				onRetry={(id) => void store.retry(id)}
				onDiscard={(id) => store.discard(id)}
			/>
		{/each}
	</div>

	{#if store.newMessageCount > 0}
		<button type="button" class="jump" onclick={jumpToBottom}>
			<ArrowDown size={14} strokeWidth={2.5} aria-hidden="true" />
			{store.newMessageCount} new {store.newMessageCount === 1 ? 'message' : 'messages'}
		</button>
	{/if}
</div>

<style>
	.list-wrap {
		position: relative;
		flex: 1 1 auto;
		min-height: 0;
		display: flex;
		flex-direction: column;
	}

	.list {
		position: relative;
		flex: 1 1 auto;
		min-height: 0;
		overflow-y: auto;
		padding: 8px 20px;
	}

	.older {
		padding: 6px 0;
		text-align: center;
	}

	.day {
		position: sticky;
		top: 0;
		z-index: 4;
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 8px 0 4px;
		background: var(--color-card);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.day .rule {
		flex: 1;
		border-top: 1px solid var(--color-line-soft);
	}

	.unread {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 6px 0;
		font-size: 12px;
		font-weight: 700;
		color: var(--color-pennant);
	}

	.unread .rule {
		flex: 1;
		border-top: 1.5px solid var(--color-pennant);
	}

	.empty {
		display: flex;
		height: 100%;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		padding: 40px 0;
		text-align: center;
	}

	.empty-title {
		margin: 0;
		font-size: 15px;
		font-weight: 600;
		color: var(--color-ink);
	}

	.empty-line {
		margin: 4px 0 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.jump {
		position: absolute;
		bottom: 10px;
		left: 50%;
		z-index: 5;
		transform: translateX(-50%);
		display: inline-flex;
		align-items: center;
		gap: 6px;
		min-height: 32px;
		padding: 0 14px;
		border: 0;
		border-radius: 999px;
		background: var(--color-field);
		color: var(--color-white);
		font-family: var(--font-sans);
		font-size: 13px;
		font-weight: 700;
		box-shadow: var(--shadow-toolbar);
		cursor: pointer;
	}

	@media (max-width: 639.98px) {
		.list {
			padding: 0 14px 8px;
		}

		.jump {
			min-height: 44px;
		}
	}
</style>
