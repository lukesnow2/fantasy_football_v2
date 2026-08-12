<script lang="ts">
	import { tick } from 'svelte';
	import { ArrowDown, MessageSquare } from 'lucide-svelte';
	import MessageRow from './MessageRow.svelte';
	import ThreadReplies from './ThreadReplies.svelte';
	import { BOTTOM_THRESHOLD_PX } from '$lib/chat/constants';
	import { dayKey, formatDayDivider, shouldGroupWith } from '$lib/chat/format';
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

	// Expanding a thread can add a few hundred pixels below the fold. Without
	// this, the replies you just asked for open off-screen and the panel looks
	// like it did nothing.
	let lastExpandedId = $state<string | null>(null);
	$effect(() => {
		const id = expandedThreadId;
		if (id === lastExpandedId) return;
		lastExpandedId = id;
		if (!id || !container) return;

		void tick().then(() => {
			const thread = container?.querySelector('[data-thread-for]');
			thread?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
		});
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

<div
	bind:this={container}
	onscroll={onScroll}
	class="relative min-h-0 flex-1 overflow-y-auto px-2 py-2"
	style="overflow-anchor: none;"
	role="log"
	aria-live="polite"
	aria-label="League chat messages"
>
	{#if store.hasMoreHistory}
		<div class="flex justify-center py-2">
			<button
				type="button"
				onclick={loadOlder}
				disabled={store.loadingOlder}
				class="rounded-full border border-slate-600 bg-slate-800 px-3 py-1 text-xs text-slate-300 hover:bg-slate-700 disabled:opacity-50"
			>
				{store.loadingOlder ? 'Loading…' : 'Load earlier messages'}
			</button>
		</div>
	{/if}

	{#if messages.length === 0}
		<div class="flex h-full flex-col items-center justify-center gap-2 py-10 text-center">
			<MessageSquare class="h-8 w-8 text-slate-600" />
			<p class="text-sm font-medium text-slate-300">No messages yet.</p>
			<p class="text-xs text-slate-500">Be the first to say something.</p>
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

		{#if newDay}
			<div class="sticky top-0 z-10 my-2 flex items-center gap-3 py-1">
				<div class="h-px flex-1 bg-slate-700"></div>
				<span class="rounded-full border border-slate-700 bg-slate-800 px-2.5 py-0.5 text-[11px] font-medium text-slate-400">
					{formatDayDivider(message.createdAt)}
				</span>
				<div class="h-px flex-1 bg-slate-700"></div>
			</div>
		{/if}

		{#if firstUnread}
			<div class="my-2 flex items-center gap-3">
				<div class="h-px flex-1 bg-amber-500/60"></div>
				<span class="text-[11px] font-semibold uppercase tracking-wide text-amber-400">New</span>
			</div>
		{/if}

		<MessageRow
			{message}
			reactions={store.reactionsFor(message.messageId)}
			roster={store.roster}
			meManagerKey={store.meManagerKey}
			{canWrite}
			compact={!newDay && !firstUnread && shouldGroupWith(previous, message)}
			threadExpanded={expandedThreadId === message.messageId}
			onToggleReaction={handleToggleReaction}
			{onOpenPicker}
			onOpenThread={onToggleThread}
			onEdit={(id, content) => void store.edit(id, content)}
			onDelete={(id) => void store.remove(id)}
			onRetry={(id) => void store.retry(id)}
			onDiscard={(id) => store.discard(id)}
		/>

		{#if expandedThreadId === message.messageId}
			<ThreadReplies
				replies={store.repliesFor(message.messageId)}
				loading={store.isThreadLoading(message.messageId)}
				roster={store.roster}
				meManagerKey={store.meManagerKey}
				{canWrite}
				reactionsFor={(id) => store.reactionsFor(id)}
				onSendReply={(content) =>
					void store.send(content, { parentMessageKey: message.messageKey ?? undefined })}
				onToggleReaction={handleToggleReaction}
				{onOpenPicker}
				onEdit={(id, content) => void store.edit(id, content)}
				onDelete={(id) => void store.remove(id)}
				onRetry={(id) => void store.retry(id)}
				onDiscard={(id) => store.discard(id)}
				onCollapse={() => onToggleThread(message.messageId)}
			/>
		{/if}
	{/each}
</div>

{#if store.newMessageCount > 0}
	<div class="pointer-events-none relative">
		<button
			type="button"
			onclick={jumpToBottom}
			class="pointer-events-auto absolute bottom-2 left-1/2 z-20 flex -translate-x-1/2 items-center gap-1.5 rounded-full bg-amber-500 px-3 py-1.5 text-xs font-medium text-black shadow-lg hover:bg-amber-400"
		>
			<ArrowDown class="h-3.5 w-3.5" />
			{store.newMessageCount} new {store.newMessageCount === 1 ? 'message' : 'messages'}
		</button>
	</div>
{/if}
