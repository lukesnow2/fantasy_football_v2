<script lang="ts">
	import { AlertCircle, MessageSquare } from 'lucide-svelte';
	import ChatAvatar from './ChatAvatar.svelte';
	import MessageActions from './MessageActions.svelte';
	import ReactionBar from './ReactionBar.svelte';
	import { formatMessageTime, formatRelative } from '$lib/chat/format';
	import { renderMarkdown } from '$lib/chat/markdown';
	import { applyMentionMarkup } from '$lib/chat/mentions';
	import type { ChatMessageView, EmojiType, ReactionGroup, RosterMember } from '$lib/chat/types';

	interface Props {
		message: ChatMessageView;
		reactions: ReactionGroup[];
		roster: RosterMember[];
		meManagerKey: number | null;
		canWrite: boolean;
		/** Continuation of the previous message: no avatar, no name, hover-only time. */
		compact?: boolean;
		/** Replies can't themselves be replied to, so they hide the thread affordances. */
		allowThread?: boolean;
		threadExpanded?: boolean;
		onToggleReaction: (messageId: string, emoji: string, emojiType: EmojiType) => void;
		onOpenPicker: (messageId: string, anchor: HTMLElement) => void;
		onOpenThread?: (messageId: string) => void;
		onEdit: (messageId: string, content: string) => void;
		onDelete: (messageId: string) => void;
		onRetry: (messageId: string) => void;
		onDiscard: (messageId: string) => void;
	}

	let {
		message,
		reactions,
		roster,
		meManagerKey,
		canWrite,
		compact = false,
		allowThread = true,
		threadExpanded = false,
		onToggleReaction,
		onOpenPicker,
		onOpenThread,
		onEdit,
		onDelete,
		onRetry,
		onDiscard
	}: Props = $props();

	const isAuthor = $derived(message.authorKey === meManagerKey);
	const isDeleted = $derived(message.deletedAt !== null);
	const mentionsMe = $derived(meManagerKey !== null && message.mentions.includes(meManagerKey));
	const isPending = $derived(message.sendState === 'pending');
	const isFailed = $derived(message.sendState === 'failed');

	// Mention markup goes on *after* sanitizing. Before, and DOMPurify strips the
	// spans we just added; as a string replace, and it highlights @names inside
	// code fences and href attributes.
	const html = $derived(
		isDeleted ? '' : applyMentionMarkup(renderMarkdown(message.content), roster, meManagerKey)
	);

	let editing = $state(false);
	let draft = $state('');
	let textarea = $state<HTMLTextAreaElement | null>(null);

	export function beginEdit() {
		// $state initialisers don't re-run, so the draft is synced explicitly on
		// entry rather than derived from the message.
		draft = message.content;
		editing = true;
		queueMicrotask(() => {
			textarea?.focus();
			textarea?.setSelectionRange(draft.length, draft.length);
		});
	}

	function saveEdit() {
		const next = draft.trim();
		editing = false;
		if (next !== '' && next !== message.content) onEdit(message.messageId, next);
	}

	function cancelEdit() {
		editing = false;
		draft = message.content;
	}

	function onEditKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			saveEdit();
		} else if (event.key === 'Escape') {
			event.preventDefault();
			cancelEdit();
		}
	}
</script>

<div
	class="group relative flex gap-3 rounded-lg px-2 transition-colors hover:bg-slate-800/40
		{compact ? 'py-0.5' : 'pt-2 pb-1'}
		{mentionsMe ? 'border-l-2 border-amber-500 bg-amber-500/5' : ''}
		{isPending ? 'opacity-60' : ''}
		{isFailed ? 'border-l-2 border-red-500 bg-red-500/5 opacity-80' : ''}"
>
	{#if compact}
		<!-- Reserve the avatar gutter so text stays aligned, and reveal the
		     timestamp there on hover, the way Slack does. -->
		<div class="w-9 shrink-0 pt-0.5 text-right">
			<span
				class="text-[10px] tabular-nums text-slate-500 opacity-0 transition-opacity group-hover:opacity-100"
			>
				{formatMessageTime(message.createdAt)}
			</span>
		</div>
	{:else}
		<ChatAvatar
			managerName={message.authorName}
			displayName={message.authorDisplayName}
			profileImageUrl={message.authorProfileImage}
		/>
	{/if}

	<div class="min-w-0 flex-1">
		{#if !compact}
			<div class="flex items-baseline gap-2">
				<span class="text-sm font-semibold text-white">{message.authorDisplayName}</span>
				<span class="text-[11px] tabular-nums text-slate-500">
					{formatMessageTime(message.createdAt)}
				</span>
			</div>
		{/if}

		{#if isDeleted}
			<p class="text-sm italic text-slate-500">This message was deleted.</p>
		{:else if editing}
			<div class="mt-1">
				<textarea
					bind:this={textarea}
					bind:value={draft}
					onkeydown={onEditKeydown}
					rows="2"
					class="w-full resize-none rounded-lg border border-slate-600 bg-slate-900 px-3 py-2 text-sm text-white focus:border-amber-500 focus:outline-none"
				></textarea>
				<div class="mt-1 flex items-center gap-2 text-xs">
					<button
						type="button"
						onclick={saveEdit}
						class="rounded bg-amber-500 px-2 py-1 font-medium text-black hover:bg-amber-400"
					>
						Save
					</button>
					<button type="button" onclick={cancelEdit} class="text-slate-400 hover:text-white">
						Cancel
					</button>
					<span class="text-slate-500">Enter to save, Escape to cancel</span>
				</div>
			</div>
		{:else}
			<div class="chat-markdown text-sm leading-relaxed break-words text-slate-200">
				<!-- Sanitized by DOMPurify in renderMarkdown; see lib/chat/markdown.ts -->
				{@html html}
			</div>
			{#if message.editedAt !== null}
				<span class="text-[10px] text-slate-500">(edited)</span>
			{/if}
		{/if}

		{#if !isDeleted}
			<ReactionBar
				groups={reactions}
				{meManagerKey}
				{canWrite}
				onToggle={(emoji, emojiType) => onToggleReaction(message.messageId, emoji, emojiType)}
				onOpenPicker={(anchor) => onOpenPicker(message.messageId, anchor)}
			/>
		{/if}

		{#if isFailed}
			<div class="mt-1 flex items-center gap-2 text-xs text-red-300">
				<AlertCircle class="h-3.5 w-3.5" />
				<span>{message.failureReason ?? 'Failed to send.'}</span>
				<button
					type="button"
					onclick={() => onRetry(message.messageId)}
					class="font-medium underline hover:text-red-200">Retry</button
				>
				<button
					type="button"
					onclick={() => onDiscard(message.messageId)}
					class="text-slate-400 underline hover:text-slate-200">Discard</button
				>
			</div>
		{/if}

		{#if allowThread && message.replyCount > 0 && onOpenThread}
			<button
				type="button"
				onclick={() => onOpenThread(message.messageId)}
				class="mt-1 flex items-center gap-1.5 rounded px-1 py-0.5 text-xs font-medium text-blue-300 hover:bg-slate-700/50 hover:text-blue-200"
			>
				<MessageSquare class="h-3.5 w-3.5" />
				{message.replyCount}
				{message.replyCount === 1 ? 'reply' : 'replies'}
				{#if message.lastReplyAt}
					<span class="font-normal text-slate-500">· last {formatRelative(message.lastReplyAt)}</span>
				{/if}
				<span class="font-normal text-slate-500">{threadExpanded ? '· hide' : ''}</span>
			</button>
		{/if}
	</div>

	{#if canWrite && !isDeleted && !editing && !isPending}
		<MessageActions
			{isAuthor}
			canReply={allowThread && onOpenThread !== undefined}
			onReact={(emoji, emojiType) => onToggleReaction(message.messageId, emoji, emojiType)}
			onOpenPicker={(anchor) => onOpenPicker(message.messageId, anchor)}
			onReply={() => onOpenThread?.(message.messageId)}
			onEdit={beginEdit}
			onDelete={() => onDelete(message.messageId)}
		/>
	{/if}
</div>

<style>
	/* Scoped to the rendered markdown, which arrives as {@html} and so is outside
	   Svelte's per-component class scoping — :global is required here. */
	.chat-markdown :global(p) {
		margin: 0;
	}
	.chat-markdown :global(p + p) {
		margin-top: 0.375rem;
	}
	.chat-markdown :global(a) {
		color: #7dd3fc;
		text-decoration: underline;
		text-underline-offset: 2px;
	}
	.chat-markdown :global(a:hover) {
		color: #bae6fd;
	}
	.chat-markdown :global(code) {
		border-radius: 0.25rem;
		background: rgb(15 23 42 / 0.8);
		padding: 0.0625rem 0.25rem;
		font-size: 0.8125rem;
		color: #fbbf24;
	}
	.chat-markdown :global(pre) {
		margin: 0.375rem 0;
		overflow-x: auto;
		border-radius: 0.5rem;
		background: rgb(15 23 42 / 0.9);
		padding: 0.5rem 0.75rem;
	}
	.chat-markdown :global(pre code) {
		background: none;
		padding: 0;
		color: #e2e8f0;
	}
	.chat-markdown :global(blockquote) {
		margin: 0.25rem 0;
		border-left: 2px solid #475569;
		padding-left: 0.625rem;
		color: #cbd5e1;
	}
	.chat-markdown :global(ul),
	.chat-markdown :global(ol) {
		margin: 0.25rem 0;
		padding-left: 1.25rem;
	}
	.chat-markdown :global(ul) {
		list-style: disc;
	}
	.chat-markdown :global(ol) {
		list-style: decimal;
	}
</style>
