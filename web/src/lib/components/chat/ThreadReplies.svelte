<script lang="ts">
	import MessageRow from './MessageRow.svelte';
	import Composer from './Composer.svelte';
	import type { ChatMessageView, EmojiType, ReactionGroup, RosterMember } from '$lib/chat/types';

	interface Props {
		replies: ChatMessageView[];
		loading: boolean;
		roster: RosterMember[];
		meManagerKey: number | null;
		canWrite: boolean;
		reactionsFor: (messageId: string) => ReactionGroup[];
		onSendReply: (content: string) => void;
		onToggleReaction: (messageId: string, emoji: string, emojiType: EmojiType) => void;
		onOpenPicker: (messageId: string, anchor: HTMLElement) => void;
		onEdit: (messageId: string, content: string) => void;
		onDelete: (messageId: string) => void;
		onRetry: (messageId: string) => void;
		onDiscard: (messageId: string) => void;
		onCollapse: () => void;
	}

	let {
		replies,
		loading,
		roster,
		meManagerKey,
		canWrite,
		reactionsFor,
		onSendReply,
		onToggleReaction,
		onOpenPicker,
		onEdit,
		onDelete,
		onRetry,
		onDiscard,
		onCollapse
	}: Props = $props();
</script>

<!-- The Thread card beside the feed (WP12 S12.5), drawn on the Chat board in
     place of the old inline replies. The data path is the same as inline: the
     panel's one store loads the thread (store.loadThread) and sends replies
     with parentMessageKey, so there's still one poll loop. -->
<aside aria-label="Thread" data-thread-for="expanded">
	<div class="flex items-center justify-between">
		<h2>Thread</h2>
		<button type="button" aria-label="Close thread" onclick={onCollapse}>×</button>
	</div>
	{#if loading && replies.length === 0}
		<p class="py-2 text-xs text-slate-500">Loading replies…</p>
	{:else}
		{#each replies as reply (reply.messageId)}
			<MessageRow
				message={reply}
				reactions={reactionsFor(reply.messageId)}
				{roster}
				{meManagerKey}
				{canWrite}
				allowThread={false}
				{onToggleReaction}
				{onOpenPicker}
				{onEdit}
				{onDelete}
				{onRetry}
				{onDiscard}
			/>
		{/each}
	{/if}

	{#if canWrite}
		<div class="mt-1">
			<Composer
				{roster}
				placeholder="Reply in thread"
				autofocus
				onSend={onSendReply}
			/>
		</div>
	{/if}
</aside>
