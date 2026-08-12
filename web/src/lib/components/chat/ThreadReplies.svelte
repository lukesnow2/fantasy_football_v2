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

<!-- Inline under the root message, not a side panel: one scroll container, one
     cursor, one poll loop. The rail on the left is what says "these belong to
     the message above". -->
<div data-thread-for="expanded" class="ml-6 border-l-2 border-slate-700 pl-3">
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
				placeholder="Reply…"
				autofocus
				onSend={onSendReply}
			/>
		</div>
	{/if}

	<button
		type="button"
		onclick={onCollapse}
		class="mt-1 px-2 py-1 text-xs text-slate-400 hover:text-slate-200"
	>
		Hide replies
	</button>
</div>
