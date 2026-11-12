<script lang="ts">
	import { MediaQuery } from 'svelte/reactivity';
	import Card from '../ui/Card.svelte';
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

	// Below the side-by-side width the card is a full-screen sheet, so Escape
	// closes it the way it closes any sheet.
	const sheet = new MediaQuery('max-width: 983.98px');
</script>

<!-- The Thread card beside the feed (WP12 S12.5), drawn on the Chat board in
     place of the old inline replies. The data path is the same as inline: the
     panel's one store loads the thread (store.loadThread) and sends replies
     with parentMessageKey, so there's still one poll loop. Narrower than the
     row can hold both cards, it covers the screen as a sheet (S12.10). -->
<svelte:window
	onkeydown={(event) => {
		if (event.key === 'Escape' && !event.defaultPrevented && sheet.current) onCollapse();
	}}
/>

<Card as="aside" flush padding="0" phonePadding="0" label="Thread" class="thread">
	<div class="thread-head" data-thread-for="expanded">
		<h2>Thread</h2>
		<button type="button" class="close" aria-label="Close thread" onclick={onCollapse}>
			<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"
				><path d="M6 6l12 12M18 6L6 18"></path></svg
			>
		</button>
	</div>

	<div class="replies">
		{#if loading && replies.length === 0}
			<p class="loading">Loading replies…</p>
		{:else}
			{#each replies as reply (reply.messageId)}
				<MessageRow
					message={reply}
					reactions={reactionsFor(reply.messageId)}
					{roster}
					{meManagerKey}
					{canWrite}
					size="reply"
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
	</div>

	{#if canWrite}
		<div class="reply-box">
			<Composer {roster} size="small" placeholder="Reply in thread" autofocus onSend={onSendReply} />
		</div>
	{/if}
</Card>

<style>
	:global(.chat-row) > :global(.thread) {
		flex: 1 1 320px;
		min-width: 0;
		height: 100%;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.thread-head {
		flex: none;
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 16px 20px;
		border-bottom: 1px solid var(--color-line);
	}

	h2 {
		margin: 0;
		font-family: var(--font-slab);
		font-size: 22px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.close {
		width: 36px;
		height: 36px;
		padding: 0;
		border: 0;
		border-radius: 8px;
		background: transparent;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		color: var(--color-ink-2);
		cursor: pointer;
	}

	.close:hover {
		background: var(--color-field-wash);
	}

	.close:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: -2px;
	}

	.replies {
		flex: 1 1 auto;
		min-height: 0;
		overflow-y: auto;
		padding: 8px 20px;
	}

	.loading {
		margin: 10px 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.reply-box {
		flex: none;
		padding: 14px 20px 18px;
		border-top: 1px solid var(--color-line);
	}

	/* Where the row can't hold both cards side by side (936px of row, so a
	   984px viewport with the 24px gutters), the thread is a full-height sheet
	   over everything, band included. */
	@media (max-width: 983.98px) {
		:global(.chat-row) > :global(.thread) {
			position: fixed;
			inset: 0;
			z-index: 100;
			height: 100dvh;
			border: 0;
			border-radius: 0;
		}

		.thread-head {
			padding: 10px 10px 10px 16px;
		}

		.close {
			width: 44px;
			height: 44px;
		}

		.replies {
			padding: 8px 16px;
		}

		.reply-box {
			padding: 12px 16px 16px;
		}
	}
</style>
