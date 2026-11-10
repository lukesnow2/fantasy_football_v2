<script lang="ts">
	import { page } from '$app/state';
	import Avatar from '../ui/Avatar.svelte';
	import Card from '../ui/Card.svelte';
	import SignedOutPanel from './SignedOutPanel.svelte';
	import { formatRelative, toPlainText } from '$lib/chat/format';
	import type { ChatMessageDTO } from '$lib/chat/types';

	/**
	 * A read-only peek at the chat for the public This Season page.
	 *
	 * Deliberately not a ChatPanel: /this-season is public and /api/chat is not,
	 * so mounting the real panel here would start a poll loop that 401s on every
	 * tick for every signed-out visitor. This fetches once, only when signed in,
	 * and sends people to /chat to actually talk.
	 */

	const canRead = $derived(page.data.authenticatedManager?.managerKey != null);

	let messages = $state<ChatMessageDTO[]>([]);
	let loading = $state(true);

	$effect(() => {
		if (!canRead) {
			loading = false;
			return;
		}

		let cancelled = false;
		(async () => {
			try {
				const response = await fetch('/api/chat/messages?channelId=general&limit=5');
				if (!response.ok || cancelled) return;
				const data = await response.json();
				messages = data.messages ?? [];
			} catch {
				// The panel degrades to its empty state; /chat is one click away.
			} finally {
				if (!cancelled) loading = false;
			}
		})();

		return () => {
			cancelled = true;
		};
	});

	const recent = $derived(messages.slice(-4));
</script>

<Card
	title="League chat"
	class="chat-preview"
	headGap={14}
	phonePadding="6px 16px 16px"
	phoneHeadGap={6}
	phoneHeadAlign="center"
>
	{#snippet action()}<a class="field-link open-chat" href="/chat">Open chat →</a>{/snippet}

	{#if !canRead}
		<!-- "You'll land right back here" is only true with this page as the redirect. -->
		<SignedOutPanel well redirectTo={page.url.pathname} />
	{:else if loading}
		<div class="rows" aria-busy="true">
			{#each [70, 50, 62] as width (width)}
				<div class="row">
					<span class="dot"></span>
					<span class="lines"
						><span class="bar short"></span><span class="bar" style:width="{width}%"></span></span
					>
				</div>
			{/each}
		</div>
	{:else if recent.length === 0}
		<p class="empty">
			Nothing said yet. <a class="field-link" href="/chat">Start it off.</a>
		</p>
	{:else}
		<ul class="rows">
			{#each recent as message (message.messageId)}
				<li class="row">
					<Avatar name={message.authorName ?? message.authorDisplayName} size={26} />
					<div class="text">
						<div class="who">
							<span class="author">{message.authorDisplayName}</span>
							<span class="when">{formatRelative(message.createdAt)}</span>
						</div>
						<!-- Stripped to plain text on purpose: the preview is a glance, and
						     rendering markdown would ship marked + DOMPurify to a public page. -->
						<p class="body">{toPlainText(message.content)}</p>
					</div>
				</li>
			{/each}
		</ul>
	{/if}
</Card>

<style>
	:global(.chat-preview) {
		display: flex;
		flex-direction: column;
	}

	.open-chat {
		font-size: 14px;
		font-weight: 600;
	}

	.rows {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.row {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 9px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.text {
		flex: 1;
		min-width: 0;
	}

	.who {
		display: flex;
		align-items: baseline;
		gap: 8px;
		font-size: 15px;
	}

	.author {
		font-weight: 600;
	}

	.when {
		font-size: 12px;
		color: var(--color-muted);
	}

	.body {
		margin: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.empty {
		margin: 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.dot {
		width: 26px;
		height: 26px;
		flex: none;
		border-radius: 50%;
		background: var(--color-line-soft);
	}

	.lines {
		flex: 1;
		display: grid;
		gap: 6px;
	}

	.bar {
		display: block;
		height: 12px;
		border-radius: 4px;
		background: var(--color-line-soft);
	}

	.bar.short {
		width: 80px;
	}

	@media (max-width: 639.98px) {
		.open-chat {
			display: inline-flex;
			align-items: center;
			min-height: 44px;
		}
	}
</style>
