<script lang="ts">
	import { page } from '$app/state';
	import { ArrowRight, MessageSquare } from 'lucide-svelte';
	import ChatAvatar from './ChatAvatar.svelte';
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

<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
	<div class="mb-4 flex items-center justify-between">
		<h2 class="flex items-center gap-2 text-xl font-bold text-white">
			<span class="flex h-7 w-7 items-center justify-center rounded-lg bg-green-500/15">
				<MessageSquare class="h-4 w-4 text-green-400" />
			</span>
			League Chat
		</h2>
		<a
			href="/chat"
			class="flex items-center gap-1 text-sm font-medium text-green-400 hover:text-green-300"
		>
			Open chat <ArrowRight class="h-3.5 w-3.5" />
		</a>
	</div>

	{#if !canRead}
		<SignedOutPanel redirectTo="/chat" />
	{:else if loading}
		<div class="space-y-3">
			{#each Array(3) as _, i}
				<div class="flex gap-3">
					<div class="h-9 w-9 shrink-0 animate-pulse rounded-lg bg-slate-700/50"></div>
					<div class="flex-1 space-y-2">
						<div class="h-3 w-20 animate-pulse rounded bg-slate-700/50"></div>
						<div class="h-3 animate-pulse rounded bg-slate-700/40" style="width: {[70, 50, 62][i]}%"></div>
					</div>
				</div>
			{/each}
		</div>
	{:else if recent.length === 0}
		<p class="py-6 text-center text-sm text-slate-500">
			Nothing said yet. <a href="/chat" class="text-green-400 hover:underline">Start it off.</a>
		</p>
	{:else}
		<ul class="space-y-3">
			{#each recent as message (message.messageId)}
				<li class="flex gap-3">
					<ChatAvatar
						managerName={message.authorName}
						displayName={message.authorDisplayName}
						profileImageUrl={message.authorProfileImage}
					/>
					<div class="min-w-0 flex-1">
						<div class="flex items-baseline gap-2">
							<span class="text-sm font-semibold text-white">{message.authorDisplayName}</span>
							<span class="text-[11px] text-slate-500">{formatRelative(message.createdAt)}</span>
						</div>
						<!-- Stripped to plain text on purpose: the preview is a glance, and
						     rendering markdown would ship marked + DOMPurify to a public page. -->
						<p class="truncate text-sm text-slate-300">{toPlainText(message.content)}</p>
					</div>
				</li>
			{/each}
		</ul>
	{/if}
</section>
