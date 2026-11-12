<script lang="ts">
	import { Button, Card } from '$lib/components/ui';
	import StatusTile from '../StatusTile.svelte';
	import type { PageData } from './$types';

	let { data }: { data: PageData } = $props();

	// Specific wording is safe here. Reaching this page at all requires holding a
	// token, so there is no roster to protect — and a manager who is told "this
	// link was already used" stops retrying the same link.
	const MESSAGES: Record<string, { title: string; detail: string }> = {
		missing: {
			title: 'No sign-in link found',
			detail: 'That URL is missing its token. Try the link in your email again, or request a new one.'
		},
		not_found: {
			title: "This link isn't valid",
			detail: 'It may have been mistyped or truncated by your email client. Request a fresh one below.'
		},
		expired: {
			title: 'This link has expired',
			detail: 'Sign-in links last 15 minutes. Request a new one and it should arrive within a moment.'
		},
		consumed: {
			title: 'This link was already used',
			detail:
				"Each link works once. Some email apps open links on their own, so if you aren't signed in yet, request a new one."
		},
		not_allowed: {
			title: 'This address is no longer on the roster',
			detail: 'Ask the commissioner to reactivate your account, then try again.'
		}
	};

	const failed = $derived(data.failed ?? 'not_found');
	const message = $derived(MESSAGES[failed] ?? MESSAGES.not_found);
</script>

<svelte:head>
	<title>Sign-in link problem | The League</title>
</svelte:head>

<div class="column">
	<Card tone="sheet" padding="36px" phonePadding="24px" label="Sign-in link problem">
		<StatusTile tone="pennant">
			{#if failed === 'expired'}
				<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
					><circle cx="12" cy="12" r="9"></circle><path d="M12 7v5l3 2"></path></svg
				>
			{:else}
				<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
					><circle cx="12" cy="12" r="9"></circle><path d="M12 7.5v5M12 16.2h.01"></path></svg
				>
			{/if}
		</StatusTile>
		<h1>{message.title}</h1>
		<p class="detail">{message.detail}</p>
		<Button href="/login" size="xl" class="again">Send me a new link</Button>
	</Card>
</div>

<style>
	/* The board's card is 460px of content with 36px of padding each side. */
	.column {
		max-width: 532px;
		margin: 0 auto;
	}

	h1 {
		margin: 18px 0 10px;
		font-family: var(--font-slab);
		font-size: 36px;
		font-weight: 700;
		line-height: 1.05;
		color: var(--color-ink);
	}

	.detail {
		margin: 0;
		font-size: 16px;
		line-height: 1.55;
		color: var(--color-ink-2);
	}

	.column :global(.again) {
		margin-top: 22px;
	}

	@media (max-width: 639.98px) {
		.column {
			max-width: none;
		}

		.column :global(.again) {
			display: flex;
			width: 100%;
		}
	}
</style>
