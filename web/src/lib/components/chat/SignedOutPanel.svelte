<script lang="ts">
	import { Lock } from 'lucide-svelte';
	import Button from '../ui/Button.svelte';

	interface Props {
		/** Signed in but not on the roster — a different problem with a different fix. */
		reason?: 'signed-out' | 'not-a-member';
		redirectTo?: string;
		/**
		 * Drawn as This Season's League chat well (WP4 S4.7). Without it, the
		 * full chat panel's look, until WP12 restyles that panel to the Chat board.
		 */
		well?: boolean;
	}

	let { reason = 'signed-out', redirectTo = '/this-season', well = false }: Props = $props();
</script>

<!-- Rendered *instead of* the chat, and the store makes no requests in this
     state. The previous panel fetched regardless, swallowed the 401 behind
     `if (response.ok)`, and left a silently empty box with no explanation. -->
{#if !well}
	<div class="flex flex-1 flex-col items-center justify-center gap-3 px-6 py-12 text-center">
		<Lock class="h-8 w-8 text-slate-600" />

		{#if reason === 'not-a-member'}
			<p class="text-sm font-medium text-slate-300">Your account isn't on the league roster yet.</p>
			<p class="max-w-xs text-xs text-slate-500">
				Ask the commissioner to add you, then sign in again.
			</p>
		{:else}
			<p class="text-sm font-medium text-slate-300">League chat is for managers.</p>
			<p class="max-w-xs text-xs text-slate-500">
				Sign in with your league email and you'll land right back here.
			</p>
			<a
				href="/login?redirect={encodeURIComponent(redirectTo)}"
				class="mt-1 rounded-lg bg-amber-500 px-4 py-2 text-sm font-medium text-black transition-colors hover:bg-amber-400"
			>
				Sign in
			</a>
		{/if}
	</div>
{:else}
	<div class="signed-out">
		<svg
			width="28"
			height="28"
			viewBox="0 0 24 24"
			fill="none"
			stroke="currentColor"
			stroke-width="1.8"
			stroke-linecap="round"
			stroke-linejoin="round"
			aria-hidden="true"
			><rect x="5" y="11" width="14" height="9" rx="2"></rect><path d="M8 11V8a4 4 0 0 1 8 0v3"
			></path></svg
		>

		{#if reason === 'not-a-member'}
			<p class="title">Your account isn't on the league roster yet.</p>
			<p class="line">Ask the commissioner to add you, then sign in again.</p>
		{:else}
			<p class="title">League chat is for managers.</p>
			<p class="line">Sign in with your league email and you'll land right back here.</p>
			<Button
				variant="primary"
				class="sign-in"
				href="/login?redirect={encodeURIComponent(redirectTo)}">Sign in</Button
			>
		{/if}
	</div>
{/if}

<style>
	.signed-out {
		flex: 1;
		margin: 0;
		padding: 26px 20px;
		border-radius: 10px;
		background: var(--color-ground);
		text-align: center;
	}

	/* Inline, as the board draws it, so the well's text-align centers it. */
	svg {
		display: inline;
		vertical-align: baseline;
		color: var(--color-muted);
	}

	.title {
		margin: 8px 0 0;
		font-size: 16px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.line {
		margin: 4px 0 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.signed-out :global(.sign-in) {
		margin-top: 14px;
		font-weight: 700;
	}

	@media (max-width: 639.98px) {
		.signed-out {
			padding: 22px 16px;
		}

		.line {
			line-height: 1.45;
		}
	}
</style>
