<script lang="ts">
	import Button from '../ui/Button.svelte';

	interface Props {
		/** Signed in but not on the roster — a different problem with a different fix. */
		reason?: 'signed-out' | 'not-a-member';
		redirectTo?: string;
		/**
		 * Drawn as This Season's League chat well (WP4 S4.7). Without it, the
		 * state inside Chat's messages card (WP12 S12.9): centered, a slab title
		 * and one muted line.
		 */
		well?: boolean;
	}

	let { reason = 'signed-out', redirectTo = '/this-season', well = false }: Props = $props();
</script>

<!-- Rendered *instead of* the chat, and the store makes no requests in this
     state. The previous panel fetched regardless, swallowed the 401 behind
     `if (response.ok)`, and left a silently empty box with no explanation. -->
{#if !well}
	<div class="panel">
		{#if reason === 'not-a-member'}
			<p class="panel-title">Your account isn't on the league roster yet.</p>
			<p class="panel-line">Ask the commissioner to add you, then sign in again.</p>
		{:else}
			<p class="panel-title">League chat is for managers.</p>
			<p class="panel-line">Sign in with your league email and you'll land right back here.</p>
			<Button
				variant="primary"
				class="sign-in"
				href="/login?redirect={encodeURIComponent(redirectTo)}">Sign in</Button
			>
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
	.panel {
		flex: 1;
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		padding: 48px;
		text-align: center;
	}

	.panel-title {
		margin: 0;
		font-family: var(--font-slab);
		font-size: 22px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.panel-line {
		margin: 6px 0 0;
		font-size: 15px;
		color: var(--color-muted);
	}

	.panel :global(.sign-in) {
		margin-top: 18px;
	}

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
		.panel {
			padding: 40px 20px;
		}

		.signed-out {
			padding: 22px 16px;
		}

		.line {
			line-height: 1.45;
		}
	}
</style>
