<script lang="ts">
	import { Card } from '$lib/components/ui';
	import StatusTile from '../StatusTile.svelte';
</script>

<svelte:head>
	<title>Check your email | The League</title>
</svelte:head>

<!--
	Deliberately static and unconditional.

	This page is reached identically whether a link was actually sent, the address
	is not on the roster, or the sender hit a rate limit. It must not echo the
	submitted address back or vary its wording, or it becomes the roster-
	enumeration oracle the login action exists to close.
-->
<div class="column">
	<Card tone="sheet" padding="36px" phonePadding="24px" label="Check your email">
		<StatusTile>
			<svg
				width="28"
				height="28"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="1.8"
				stroke-linecap="round"
				stroke-linejoin="round"
				><rect x="3" y="5" width="18" height="14" rx="2"></rect><path d="M3.5 6.5l8.5 6.5 8.5-6.5"
				></path></svg
			>
		</StatusTile>
		<h1>Check your email</h1>
		<p class="detail">
			If that address is on the league roster, a sign-in link is on its way. It expires in 15
			minutes and can only be used once.
		</p>
		<p class="retry">
			Didn't get it? Check your spam folder, then <a href="/login" class="field-link">try again</a>.
		</p>
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

	.retry {
		margin: 18px 0 0;
		font-size: 15px;
		color: var(--color-ink-2);
	}

	.retry a {
		font-weight: 700;
	}

	@media (max-width: 639.98px) {
		.column {
			max-width: none;
		}

		.retry {
			line-height: 1.5;
		}

		/* A 44px tap area around the inline link, as the phone board draws it. */
		.retry a {
			display: inline-block;
			padding: 12px 0;
			margin: -12px 0;
		}
	}
</style>
