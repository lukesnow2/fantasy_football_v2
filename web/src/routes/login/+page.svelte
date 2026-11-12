<script lang="ts">
	import { enhance } from '$app/forms';
	import { Button, Card } from '$lib/components/ui';
	import type { ActionData, PageData } from './$types';

	let { data, form }: { data: PageData; form: ActionData } = $props();

	let submitting = $state(false);
</script>

<svelte:head>
	<title>Sign in | The League</title>
</svelte:head>

<div class="column">
	<Card tone="sheet" padding="36px" phonePadding="24px" label="Sign in">
		{#if data.membershipRevoked}
			<p class="revoked" role="status">
				Your account is signed in, but it's no longer on the league roster. Ask the commissioner to
				reactivate it.
			</p>
		{/if}

		<div class="kicker">Members only</div>
		<h1>Sign in to The League</h1>
		<p class="intro">Enter your email and we'll send you a sign-in link. No password needed.</p>

		<form
			method="POST"
			use:enhance={() => {
				submitting = true;
				return async ({ update }) => {
					await update();
					submitting = false;
				};
			}}
		>
			<label for="email">Email address</label>
			<input
				id="email"
				name="email"
				type="email"
				autocomplete="email"
				required
				placeholder="you@example.com"
				class:invalid={!!form?.error}
				aria-invalid={form?.error ? 'true' : undefined}
				aria-describedby={form?.error ? 'email-error' : undefined}
			/>
			{#if form?.error}
				<p id="email-error" class="error">{form.error}</p>
			{/if}

			<Button type="submit" size="xl" disabled={submitting} class="submit"
				>{submitting ? 'Sending…' : 'Email me a sign-in link'}</Button
			>
		</form>

		<p class="note">
			The League is invite-only. If you're one of the ten managers and can't get in, ask the
			commissioner to check the address on file.
		</p>
	</Card>
</div>

<style>
	/* The board's card is 460px of content with 36px of padding each side. */
	.column {
		max-width: 532px;
		margin: 0 auto;
	}

	.revoked {
		margin: 0 0 20px;
		padding: 12px 14px;
		border-radius: 10px;
		background: var(--color-pennant-tint);
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-pennant-dark);
	}

	.kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	h1 {
		margin: 8px 0 10px;
		font-family: var(--font-slab);
		font-size: 40px;
		font-weight: 700;
		line-height: 1.05;
		color: var(--color-ink);
	}

	.intro {
		margin: 0;
		font-size: 16px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	form {
		margin-top: 24px;
		display: flex;
		flex-direction: column;
	}

	label {
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink);
	}

	/* Content-box on desktop, as the board draws it (50px plus the border);
	   the phone board sets border-box. */
	input {
		box-sizing: content-box;
		margin-top: 6px;
		min-height: 50px;
		padding: 0 14px;
		border: 1.5px solid var(--color-line-strong);
		border-radius: 10px;
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 16px;
		color: var(--color-ink);
		box-shadow: none;
	}

	input::placeholder {
		color: var(--color-faint);
	}

	input:focus {
		outline: none;
		border-color: var(--color-field);
		box-shadow: none;
	}

	input.invalid {
		border-color: var(--color-pennant);
	}

	.error {
		margin: 6px 0 0;
		font-size: 14px;
		color: var(--color-pennant-dark);
	}

	form :global(.submit) {
		margin-top: 14px;
		width: 100%;
	}

	.note {
		margin: 20px 0 0;
		padding-top: 16px;
		border-top: 1px solid var(--color-line-soft);
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.column {
			max-width: none;
		}

		.kicker {
			font-size: 12px;
		}

		h1 {
			font-size: 38px;
		}

		form {
			margin-top: 20px;
		}

		input {
			box-sizing: border-box;
		}
	}
</style>
