<script lang="ts">
	import { enhance } from '$app/forms';
	import { Mail, Trophy } from 'lucide-svelte';
	import type { ActionData, PageData } from './$types';

	export let data: PageData;
	export let form: ActionData;

	let submitting = false;
</script>

<svelte:head>
	<title>Sign in | The League</title>
</svelte:head>

<div class="flex min-h-[70vh] items-center justify-center px-4">
	<div class="w-full max-w-md">
		<div class="mb-8 text-center">
			<Trophy class="mx-auto mb-3 h-10 w-10 text-amber-400" />
			<h1 class="text-2xl font-bold text-white">Sign in to The League</h1>
			<p class="mt-2 text-sm text-slate-400">
				Enter your email and we'll send you a sign-in link. No password needed.
			</p>
		</div>

		{#if data.membershipRevoked}
			<div class="mb-6 rounded-lg border border-amber-600/40 bg-amber-900/20 p-4 text-sm text-amber-200">
				Your account is signed in, but it's no longer on the league roster. Ask the commissioner to
				reactivate it.
			</div>
		{/if}

		<form
			method="POST"
			use:enhance={() => {
				submitting = true;
				return async ({ update }) => {
					await update();
					submitting = false;
				};
			}}
			class="space-y-4 rounded-xl border border-slate-700 bg-slate-800/50 p-6"
		>
			<div>
				<label for="email" class="mb-2 block text-sm font-medium text-slate-300">
					Email address
				</label>
				<input
					id="email"
					name="email"
					type="email"
					autocomplete="email"
					required
					placeholder="you@example.com"
					class="w-full rounded-lg border border-slate-600 bg-slate-900 px-4 py-2.5 text-white placeholder-slate-500 focus:border-amber-500 focus:ring-1 focus:ring-amber-500 focus:outline-none"
				/>
			</div>

			{#if form?.error}
				<p class="text-sm text-red-400">{form.error}</p>
			{/if}

			<button
				type="submit"
				disabled={submitting}
				class="flex w-full items-center justify-center space-x-2 rounded-lg bg-amber-500 px-4 py-2.5 font-semibold text-slate-900 transition-colors hover:bg-amber-400 disabled:cursor-not-allowed disabled:opacity-60"
			>
				<Mail class="h-4 w-4" />
				<span>{submitting ? 'Sending…' : 'Email me a sign-in link'}</span>
			</button>
		</form>

		<p class="mt-6 text-center text-xs text-slate-500">
			The League is invite-only. If you're one of the ten managers and can't get in, ask the
			commissioner to check the address on file.
		</p>
	</div>
</div>
