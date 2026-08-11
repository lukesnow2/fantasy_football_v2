<script lang="ts">
	import { enhance } from '$app/forms';
	import { Bell, Mail, Shield, User } from 'lucide-svelte';
	import type { ActionData, PageData } from './$types';

	export let data: PageData;
	export let form: ActionData;

	$: prefs = data.manager?.notificationPreferences;
</script>

<svelte:head>
	<title>Settings | The League</title>
</svelte:head>

<div class="mx-auto max-w-2xl space-y-6 px-4 py-8">
	<h1 class="text-2xl font-bold text-white">Settings</h1>

	{#if data.manager}
		<section class="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
			<h2 class="mb-4 flex items-center space-x-2 text-lg font-semibold text-white">
				<User class="h-5 w-5 text-amber-400" />
				<span>Profile</span>
			</h2>
			<dl class="space-y-3 text-sm">
				<div class="flex justify-between">
					<dt class="text-slate-400">Manager</dt>
					<dd class="text-white">{data.manager.displayName}</dd>
				</div>
				<div class="flex justify-between">
					<dt class="text-slate-400">Email</dt>
					<dd class="text-white">{data.manager.email}</dd>
				</div>
				<div class="flex justify-between">
					<dt class="text-slate-400">Role</dt>
					<dd class="text-white capitalize">{data.role}</dd>
				</div>
			</dl>
			<p class="mt-4 text-xs text-slate-500">
				Your email is how you sign in. Ask the commissioner to change it.
			</p>
		</section>

		<section class="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
			<h2 class="mb-4 flex items-center space-x-2 text-lg font-semibold text-white">
				<Bell class="h-5 w-5 text-amber-400" />
				<span>Email notifications</span>
			</h2>

			<form method="POST" action="?/notifications" use:enhance class="space-y-4">
				<label class="flex items-start space-x-3">
					<input
						type="checkbox"
						name="emailOnNewProposal"
						checked={prefs?.emailOnNewProposal}
						class="mt-1 rounded border-slate-600 bg-slate-900 text-amber-500 focus:ring-amber-500"
					/>
					<span class="text-sm">
						<span class="block text-white">New rule proposals</span>
						<span class="block text-slate-400">
							When someone opens a constitutional amendment for voting.
						</span>
					</span>
				</label>

				<label class="flex items-start space-x-3">
					<input
						type="checkbox"
						name="emailOnVoteResults"
						checked={prefs?.emailOnVoteResults}
						class="mt-1 rounded border-slate-600 bg-slate-900 text-amber-500 focus:ring-amber-500"
					/>
					<span class="text-sm">
						<span class="block text-white">Vote results</span>
						<span class="block text-slate-400">When a proposal passes or fails.</span>
					</span>
				</label>

				<label class="flex items-start space-x-3">
					<input
						type="checkbox"
						name="emailOnTradeOffers"
						checked={prefs?.emailOnTradeOffers}
						class="mt-1 rounded border-slate-600 bg-slate-900 text-amber-500 focus:ring-amber-500"
					/>
					<span class="text-sm">
						<span class="block text-white">Trade offers</span>
						<span class="block text-slate-400">When another manager sends you an offer.</span>
					</span>
				</label>

				{#if form?.success}
					<p class="text-sm text-green-400">Saved.</p>
				{:else if form?.error}
					<p class="text-sm text-red-400">{form.error}</p>
				{/if}

				<button
					type="submit"
					class="rounded-lg bg-amber-500 px-4 py-2 font-semibold text-slate-900 transition-colors hover:bg-amber-400"
				>
					Save preferences
				</button>
			</form>
		</section>

		<section class="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
			<h2 class="mb-3 flex items-center space-x-2 text-lg font-semibold text-white">
				<Shield class="h-5 w-5 text-amber-400" />
				<span>Signing in</span>
			</h2>
			<p class="flex items-start space-x-2 text-sm text-slate-400">
				<Mail class="mt-0.5 h-4 w-4 shrink-0 text-slate-500" />
				<span>
					The League uses emailed sign-in links. There's no password to manage, and links expire
					after 15 minutes.
				</span>
			</p>
		</section>
	{:else}
		<p class="text-slate-400">
			Your account isn't linked to a manager profile. Ask the commissioner to check the roster.
		</p>
	{/if}
</div>
