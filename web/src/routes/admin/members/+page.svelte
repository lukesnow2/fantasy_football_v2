<script lang="ts">
	import { enhance } from '$app/forms';
	import { AlertTriangle, Check, Mail, Shield, UserPlus, UserX } from 'lucide-svelte';
	import type { ActionData, PageData } from './$types';

	export let data: PageData;
	export let form: ActionData;

	function when(value: string | Date | null): string {
		if (!value) return '—';
		return new Date(value).toLocaleDateString('en-US', {
			month: 'short',
			day: 'numeric',
			year: 'numeric'
		});
	}
</script>

<svelte:head>
	<title>League members | The League</title>
</svelte:head>

<div class="mx-auto max-w-4xl space-y-6 px-4 py-8">
	<header>
		<h1 class="flex items-center gap-2 text-2xl font-bold text-white">
			<Shield class="h-6 w-6 text-amber-400" /> League members
		</h1>
		<p class="mt-1 text-sm text-slate-400">
			The allowlist. Only these addresses can sign in — there's no registration.
		</p>
	</header>

	{#if !data.mailConfig.ok}
		<section class="rounded-xl border border-red-600/40 bg-red-900/20 p-4">
			<h2 class="flex items-center gap-2 text-sm font-semibold text-red-300">
				<AlertTriangle class="h-4 w-4" /> Sign-in email is not configured
			</h2>
			<p class="mt-1 text-sm text-red-200/80">
				Invites and sign-in links will not reach anyone until this is fixed. Set these in the
				deployment's environment variables.
			</p>
			<ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-red-200/80">
				{#each data.mailConfig.problems as problem}
					<li>{problem}</li>
				{/each}
			</ul>
		</section>
	{/if}

	{#if form?.error}
		<p class="rounded-lg border border-red-600/40 bg-red-900/20 p-3 text-sm text-red-300">
			{form.error}
		</p>
	{:else if form?.success}
		<p class="rounded-lg border border-green-600/40 bg-green-900/20 p-3 text-sm text-green-300">
			{form.success}
		</p>
	{/if}

	{#if data.unclaimed.length > 0}
		<section class="rounded-xl border border-amber-600/40 bg-amber-900/10 p-4">
			<h2 class="flex items-center gap-2 text-sm font-semibold text-amber-300">
				<AlertTriangle class="h-4 w-4" />
				{data.unclaimed.length} manager{data.unclaimed.length === 1 ? '' : 's'} not on the allowlist
			</h2>
			<p class="mt-1 text-sm text-amber-200/80">
				They can't sign in until they have an address here. Adding one takes effect
				immediately — no seed script, no redeploy.
			</p>

			<ul class="mt-3 space-y-2">
				{#each data.unclaimed as manager (manager.managerKey)}
					<li>
						<form
							method="POST"
							action="?/addMember"
							use:enhance
							class="flex flex-wrap items-center gap-2"
						>
							<input type="hidden" name="managerKey" value={manager.managerKey} />
							<span class="w-40 shrink-0 text-sm font-medium text-white">
								{manager.managerName}
							</span>
							<input
								name="email"
								type="email"
								required
								placeholder="email@example.com"
								class="w-56 rounded border border-slate-600 bg-slate-900 px-2 py-1 text-xs text-white placeholder-slate-500"
							/>
							<select
								name="role"
								class="rounded border border-slate-600 bg-slate-900 px-2 py-1 text-xs text-white"
							>
								<option value="member">member</option>
								<option value="commissioner">commissioner</option>
							</select>
							<button
								class="flex items-center gap-1 rounded border border-amber-600/50 bg-amber-900/30 px-2 py-1 text-xs text-amber-200 hover:bg-amber-900/50"
							>
								<UserPlus class="h-3 w-3" />
								Add
							</button>
						</form>
					</li>
				{/each}
			</ul>
		</section>
	{/if}

	<div class="overflow-hidden rounded-xl border border-slate-700">
		<table class="w-full text-sm">
			<thead class="bg-slate-800 text-left text-xs uppercase tracking-wide text-slate-400">
				<tr>
					<th class="px-4 py-3">Manager</th>
					<th class="px-4 py-3">Email</th>
					<th class="px-4 py-3">Role</th>
					<th class="px-4 py-3">Invited</th>
					<th class="px-4 py-3">First login</th>
					<th class="px-4 py-3 text-right">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-slate-700 bg-slate-800/40">
				{#each data.members as member (member.id)}
					<tr class:opacity-50={!member.active}>
						<td class="px-4 py-3">
							<span class="font-medium text-white">{member.displayName}</span>
							{#if member.role === 'commissioner'}
								<span class="ml-1.5 rounded bg-amber-900/40 px-1.5 py-0.5 text-xs text-amber-300">
									commissioner
								</span>
							{/if}
							{#if !member.active}
								<span class="ml-1.5 text-xs text-slate-500">deactivated</span>
							{/if}
						</td>
						<td class="px-4 py-3">
							<form method="POST" action="?/updateEmail" use:enhance class="flex gap-1">
								<input type="hidden" name="memberId" value={member.id} />
								<input
									name="email"
									value={member.email}
									class="w-52 rounded border border-slate-600 bg-slate-900 px-2 py-1 text-xs text-white"
								/>
								<button class="rounded px-2 text-xs text-slate-400 hover:text-amber-400" title="Save">
									<Check class="h-3.5 w-3.5" />
								</button>
							</form>
						</td>
						<td class="px-4 py-3">
							<form method="POST" action="?/setRole" use:enhance class="flex gap-1">
								<input type="hidden" name="memberId" value={member.id} />
								<select
									name="role"
									value={member.role}
									class="rounded border border-slate-600 bg-slate-900 px-2 py-1 text-xs text-white"
								>
									<option value="member">member</option>
									<option value="commissioner">commissioner</option>
								</select>
								<button class="rounded px-2 text-xs text-slate-400 hover:text-amber-400" title="Save role">
									<Check class="h-3.5 w-3.5" />
								</button>
							</form>
						</td>
						<td class="px-4 py-3 text-slate-400">{when(member.invitedAt)}</td>
						<td class="px-4 py-3 text-slate-400">{when(member.firstLoginAt)}</td>
						<td class="px-4 py-3">
							<div class="flex justify-end gap-2">
								<form method="POST" action="?/invite" use:enhance>
									<input type="hidden" name="memberId" value={member.id} />
									<button
										disabled={!member.active}
										class="flex items-center gap-1 rounded border border-slate-600 px-2 py-1 text-xs text-slate-300 hover:bg-slate-700 disabled:opacity-40"
									>
										<Mail class="h-3 w-3" />
										{member.firstLoginAt ? 'Resend link' : 'Invite'}
									</button>
								</form>
								<form method="POST" action="?/setActive" use:enhance>
									<input type="hidden" name="memberId" value={member.id} />
									<input type="hidden" name="active" value={(!member.active).toString()} />
									<button
										class="flex items-center gap-1 rounded border border-slate-600 px-2 py-1 text-xs text-slate-300 hover:bg-slate-700"
									>
										<UserX class="h-3 w-3" />
										{member.active ? 'Deactivate' : 'Reactivate'}
									</button>
								</form>
							</div>
						</td>
					</tr>
				{/each}

				{#if data.members.length === 0}
					<tr>
						<td colspan="6" class="px-4 py-8 text-center text-slate-400">
							Nobody on the allowlist yet. Add managers from the panel above.
						</td>
					</tr>
				{/if}
			</tbody>
		</table>
	</div>

	<p class="text-xs text-slate-500">
		Deactivating a manager blocks future sign-ins <em>and</em> ends any session they currently have.
	</p>
</div>
