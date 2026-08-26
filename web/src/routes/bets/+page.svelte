<script lang="ts">
	import { enhance } from '$app/forms';
	import { Gavel, Handshake, ScrollText, Swords, Trophy, XCircle } from 'lucide-svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import type { ActionData, PageData } from './$types';

	let { data, form }: { data: PageData; form: ActionData } = $props();

	let showForm = $state(false);
	let counterpartyKey = $state('');
	let resolving = $state<number | null>(null);

	const me = $derived(data.myManagerKey);

	const onBoard = $derived(data.wagers.filter((w) => w.status === 'open'));
	const live = $derived(data.wagers.filter((w) => w.status === 'accepted'));
	const awaitingRuling = $derived(data.wagers.filter((w) => w.status === 'pending_resolution'));
	const settled = $derived(
		data.wagers.filter((w) => w.status === 'settled' || w.status === 'void')
	);
	// Voided bets share the Settled section — they have a ruling — but they are
	// explicitly not settled, so the counter must not claim they are.
	const settledCount = $derived(data.wagers.filter((w) => w.status === 'settled').length);
	// Declined offers were fetched and then rendered nowhere, so a bet someone
	// passed on simply disappeared and the proposer could not tell that from one
	// still sitting unanswered.
	const passed = $derived(
		data.wagers.filter(
			(w) => w.status === 'declined' && (w.proposedBy === me || w.counterpartyKey === me)
		)
	);

	/** Can I take this one? Open props are anyone's; a head-to-head is only its target's. */
	function canAccept(w: PageData['wagers'][number]): boolean {
		if (me == null || w.proposedBy === me) return false;
		return w.counterpartyKey == null || w.counterpartyKey === me;
	}

	function isMine(w: PageData['wagers'][number]): boolean {
		return me != null && (w.proposedBy === me || w.acceptedBy === me);
	}

	function when(value: string | Date | null): string {
		if (!value) return '—';
		return new Date(value).toLocaleDateString('en-US', {
			month: 'short',
			day: 'numeric',
			year: 'numeric'
		});
	}

	function sides(w: PageData['wagers'][number]): string {
		const other = w.takerName ?? w.counterpartyName ?? 'whoever takes it';
		return `${w.proposerName ?? 'Someone'} vs ${other}`;
	}

	function verdict(w: PageData['wagers'][number]): string {
		if (w.status === 'void') return 'Voided — no action';
		if (w.outcome === 'push') return 'Push';
		return w.winnerName ? `${w.winnerName} won` : 'Settled';
	}

	const currentYear = new Date().getFullYear();
</script>

<svelte:head>
	<title>Bet Board - The League</title>
</svelte:head>

<div class="space-y-8">
	<PageHeader icon={Handshake} title="Bet Board" accent="amber">
		A record of the side bets and props managers have agreed to, and what they were worth. Settled
		here in an entirely unofficial capacity — no money moves through this site, nothing here is
		enforceable, and the commissioner's ruling is worth exactly as much as you all decide it is.
	</PageHeader>

	{#if form?.error}
		<p class="rounded-lg border border-red-600/40 bg-red-900/20 p-3 text-sm text-red-300">
			{form.error}
		</p>
	{/if}

	<!-- Filter bar convention: page-level controls live here, never in the header. -->
	<div class="flex flex-wrap items-center justify-between gap-3 rounded-lg bg-slate-800/30 p-4">
		<div class="text-sm text-slate-400">
			{onBoard.length} on the board • {live.length} live • {awaitingRuling.length} awaiting a ruling
			• {settledCount} settled
		</div>
		<button
			onclick={() => (showForm = !showForm)}
			class="rounded-lg bg-amber-500 px-4 py-2 font-semibold text-black transition-colors hover:bg-amber-600"
		>
			{showForm ? 'Never mind' : 'Post a bet'}
		</button>
	</div>

	{#if showForm}
		<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
			<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
				<Handshake class="mr-3 h-6 w-6 text-amber-400" />
				Put it on the board
			</h2>
			<form
				method="POST"
				action="?/propose"
				use:enhance={() => {
					return async ({ update }) => {
						await update();
						showForm = false;
						counterpartyKey = '';
					};
				}}
				class="space-y-4"
			>
				<div>
					<label for="title" class="mb-1 block text-sm font-medium text-slate-300">
						What's the bet?
					</label>
					<input
						id="title"
						name="title"
						required
						maxlength="200"
						placeholder="Gabe misses the playoffs"
						class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-white focus:border-amber-400 focus:outline-none"
					/>
				</div>

				<div>
					<label for="terms" class="mb-1 block text-sm font-medium text-slate-300">
						Terms — what has to happen for you to win
					</label>
					<textarea
						id="terms"
						name="terms"
						required
						rows="3"
						maxlength="2000"
						placeholder="I win if he finishes 7th or worse. Ties go to him."
						class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-white focus:border-amber-400 focus:outline-none"
					></textarea>
					<p class="mt-1 text-xs text-slate-500">
						Write it out properly now. This is the wording everyone argues over in December.
					</p>
				</div>

				<div class="grid grid-cols-1 gap-4 md:grid-cols-3">
					<div>
						<label for="stake" class="mb-1 block text-sm font-medium text-slate-300">
							Stake / price
						</label>
						<input
							id="stake"
							name="stake"
							required
							maxlength="200"
							placeholder="$20, or loser buys wings"
							class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-white focus:border-amber-400 focus:outline-none"
						/>
					</div>

					<div>
						<label for="counterpartyKey" class="mb-1 block text-sm font-medium text-slate-300">
							Against
						</label>
						<select
							id="counterpartyKey"
							name="counterpartyKey"
							bind:value={counterpartyKey}
							class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-white focus:border-amber-400 focus:outline-none"
						>
							<option value="">Open to anyone</option>
							{#each data.members.filter((m) => m.managerKey !== me) as member (member.managerKey)}
								<option value={member.managerKey}>{member.name}</option>
							{/each}
						</select>
					</div>

					<div>
						<label for="seasonYear" class="mb-1 block text-sm font-medium text-slate-300">
							Season <span class="text-slate-500">(optional)</span>
						</label>
						<input
							id="seasonYear"
							name="seasonYear"
							type="number"
							min="2005"
							max={currentYear + 1}
							placeholder={String(currentYear)}
							class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-white focus:border-amber-400 focus:outline-none"
						/>
					</div>
				</div>

				<button
					type="submit"
					class="rounded-lg bg-amber-500 px-8 py-3 font-semibold text-black transition-colors hover:bg-amber-600"
				>
					Post it
				</button>
			</form>
		</section>
	{/if}

	<!-- On the board -->
	<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
		<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
			<ScrollText class="mr-3 h-6 w-6 text-blue-400" />
			On the board
		</h2>
		{#if onBoard.length === 0}
			<p class="text-slate-400">Nothing offered right now.</p>
		{:else}
			<ul class="space-y-4">
				{#each onBoard as w (w.wagerKey)}
					<li class="rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
						<div class="flex flex-wrap items-start justify-between gap-3">
							<div class="min-w-0">
								<h3 class="font-semibold text-white">{w.title}</h3>
								<p class="mt-1 text-sm text-slate-300">{w.terms}</p>
								<p class="mt-2 text-sm text-slate-400">
									<span class="font-medium text-amber-400">{w.stake}</span>
									• {w.proposerName ?? 'Someone'} →
									{w.counterpartyKey == null ? 'open to anyone' : w.counterpartyName}
									{#if w.seasonYear}• {w.seasonYear}{/if}
								</p>
							</div>
							<div class="flex shrink-0 gap-2">
								{#if canAccept(w)}
									<form method="POST" action="?/accept" use:enhance>
										<input type="hidden" name="wagerKey" value={w.wagerKey} />
										<button
											class="rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-green-500"
										>
											Take it
										</button>
									</form>
									{#if w.counterpartyKey === me}
										<form method="POST" action="?/decline" use:enhance>
											<input type="hidden" name="wagerKey" value={w.wagerKey} />
											<button
												class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-600"
											>
												Pass
											</button>
										</form>
									{/if}
								{:else if w.proposedBy === me}
									<form method="POST" action="?/withdraw" use:enhance>
										<input type="hidden" name="wagerKey" value={w.wagerKey} />
										<button
											class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-600"
										>
											Pull it
										</button>
									</form>
								{/if}
							</div>
						</div>
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<!-- Passed on -->
	{#if passed.length > 0}
		<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
			<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
				<XCircle class="mr-3 h-6 w-6 text-slate-400" />
				Passed on
			</h2>
			<ul class="space-y-3">
				{#each passed as w (w.wagerKey)}
					<li class="rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
						<div class="flex flex-wrap items-start justify-between gap-3">
							<div class="min-w-0">
								<h3 class="font-semibold text-slate-300">{w.title}</h3>
								<p class="mt-1 text-sm text-slate-400">
									<span class="font-medium text-amber-400/70">{w.stake}</span>
									• {w.counterpartyName ?? 'They'} passed
								</p>
							</div>
							{#if w.proposedBy === me}
								<form method="POST" action="?/withdraw" use:enhance>
									<input type="hidden" name="wagerKey" value={w.wagerKey} />
									<button
										class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-600"
									>
										Clear it
									</button>
								</form>
							{/if}
						</div>
					</li>
				{/each}
			</ul>
		</section>
	{/if}

	<!-- Live -->
	<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
		<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
			<Swords class="mr-3 h-6 w-6 text-green-400" />
			Live
		</h2>
		{#if live.length === 0}
			<p class="text-slate-400">No bets in flight.</p>
		{:else}
			<ul class="space-y-4">
				{#each live as w (w.wagerKey)}
					<li class="rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
						<h3 class="font-semibold text-white">{w.title}</h3>
						<p class="mt-1 text-sm text-slate-300">{w.terms}</p>
						<p class="mt-2 text-sm text-slate-400">
							<span class="font-medium text-amber-400">{w.stake}</span>
							• {sides(w)} • agreed {when(w.acceptedAt)}
						</p>

						{#if isMine(w)}
							<form
								method="POST"
								action="?/requestResolution"
								use:enhance
								class="mt-3 flex flex-wrap items-center gap-2"
							>
								<input type="hidden" name="wagerKey" value={w.wagerKey} />
								<input
									name="resolutionNote"
									maxlength="1000"
									placeholder="What happened? (optional)"
									class="min-w-0 flex-1 rounded border border-slate-600 bg-slate-700 px-3 py-2 text-sm text-white focus:border-amber-400 focus:outline-none"
								/>
								<button
									class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-600"
								>
									This one's over
								</button>
							</form>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<!-- Awaiting a ruling -->
	<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
		<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
			<Gavel class="mr-3 h-6 w-6 text-purple-400" />
			Awaiting a ruling
		</h2>
		{#if awaitingRuling.length === 0}
			<p class="text-slate-400">Nothing on the commissioner's desk.</p>
		{:else}
			<ul class="space-y-4">
				{#each awaitingRuling as w (w.wagerKey)}
					<li class="rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
						<h3 class="font-semibold text-white">{w.title}</h3>
						<p class="mt-1 text-sm text-slate-300">{w.terms}</p>
						<p class="mt-2 text-sm text-slate-400">
							<span class="font-medium text-amber-400">{w.stake}</span>
							• {sides(w)} • flagged {when(w.resolutionRequestedAt)}
						</p>
						{#if w.resolutionNote}
							<p class="mt-2 border-l-2 border-slate-600 pl-3 text-sm text-slate-300 italic">
								{w.resolutionNote}
							</p>
						{/if}

						{#if data.isCommissioner}
							{#if resolving === w.wagerKey}
								<form
									method="POST"
									action="?/resolve"
									use:enhance={() => {
										return async ({ update }) => {
											await update();
											resolving = null;
										};
									}}
									class="mt-3 space-y-3 rounded-lg border border-purple-600/40 bg-purple-900/10 p-3"
								>
									<input type="hidden" name="wagerKey" value={w.wagerKey} />
									<fieldset class="flex flex-wrap gap-4">
										<legend class="mb-2 text-sm font-medium text-slate-300">Who took it?</legend>
										<label class="flex items-center gap-2 text-sm text-slate-300">
											<input type="radio" name="outcome" value="proposer" required />
											{w.proposerName}
										</label>
										<label class="flex items-center gap-2 text-sm text-slate-300">
											<input type="radio" name="outcome" value="taker" />
											{w.takerName}
										</label>
										<label class="flex items-center gap-2 text-sm text-slate-300">
											<input type="radio" name="outcome" value="push" /> Push
										</label>
										<label class="flex items-center gap-2 text-sm text-slate-300">
											<input type="radio" name="outcome" value="void" /> Void it
										</label>
									</fieldset>
									<input
										name="rulingNote"
										maxlength="1000"
										placeholder="Reasoning (optional)"
										class="w-full rounded border border-slate-600 bg-slate-700 px-3 py-2 text-sm text-white focus:border-amber-400 focus:outline-none"
									/>
									<div class="flex gap-2">
										<button
											class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-purple-500"
										>
											Record the ruling
										</button>
										<button
											type="button"
											onclick={() => (resolving = null)}
											class="rounded-lg bg-slate-700 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-600"
										>
											Cancel
										</button>
									</div>
								</form>
							{:else}
								<button
									onclick={() => (resolving = w.wagerKey)}
									class="mt-3 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-purple-500"
								>
									Rule on it
								</button>
							{/if}
						{:else}
							<p class="mt-3 text-sm text-slate-500">The commissioner settles this one.</p>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<!-- Settled -->
	<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
		<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
			<Trophy class="mr-3 h-6 w-6 text-amber-400" />
			Settled
		</h2>
		{#if settled.length === 0}
			<p class="text-slate-400">Nothing has been ruled on yet.</p>
		{:else}
			<ul class="space-y-3">
				{#each settled as w (w.wagerKey)}
					<li class="rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
						<div class="flex flex-wrap items-baseline justify-between gap-2">
							<h3 class="font-semibold text-white">{w.title}</h3>
							<span
								class="text-sm font-semibold {w.status === 'void'
									? 'text-slate-500'
									: 'text-green-400'}"
							>
								{verdict(w)}
							</span>
						</div>
						<p class="mt-1 text-sm text-slate-300">{w.terms}</p>
						<p class="mt-2 text-sm text-slate-400">
							<span class="font-medium text-amber-400">{w.stake}</span>
							• {sides(w)} • ruled {when(w.resolvedAt)}
						</p>
						{#if w.rulingNote}
							<p class="mt-2 border-l-2 border-slate-600 pl-3 text-sm text-slate-400 italic">
								{w.rulingNote}
							</p>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<!-- Ledger -->
	{#if data.ledger.length > 0}
		<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
			<h2 class="mb-6 flex items-center text-2xl font-bold text-white">
				<Handshake class="mr-3 h-6 w-6 text-cyan-400" />
				The ledger
			</h2>
			<div class="grid grid-cols-2 gap-6 md:grid-cols-4">
				{#each data.ledger as entry (entry.managerKey)}
					<div class="text-center">
						<div class="text-3xl font-bold text-cyan-400">
							{entry.won}-{entry.lost}{entry.pushed ? `-${entry.pushed}` : ''}
						</div>
						<div class="text-slate-400">{entry.name}</div>
					</div>
				{/each}
			</div>
			<p class="mt-6 text-xs text-slate-500">
				Settled bets only. Pushes and voided bets don't count toward a record.
			</p>
		</section>
	{/if}
</div>
