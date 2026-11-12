<script lang="ts">
	import { enhance } from '$app/forms';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import {
		Avatar,
		Button,
		Card,
		EmptyValue,
		RadioChip,
		Tag,
		TextField
	} from '$lib/components/ui';
	import { profileHref } from '$lib/home';
	import {
		betCounts,
		betDate,
		countChips,
		groupBets,
		ledgerRows,
		STAGES,
		verdictLabel
	} from '$lib/bet-board';
	import { readerTimeZone } from '$lib/utils/reader-time-zone.svelte';
	import type { ActionData, PageData } from './$types';

	let { data, form }: { data: PageData; form: ActionData } = $props();

	type Bet = PageData['wagers'][number];

	// The reader's zone for ruling dates (see readerTimeZone).
	const zone = readerTimeZone(data.timeZone);

	let showForm = $state(false);
	let counterpartyKey = $state('');
	let resolving = $state<number | null>(null);

	const me = $derived(data.myManagerKey);

	// Declined offers sit in Passed on, so the proposer can tell a bet someone
	// passed on from one still unanswered. load only sends the ones this member
	// is party to, so there is nothing to filter by manager here.
	const groups = $derived(groupBets(data.wagers));
	// Voided bets share the Settled card, since they have a ruling, but they are
	// explicitly not settled, so the counter doesn't claim they are.
	const chips = $derived(countChips(betCounts(data.wagers)));
	const ledger = $derived(ledgerRows(data.members, data.ledger));
	// Passed on isn't on the board; it only shows when it has bets (S9.4).
	const stages = $derived(STAGES.filter((s) => s.key !== 'passed' || groups.passed.length > 0));

	const againstOptions = $derived([
		{ value: '', label: 'Open to anyone' },
		...data.members
			.filter((m) => m.managerKey !== me)
			.map((m) => ({ value: String(m.managerKey), label: m.name }))
	]);

	/** Can I take this one? Open props are anyone's; a head-to-head is only its target's. */
	function canAccept(w: Bet): boolean {
		if (me == null || w.proposedBy === me) return false;
		return w.counterpartyKey == null || w.counterpartyKey === me;
	}

	function isMine(w: Bet): boolean {
		return me != null && (w.proposedBy === me || w.acceptedBy === me);
	}

	const currentYear = new Date().getFullYear();
</script>

<svelte:head>
	<title>Bet Board - The League</title>
</svelte:head>

{#snippet who(name: string | null)}
	{#if name}<a class="name" href={profileHref(name)}>{name}</a>{:else}Someone{/if}
{/snippet}

{#snippet person(name: string | null)}
	{#if name}
		<a class="person" href={profileHref(name)}><Avatar {name} size={22} alt="" />{name}</a>
	{:else}
		<span class="person">Someone</span>
	{/if}
{/snippet}

{#snippet betHead(title: string, stake: string)}
	<div class="bet-head"><b class="bet-title">{title}</b><span class="stake">{stake}</span></div>
{/snippet}

{#snippet openBet(w: Bet)}
	<Card tone="inset" padding="14px 16px" phonePadding="12px 14px" label={w.title}>
		{@render betHead(w.title, w.stake)}
		<p class="terms">{w.terms}</p>
		<div class="meta">
			{@render who(w.proposerName)} →
			{#if w.counterpartyKey == null}open to anyone{:else}{@render who(
					w.counterpartyName
				)}{/if}{#if w.seasonYear}&nbsp;· {w.seasonYear}{/if}
		</div>
		{#if canAccept(w) || w.proposedBy === me}
			<div class="actions">
				{#if canAccept(w)}
					<form method="POST" action="?/accept" use:enhance>
						<input type="hidden" name="wagerKey" value={w.wagerKey} />
						<Button type="submit" size="sm">Take it</Button>
					</form>
					{#if w.counterpartyKey === me}
						<form method="POST" action="?/decline" use:enhance>
							<input type="hidden" name="wagerKey" value={w.wagerKey} />
							<Button type="submit" size="sm" variant="secondary">Pass</Button>
						</form>
					{/if}
				{:else if w.proposedBy === me}
					<form method="POST" action="?/withdraw" use:enhance class="push-right">
						<input type="hidden" name="wagerKey" value={w.wagerKey} />
						<Button type="submit" size="sm" variant="text" tone="pennant">Pull it</Button>
					</form>
				{/if}
			</div>
		{/if}
	</Card>
{/snippet}

{#snippet liveBet(w: Bet)}
	<Card tone="inset" padding="14px 16px" phonePadding="12px 14px" label={w.title}>
		{@render betHead(w.title, w.stake)}
		<div class="pair">
			<span class="side"
				>{@render person(w.proposerName)}<span class="vs"><span class="sp">&nbsp;</span>vs</span
				></span
			>{@render person(w.takerName)}
		</div>
		{#if isMine(w)}
			<form method="POST" action="?/requestResolution" use:enhance class="over">
				<input type="hidden" name="wagerKey" value={w.wagerKey} />
				<TextField
					size="sm"
					label="What happened?"
					hint="Optional"
					name="resolutionNote"
					maxlength={1000}
					placeholder="He finished 8th."
				/>
				<div class="over-button">
					<Button type="submit" size="sm" variant="outline">This one's over</Button>
				</div>
			</form>
		{/if}
	</Card>
{/snippet}

{#snippet flaggedBet(w: Bet)}
	<Card tone="inset-pennant" padding="14px 16px" phonePadding="12px 14px" label={w.title}>
		{@render betHead(w.title, w.stake)}
		{#if w.resolutionNote}
			<blockquote class="note">"{w.resolutionNote}"</blockquote>
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
				>
					<input type="hidden" name="wagerKey" value={w.wagerKey} />
					<fieldset class="ruling">
						<legend>Commissioner's ruling</legend>
						<div class="outcomes">
							<RadioChip
								name="outcome"
								value="proposer"
								label={w.proposerName ?? 'Someone'}
								required
							/>
							<RadioChip name="outcome" value="taker" label={w.takerName ?? 'Someone'} />
							<RadioChip name="outcome" value="push" label="Push" />
							<RadioChip name="outcome" value="void" label="Void it" />
						</div>
						<div class="reasoning">
							<TextField
								size="sm"
								label="Reasoning"
								hideLabel
								name="rulingNote"
								maxlength={1000}
								placeholder="Reasoning (optional)"
							/>
						</div>
						<div class="ruling-actions">
							<Button type="submit" size="sm" tone="pennant">Record the ruling</Button>
							<Button size="sm" variant="text" onclick={() => (resolving = null)}>Cancel</Button>
						</div>
					</fieldset>
				</form>
			{:else}
				<div class="rule-on-it">
					<Button size="sm" tone="pennant" onclick={() => (resolving = w.wagerKey)}>
						Rule on it
					</Button>
				</div>
			{/if}
		{:else}
			<p class="commissioner">The commissioner settles this one.</p>
		{/if}
	</Card>
{/snippet}

{#snippet settledBet(w: Bet)}
	{@const verdict = verdictLabel(w)}
	{@const ruled = betDate(w.resolvedAt, zone.current)}
	<Card tone="inset" padding="14px 16px" phonePadding="12px 14px" label={w.title}>
		<div class="bet-head">
			<b class="bet-title">{w.title}</b><span class="verdict {verdict.tone}">{verdict.text}</span>
		</div>
		<div class="meta settled-meta">
			{w.stake} · {@render who(w.proposerName)} vs {@render who(w.takerName)} ·
			{#if ruled}ruled {ruled}{:else}<EmptyValue text="no ruling date" />{/if}
		</div>
		{#if w.rulingNote}
			<blockquote class="note">"{w.rulingNote}"</blockquote>
		{/if}
	</Card>
{/snippet}

{#snippet passedBet(w: Bet)}
	<Card tone="inset" padding="14px 16px" phonePadding="12px 14px" label={w.title}>
		{@render betHead(w.title, w.stake)}
		<div class="meta">
			{#if w.counterpartyName}{@render who(w.counterpartyName)}{:else}They{/if} passed
		</div>
		{#if w.proposedBy === me}
			<div class="actions">
				<form method="POST" action="?/withdraw" use:enhance class="push-right">
					<input type="hidden" name="wagerKey" value={w.wagerKey} />
					<Button type="submit" size="sm" variant="text" tone="pennant">Clear it</Button>
				</form>
			</div>
		{/if}
	</Card>
{/snippet}

<PageHeader
	kicker="Activity · members only"
	title="Bet Board"
	introWidth={680}
	blockBasis={560}
	phoneActionGap={14}
	phoneActionFull
>
	A record of the side bets and props managers have agreed to, and what they were worth. It's all
	unofficial: no money moves through this site, nothing here is enforceable, and the commissioner's
	ruling is worth exactly as much as you all decide it is.
	{#snippet action()}
		<Button size="lg" aria-expanded={showForm} onclick={() => (showForm = !showForm)}>
			{showForm ? 'Never mind' : 'Post a bet'}
		</Button>
	{/snippet}
</PageHeader>

{#if form?.error}
	<p class="error" role="alert">{form.error}</p>
{/if}

<div class="counts">
	{#each chips as chip (chip)}
		<Tag variant="count" size="count" center>{chip}</Tag>
	{/each}
</div>

{#if showForm}
	<div class="post">
		<Card title="Put it on the board" tone="highlight" padding="24px" headGap={16} phoneHeadGap={14}>
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
				class="bet-form"
			>
				<TextField
					label="What's the bet?"
					wide
					name="title"
					required
					maxlength={200}
					placeholder="Gabe misses the playoffs"
				/>
				<TextField
					kind="textarea"
					label="Terms: what has to happen for you to win"
					wide
					name="terms"
					required
					rows={3}
					maxlength={2000}
					placeholder="I win if he finishes 7th or worse. Ties go to him."
					helper="Write it out properly now. This is the wording everyone argues over in December."
				/>
				<TextField
					label="Stake"
					name="stake"
					required
					maxlength={200}
					placeholder="$20, or loser buys wings"
				/>
				<TextField
					kind="select"
					label="Against"
					name="counterpartyKey"
					bind:value={counterpartyKey}
					options={againstOptions}
				/>
				<TextField
					label="Season"
					name="seasonYear"
					type="number"
					min={2005}
					max={currentYear + 1}
					placeholder={String(currentYear)}
				/>
				<div class="submit">
					<Button type="submit" size="lg" padX={26}>Post it</Button>
				</div>
			</form>
		</Card>
	</div>
{/if}

<div class="stages">
	{#each stages as stage (stage.key)}
		{@const bets = groups[stage.key]}
		<Card title={stage.title} titleSize={24} dot={stage.dot} stack={12}>
			{#if bets.length === 0}
				<p class="what">{stage.what}</p>
				<div class="empty"><Card tone="dashed" padding="16px">{stage.empty}</Card></div>
			{:else}
				{#each bets as w (w.wagerKey)}
					{#if stage.key === 'onBoard'}
						{@render openBet(w)}
					{:else if stage.key === 'live'}
						{@render liveBet(w)}
					{:else if stage.key === 'awaitingRuling'}
						{@render flaggedBet(w)}
					{:else if stage.key === 'settled'}
						{@render settledBet(w)}
					{:else}
						{@render passedBet(w)}
					{/if}
				{/each}
			{/if}
		</Card>
	{/each}
</div>

<div class="ledger-card">
	<Card
		title="The ledger"
		tone="pennant"
		headSpacing="8px 16px"
		headGap={14}
		phoneHeadGap={12}
		phoneHeadStack
	>
		{#snippet action()}
			<span class="ledger-note"
				>Settled bets only. Pushes and voided bets don't count toward a record.</span
			>
		{/snippet}
		<ul class="ledger">
			{#each ledger as row (row.managerKey)}
				<li>
					<Card tone="tile" padding="0" phonePadding="0">
						<a class="tile" href={profileHref(row.name)}>
							<Avatar name={row.name} size={32} alt="" />
							<span class="tile-name">{row.name}</span>
							<span class="record" class:zero={!row.hasRecord}>{row.record}</span>
						</a>
					</Card>
				</li>
			{/each}
		</ul>
	</Card>
</div>

<style>
	.error {
		margin: 20px 0 0;
		padding: 12px 16px;
		border: 1px solid var(--color-pennant-line);
		border-radius: var(--radius-control);
		background: var(--color-pennant-tint);
		font-size: 14px;
		color: var(--color-pennant-dark);
	}

	.counts {
		margin-top: 20px;
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}

	.post {
		margin-top: 16px;
	}

	.bet-form {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		gap: 16px;
	}

	.submit {
		grid-column: 1 / -1;
		display: flex;
		justify-content: flex-end;
	}

	.stages {
		margin-top: 20px;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
		gap: 16px;
	}

	.what {
		margin: 0;
		font-size: 14px;
		line-height: 1.45;
		color: var(--color-muted);
	}

	.empty {
		margin-top: auto;
	}

	.bet-head {
		display: flex;
		justify-content: space-between;
		gap: 8px;
	}

	.bet-title {
		font-size: 16px;
		font-weight: 700;
	}

	.stake,
	.verdict {
		font-weight: 700;
		color: var(--color-field);
	}

	.verdict.muted {
		color: var(--color-muted);
	}

	.verdict.ink-2 {
		color: var(--color-ink-2);
	}

	.terms {
		margin: 6px 0 0;
		font-size: 14px;
		line-height: 1.45;
		color: var(--color-ink-2);
	}

	.meta {
		margin-top: 8px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.settled-meta {
		margin-top: 6px;
	}

	.meta .name {
		color: inherit;
	}

	.meta .name:hover {
		color: var(--color-field);
	}

	.actions {
		margin-top: 12px;
		display: flex;
		gap: 8px;
	}

	.push-right {
		margin-left: auto;
	}

	.pair {
		margin-top: 8px;
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
		color: var(--color-ink-2);
	}

	/* The proposer and "vs" read as one run of text, "Luke S vs", as the board
	   draws it; below 640px "vs" stands on its own between the two links. */
	.side {
		display: flex;
		align-items: center;
	}

	.person {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		color: var(--color-ink-2);
	}

	a.person:hover {
		color: var(--color-field);
	}

	.over {
		margin-top: 12px;
	}

	.over-button {
		margin-top: 10px;
		display: flex;
	}

	.note {
		margin: 8px 0 0;
		padding-left: 10px;
		border-left: 2px solid var(--color-line-strong);
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.ruling {
		margin: 12px 0 0;
		padding: 0;
		min-width: 0;
		border: 0;
	}

	/* Chromium's 2px legend padding, which the base styles reset. */
	.ruling legend {
		padding: 0 2px;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-pennant-dark);
	}

	.outcomes {
		margin-top: 8px;
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.reasoning {
		margin-top: 8px;
	}

	.ruling-actions {
		margin-top: 8px;
		display: flex;
		gap: 8px;
	}

	.rule-on-it {
		margin-top: 12px;
		display: flex;
	}

	.commissioner {
		margin: 8px 0 0;
		font-size: 13px;
		color: var(--color-muted);
	}

	.ledger-card {
		margin-top: 20px;
	}

	.ledger-note {
		font-size: 14px;
		color: var(--color-ink-brown);
	}

	.ledger li {
		display: flex;
	}

	.ledger li > :global(*) {
		flex: 1;
		min-width: 0;
	}

	.ledger {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
		gap: 12px;
	}

	.tile {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 10px 12px;
	}

	.tile-name {
		flex: 1;
		min-width: 0;
		font-size: 15px;
		font-weight: 600;
	}

	.record {
		font-family: var(--font-slab);
		font-size: 18px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.record.zero {
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.error {
			margin-top: 14px;
		}

		.counts {
			margin-top: 14px;
			display: grid;
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}

		.post {
			margin-top: 14px;
		}

		.bet-form {
			display: flex;
			flex-direction: column;
			gap: 14px;
		}

		.submit {
			display: block;
		}

		.submit :global(.btn) {
			width: 100%;
		}

		.stages {
			margin-top: 14px;
			display: flex;
			flex-direction: column;
			gap: 14px;
		}

		.bet-head {
			flex-wrap: wrap;
			gap: 2px 8px;
		}

		.pair {
			margin-top: 4px;
			flex-wrap: wrap;
			gap: 0 8px;
		}

		.side {
			display: contents;
		}

		.sp {
			display: none;
		}

		.person {
			min-height: 44px;
		}

		.over {
			margin-top: 8px;
		}

		.ledger-card {
			margin-top: 14px;
		}

		.ledger {
			grid-template-columns: minmax(0, 1fr);
			gap: 8px;
		}
	}
</style>
