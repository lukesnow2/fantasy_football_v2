<script lang="ts">
	import { enhance } from '$app/forms';
	import type { SubmitFunction } from '@sveltejs/kit';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { Avatar, Button, Card, DataTable, EmptyValue, Tag, type Column } from '$lib/components/ui';
	import { formatMemberDate } from './member-dates';
	import type { ActionData, PageData } from './$types';

	let { data, form }: { data: PageData; form: ActionData } = $props();

	type Member = PageData['members'][number];

	// Widths come from page CSS so the phone table can draw its own (S12.10).
	const columns: Column[] = [
		{ key: 'manager', label: 'Manager', width: 'var(--w-manager)' },
		{ key: 'email', label: 'Email', width: 'var(--w-email)' },
		{ key: 'role', label: 'Role', width: 'var(--w-role)' },
		{ key: 'invited', label: 'Invited', width: 'var(--w-date)' },
		{ key: 'firstLogin', label: 'First login', width: 'var(--w-date)' },
		{ key: 'actions', label: 'Actions', width: 'var(--w-actions)', numeric: true }
	];

	// The role select saves on change. If the action refuses (the last
	// commissioner can't step down), the select goes back to the stored role
	// and the strip says why.
	function saveRole(stored: string): SubmitFunction {
		return ({ formElement }) =>
			async ({ result, update }) => {
				await update({ reset: false });
				if (result.type !== 'success') {
					const select = formElement.querySelector('select');
					if (select) select.value = stored;
				}
			};
	}
</script>

<svelte:head>
	<title>League members | The League</title>
</svelte:head>

<PageHeader kicker="Commissioner only" kickerTone="pennant" title="League members" size={56} introWidth={640}>
	The allowlist. Only these addresses can sign in, and there's no registration.
</PageHeader>

{#if !data.mailConfig.ok}
	<!-- Not on the board: kept above the cards until Luke places it. -->
	<div class="strip error mail" role="alert">
		<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"
			><circle cx="12" cy="12" r="9"></circle><path d="M12 8v4.5M12 16h.01"></path></svg
		>
		<div>
			<p class="mail-title">Sign-in email isn't configured</p>
			<p>
				Invites and sign-in links won't reach anyone until this is fixed. Set these in the
				deployment's environment variables.
			</p>
			<ul>
				{#each data.mailConfig.problems as problem (problem)}
					<li>{problem}</li>
				{/each}
			</ul>
		</div>
	</div>
{/if}

<div class="top">
	<!--
		Always rendered, not only on failure. ORIGIN is the one setting whose wrong
		value is completely silent: mail sends, nothing throws, and a hosting
		dashboard that stores it write-only will not read it back. Showing it here
		is the only place anyone can see what domain the links actually carry.
	-->
	<Card title="Sign-in links point at" titleSize={24} headGap={12} phoneHeadGap={10} class="origin-card">
		<div class="well">
			<span class="dot" class:bad={!data.mailConfig.originOk} aria-hidden="true"></span>
			<span class="origin">{data.mailConfig.origin}/login/verify</span>
		</div>
		<p class="origin-note">
			{#if !data.mailConfig.originOk}<strong class="expected">Expected {data.mailConfig.canonicalOrigin}.</strong>
			{/if}If this isn't the site's address, change ORIGIN in the deployment's environment variables
			and redeploy. Env values are baked in per deployment.
		</p>
	</Card>

	{#if data.unclaimed.length > 0}
		<Card title="Not on the allowlist yet" titleSize={24} headGap={6} class="add-card">
			<p class="add-note">
				They can't sign in until they have an address here. Adding one takes effect immediately, with
				no seed script and no redeploy.
			</p>
			<form method="POST" action="?/addMember" use:enhance class="add-form">
				<label class="field manager-field">
					Manager
					<select name="managerKey" required>
						<option value="" selected disabled>Managers without an address</option>
						{#each data.unclaimed as manager (manager.managerKey)}
							<option value={manager.managerKey}>{manager.managerName}</option>
						{/each}
					</select>
				</label>
				<label class="field email-field">
					Email
					<input name="email" type="email" required placeholder="name@example.com" />
				</label>
				<label class="field role-field">
					Role
					<select name="role">
						<option value="member">Member</option>
						<option value="commissioner">Commissioner</option>
					</select>
				</label>
				<Button type="submit" height={46} padX={22} bold class="add">Add</Button>
			</form>
		</Card>
	{/if}
</div>

{#if form?.error}
	<div class="strip error" role="alert">
		<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"
			><circle cx="12" cy="12" r="9"></circle><path d="M12 8v4.5M12 16h.01"></path></svg
		>{form.error}
	</div>
{:else if form?.success}
	<div class="strip success" role="status">
		<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"
			><path d="M5 12l5 5 9-10"></path></svg
		>{form.success}
	</div>
{/if}

<Card
	title="On the allowlist"
	titleSize={26}
	headGap={12}
	padding="22px 24px 12px"
	phonePadding="16px 16px 10px"
	class="list-card"
>
	<div class="table">
		<DataTable
			{columns}
			rows={data.members as (Member & Record<string, unknown>)[]}
			label="On the allowlist"
			rowKey={(member) => member.id as string}
			rowPadding={10}
			columnGap={14}
			minWidth="var(--table-min)"
			stickyFirst="phone"
			stickyPad={10}
		>
			{#snippet cell(row, column)}
				{@const member = row as unknown as Member}
				{#if column.key === 'manager'}
					<span class="manager">
						<span class="who" class:off={!member.active}>
							<Avatar name={member.displayName ?? member.email} size={30} phoneSize={26} alt="" />
							<span class="name">{member.displayName ?? member.email}</span>
						</span>
						{#if !member.active}<Tag variant="missed" size="flag">Deactivated</Tag>{/if}
					</span>
				{:else if column.key === 'email'}
					<form method="POST" action="?/updateEmail" use:enhance={() => async ({ update }) => update({ reset: false })} class="email-form">
						<input type="hidden" name="memberId" value={member.id} />
						<input
							name="email"
							type="email"
							value={member.email}
							aria-label="Email for {member.displayName ?? member.email}"
							class="email-input"
						/>
						<Button type="submit" variant="icon" size="row" aria-label="Save email">✓</Button>
					</form>
				{:else if column.key === 'role'}
					<form method="POST" action="?/setRole" use:enhance={saveRole(member.role)}>
						<input type="hidden" name="memberId" value={member.id} />
						<select
							name="role"
							value={member.role}
							aria-label="Role"
							class="role-select"
							onchange={(event) => event.currentTarget.form?.requestSubmit()}
						>
							<option value="member">Member</option>
							<option value="commissioner">Commissioner</option>
						</select>
					</form>
				{:else if column.key === 'invited' || column.key === 'firstLogin'}
					{@const when = formatMemberDate(column.key === 'invited' ? member.invitedAt : member.firstLoginAt)}
					<span class="date">{#if when}{when}{:else}<EmptyValue text="Not yet" />{/if}</span>
				{:else if column.key === 'actions'}
					<span class="actions">
						<form method="POST" action="?/invite" use:enhance>
							<input type="hidden" name="memberId" value={member.id} />
							<Button type="submit" variant="secondary" size="row" tone="field" disabled={!member.active}
								>{member.firstLoginAt ? 'Resend link' : 'Invite'}</Button
							>
						</form>
						<form method="POST" action="?/setActive" use:enhance>
							<input type="hidden" name="memberId" value={member.id} />
							<input type="hidden" name="active" value={(!member.active).toString()} />
							<Button type="submit" variant="secondary" size="row">{member.active ? 'Deactivate' : 'Reactivate'}</Button>
						</form>
					</span>
				{/if}
			{/snippet}
			{#snippet empty()}
				Nobody on the allowlist yet. Add managers from the panel above.
			{/snippet}
		</DataTable>
	</div>
	<p class="table-note">
		Deactivating someone blocks future sign-ins and ends the sessions they have open now.
	</p>
</Card>

<style>
	.top {
		margin-top: 28px;
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: stretch;
	}

	/* Content-box, as the board's cards are: the bases are their text width,
	   with the padding and border outside. */
	.top :global(.origin-card),
	.top :global(.add-card) {
		box-sizing: content-box;
	}

	.top :global(.origin-card) {
		flex: 1 1 340px;
		min-width: 0;
	}

	.top :global(.add-card) {
		flex: 2 1 520px;
		min-width: 0;
	}

	.well {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 12px 14px;
		border-radius: 10px;
		background: var(--color-ground);
	}

	.dot {
		flex: none;
		width: 10px;
		height: 10px;
		border-radius: 50%;
		background: var(--color-field);
	}

	.dot.bad {
		background: var(--color-pennant);
	}

	.origin {
		min-width: 0;
		font-family: var(--font-mono);
		font-size: 14px;
		color: var(--color-ink);
		overflow-wrap: anywhere;
	}

	.origin-note {
		margin: 12px 0 0;
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.expected {
		display: block;
		font-weight: 600;
		color: var(--color-pennant-dark);
	}

	.add-note {
		margin: 0;
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.add-form {
		margin-top: 14px;
		display: flex;
		flex-wrap: wrap;
		gap: 10px;
		align-items: flex-end;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 6px;
		font-size: 14px;
		font-weight: 600;
	}

	.manager-field {
		flex: 1 1 160px;
	}

	.email-field {
		flex: 2 1 220px;
	}

	.role-field {
		flex: 1 1 140px;
	}

	/* The forms plugin redraws selects and inputs; these are the board's. */
	.field select,
	.field input {
		box-sizing: border-box;
		min-height: 46px;
		padding: 0 12px;
		border: 1px solid var(--color-line-strong);
		border-radius: var(--radius-control);
		background-color: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 400;
		color: var(--color-ink);
		box-shadow: none;
	}

	.field select {
		padding-right: 36px;
	}

	.field input::placeholder {
		color: var(--color-faint);
	}

	.field select:focus,
	.field input:focus,
	.email-input:focus,
	.role-select:focus {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
		box-shadow: none;
	}

	.strip {
		margin-top: 20px;
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 12px 16px;
		border-radius: 10px;
		font-size: 14px;
		font-weight: 600;
	}

	.strip svg {
		flex: none;
	}

	.success {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.success svg {
		color: var(--color-field);
	}

	.error {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}

	.mail {
		align-items: flex-start;
		font-weight: 400;
	}

	.mail svg {
		margin-top: 2px;
	}

	.mail p {
		margin: 0;
	}

	.mail-title {
		font-weight: 700;
	}

	.mail ul {
		margin: 6px 0 0;
		padding-left: 18px;
		list-style: disc;
	}

	:global(.list-card) {
		margin-top: 20px;
	}

	.table {
		--w-manager: auto;
		--w-email: 220px;
		--w-role: 130px;
		--w-date: 96px;
		--w-actions: 200px;
		--table-min: 992px;
	}

	/* Fixed, so the Manager column alone takes the width the others leave,
	   as the board's minmax(180px, 1fr) does. */
	.table :global(table) {
		table-layout: fixed;
	}

	.manager {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.who {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
		font-weight: 600;
	}

	.who.off {
		opacity: 0.5;
	}

	.email-form {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.email-input {
		box-sizing: border-box;
		flex: 1;
		min-width: 0;
		height: 32px;
		padding: 0 10px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius-tag);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 14px;
		color: var(--color-ink);
		box-shadow: none;
	}

	.role-select {
		box-sizing: border-box;
		min-height: 32px;
		padding: 0 28px 0 6px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius-tag);
		background-color: var(--color-card);
		background-position: right 4px center;
		font-family: var(--font-sans);
		font-size: 13px;
		color: var(--color-ink);
		box-shadow: none;
	}

	.date {
		color: var(--color-ink-2);
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		gap: 6px;
	}

	.table-note {
		margin: 12px 0 4px;
		font-size: 13px;
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.top {
			margin-top: 20px;
			flex-direction: column;
			gap: 14px;
		}

		.top :global(.origin-card),
		.top :global(.add-card) {
			flex: none;
		}

		.origin-note {
			margin-top: 10px;
		}

		.add-form {
			flex-direction: column;
			align-items: stretch;
			gap: 12px;
		}

		.field {
			flex: none;
		}

		.field select,
		.field input {
			font-size: 16px;
		}

		.add-form :global(.add) {
			margin-top: 2px;
			width: 100%;
		}

		.strip {
			margin-top: 14px;
		}

		:global(.list-card) {
			margin-top: 14px;
		}

		/* The board's 170px Manager column holds its 10px of sticky padding. */
		.table {
			--w-manager: 160px;
			--w-role: 150px;
			--table-min: 1002px;
		}

		.table :global(td) {
			padding-top: 6px;
			padding-bottom: 6px;
			font-size: 14px;
		}

		.manager,
		.who {
			gap: 8px;
		}

		.name {
			white-space: nowrap;
		}

		.email-input {
			height: 44px;
			font-size: 16px;
		}

		.role-select {
			width: 100%;
			min-height: 44px;
			font-size: 16px;
		}

		.table-note {
			margin-bottom: 0;
			line-height: 1.5;
		}
	}
</style>
