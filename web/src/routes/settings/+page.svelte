<script lang="ts">
	import { enhance } from '$app/forms';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { Avatar, Button, Card, Switch, Tag } from '$lib/components/ui';
	import type { ActionData, PageData } from './$types';

	let { data, form }: { data: PageData; form: ActionData } = $props();

	const prefs = $derived(data.manager?.notificationPreferences);

	type PrefName = 'emailOnNewProposal' | 'emailOnVoteResults' | 'emailOnTradeOffers';

	function saved(): Record<PrefName, boolean> {
		return {
			emailOnNewProposal: !!prefs?.emailOnNewProposal,
			emailOnVoteResults: !!prefs?.emailOnVoteResults,
			emailOnTradeOffers: !!prefs?.emailOnTradeOffers
		};
	}

	// The switches' own state, seeded from what's saved so the server renders
	// them right too, and reseeded when a save reloads the data. A change hides
	// the last "Saved." until the next save, so it never vouches for an
	// unsaved switch.
	let checks = $state(saved());
	let changed = $state(false);

	$effect.pre(() => {
		checks = saved();
	});

	const profileHref = $derived(
		data.manager ? `/managers/${encodeURIComponent(data.manager.managerName ?? data.manager.displayName)}` : null
	);

	const ROWS = [
		{
			name: 'emailOnNewProposal',
			label: 'New rule proposals',
			detail: 'When someone opens a constitutional amendment for voting.'
		},
		{ name: 'emailOnVoteResults', label: 'Vote results', detail: 'When a proposal passes or fails.' },
		{ name: 'emailOnTradeOffers', label: 'Trade offers', detail: 'When another manager sends you an offer.' }
	] as const;
</script>

<svelte:head>
	<title>Settings | The League</title>
</svelte:head>

<div class="settings">
	<PageHeader kicker="Your account" title="Settings" size={56}>
		Your profile and how the league gets in touch with you.
	</PageHeader>

	<div class="sections">
		{#if data.manager}
			<Card title="Profile" titleSize={26} headGap={6} phoneHeadGap={4}>
				<dl class="rows">
					<div class="row">
						<dt>Manager</dt>
						<dd>
							<a class="manager" href={profileHref}>
								<Avatar name={data.manager.managerName ?? data.manager.displayName} size={32} alt="" />
								{data.manager.displayName}
							</a>
						</dd>
					</div>
					<div class="row">
						<dt>Email</dt>
						<dd class="email">{data.manager.email}</dd>
					</div>
					<div class="row">
						<dt>Role</dt>
						<dd><Tag variant="passed">{data.role === 'commissioner' ? 'Commissioner' : 'Member'}</Tag></dd>
					</div>
				</dl>
				<p class="note">Your email is how you sign in. Ask the commissioner to change it.</p>
			</Card>

			<Card title="Email notifications" titleSize={26} headGap={6} phoneHeadGap={4}>
				<form
					method="POST"
					action="?/notifications"
					use:enhance={() => {
						return async ({ update }) => {
							// Keep the switches where they are: a reset would put them back
							// to how the page first loaded, not to what was just saved.
							await update({ reset: false });
							changed = false;
						};
					}}
				>
					{#each ROWS as row (row.name)}
						<label class="pref">
							<span>
								<span class="pref-name">{row.label}</span>
								<span class="pref-detail">{row.detail}</span>
							</span>
							<Switch
								name={row.name}
								label={row.label}
								bind:checked={checks[row.name]}
								onchange={() => (changed = true)}
							/>
						</label>
					{/each}

					<div class="save">
						{#if form?.error}
							<span class="status error" role="alert">{form.error}</span>
						{:else if form?.success && !changed}
							<span class="status saved" role="status">Saved.</span>
						{/if}
						<Button type="submit" height={46} padX={20} bold>Save preferences</Button>
					</div>
				</form>
			</Card>
		{:else}
			<!-- Not drawn on the board (D3): one card in place of the two above. -->
			<Card title="Not linked to a manager" titleSize={22} headGap={6}>
				<p class="unlinked">
					Your account isn't linked to a manager profile. Ask the commissioner to check the roster.
				</p>
			</Card>
		{/if}

		<Card tone="wash" padding="18px 22px" phonePadding="14px 16px" label="Signing in">
			<h2 class="signing-title">Signing in</h2>
			<p class="signing-body">
				The League uses emailed sign-in links. There's no password to manage, and links expire after
				15 minutes.
			</p>
		</Card>
	</div>
</div>

<style>
	/* The board's main is 760px wide with 24px gutters: a 712px column. */
	.settings {
		max-width: 712px;
		margin: 0 auto;
	}

	.sections {
		margin-top: 28px;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.rows {
		margin: 0;
	}

	.row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 12px;
		padding: 12px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.row:last-child {
		border-bottom: 0;
	}

	dt {
		flex: none;
		color: var(--color-ink-2);
	}

	/* A flex box, so the avatar link and the tag sit at their own height with
	   no line box around them. */
	dd {
		margin: 0;
		min-width: 0;
		display: flex;
		justify-content: flex-end;
	}

	.manager {
		display: inline-flex;
		align-items: center;
		gap: 10px;
		font-weight: 700;
	}

	.email {
		text-align: right;
		overflow-wrap: anywhere;
	}

	.note {
		margin: 6px 0 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.pref {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 16px;
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
		cursor: pointer;
	}

	.pref-name {
		display: block;
		font-size: 16px;
		font-weight: 700;
	}

	.pref-detail {
		display: block;
		margin-top: 2px;
		font-size: 14px;
		color: var(--color-muted);
	}

	.save {
		margin-top: 16px;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: flex-end;
		gap: 14px;
	}

	.status {
		font-size: 14px;
		font-weight: 600;
	}

	.saved {
		color: var(--color-field);
	}

	.error {
		color: var(--color-pennant-dark);
	}

	.unlinked {
		margin: 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.signing-title {
		margin: 0 0 4px;
		font-size: 16px;
		font-weight: 700;
	}

	.signing-body {
		margin: 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	@media (max-width: 639.98px) {
		.settings {
			max-width: none;
		}

		.sections {
			margin-top: 20px;
			gap: 14px;
		}

		/* Content-box, as the phone board draws it: 44px plus the padding. */
		.row {
			box-sizing: content-box;
			min-height: 44px;
			padding: 6px 0;
			font-size: 15px;
		}

		.manager {
			min-height: 44px;
		}

		.note {
			margin-top: 4px;
			line-height: 1.5;
		}

		.pref {
			padding: 12px 0;
		}

		.pref-name {
			font-size: 15px;
		}

		.pref-detail {
			line-height: 1.4;
		}

		.save {
			margin-top: 14px;
		}
	}
</style>
