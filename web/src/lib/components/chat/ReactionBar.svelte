<script lang="ts">
	import { formatReactionTooltip } from '$lib/chat/format';
	import type { EmojiType, ReactionGroup } from '$lib/chat/types';

	interface Props {
		groups: ReactionGroup[];
		meManagerKey: number | null;
		canWrite: boolean;
		onToggle: (emoji: string, emojiType: EmojiType) => void;
		onOpenPicker: (anchor: HTMLElement) => void;
	}

	let { groups, meManagerKey, canWrite, onToggle, onOpenPicker }: Props = $props();
</script>

<!-- The Chat board's reaction pills (WP12 S12.3). Each pill is a span inside
     its button, so on a touch screen the button can grow to a 44px target
     around a 28px pill, as the phone board draws it. -->
{#if groups.length > 0}
	<div class="reactions">
		{#each groups as group (group.emoji)}
			<button
				type="button"
				class="hit"
				disabled={!canWrite}
				aria-pressed={group.mine}
				aria-label={formatReactionTooltip(group, meManagerKey)}
				title={formatReactionTooltip(group, meManagerKey)}
				onclick={() => onToggle(group.emoji, group.emojiType)}
			>
				<span class="pill" class:mine={group.mine}>{group.emoji} {group.count}</span>
			</button>
		{/each}

		{#if canWrite}
			<button
				type="button"
				class="hit add"
				aria-label="Add a reaction"
				title="Add a reaction"
				onclick={(event) => onOpenPicker(event.currentTarget as HTMLElement)}
			>
				<span class="pill plus" aria-hidden="true">+</span>
			</button>
		{/if}
	</div>
{/if}

<style>
	.reactions {
		margin-top: 8px;
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.hit {
		display: inline-flex;
		align-items: center;
		padding: 0;
		border: 0;
		background: transparent;
		font-family: var(--font-sans);
		cursor: pointer;
	}

	.hit:disabled {
		cursor: default;
	}

	.pill {
		box-sizing: border-box;
		display: inline-flex;
		align-items: center;
		gap: 5px;
		min-height: 28px;
		padding: 0 9px;
		border: 1px solid var(--color-line);
		border-radius: 999px;
		background: var(--color-card);
		font-size: 13px;
		font-weight: 700;
		color: var(--color-ink);
		font-variant-numeric: tabular-nums;
	}

	.hit:not(:disabled):hover .pill:not(.mine) {
		border-color: var(--color-line-strong);
	}

	.pill.mine {
		border-color: var(--color-field);
		background: var(--color-field-tint);
	}

	.pill.plus {
		justify-content: center;
		width: 30px;
		padding: 0;
		border: 1px dashed var(--color-line-strong);
		background: transparent;
		font-size: 14px;
		font-weight: 400;
		color: var(--color-muted);
	}

	.hit.add:hover .pill.plus {
		color: var(--color-ink);
	}

	.hit:focus-visible {
		outline: none;
	}

	.hit:focus-visible .pill {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	/* The phone board: each pill in a 44px tall button and the + in a 44px
	   square, so the pills sit 8px under the text with no margin of their own. */
	@media (hover: none) {
		.reactions {
			margin-top: 0;
			gap: 0 6px;
		}

		.hit {
			min-height: 44px;
		}

		.hit.add {
			min-width: 44px;
		}
	}
</style>
