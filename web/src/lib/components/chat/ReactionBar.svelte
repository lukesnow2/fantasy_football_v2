<script lang="ts">
	import { SmilePlus } from 'lucide-svelte';
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

{#if groups.length > 0}
	<div class="mt-1 flex flex-wrap items-center gap-1">
		{#each groups as group (group.emoji)}
			<button
				type="button"
				disabled={!canWrite}
				aria-pressed={group.mine}
				aria-label={formatReactionTooltip(group, meManagerKey)}
				title={formatReactionTooltip(group, meManagerKey)}
				onclick={() => onToggle(group.emoji, group.emojiType)}
				class="flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs transition-colors disabled:cursor-default
					{group.mine
					? 'border-amber-500/60 bg-amber-500/15 text-amber-200'
					: 'border-slate-600/70 bg-slate-700/50 text-slate-300 hover:border-slate-500 hover:bg-slate-700'}"
			>
				<span class="text-sm leading-none">{group.emoji}</span>
				<span class="tabular-nums">{group.count}</span>
			</button>
		{/each}

		{#if canWrite}
			<button
				type="button"
				aria-label="Add a reaction"
				title="Add a reaction"
				onclick={(event) => onOpenPicker(event.currentTarget as HTMLElement)}
				class="flex items-center rounded-full border border-slate-600/70 bg-slate-700/50 px-1.5 py-1 text-slate-400 transition-colors hover:border-slate-500 hover:bg-slate-700 hover:text-slate-200"
			>
				<SmilePlus class="h-3.5 w-3.5" />
			</button>
		{/if}
	</div>
{/if}
