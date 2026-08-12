<script lang="ts">
	import { autoUpdate, computePosition, flip, offset, shift } from '@floating-ui/dom';
	import ChatAvatar from './ChatAvatar.svelte';
	import type { RosterMember } from '$lib/chat/types';

	interface Props {
		anchor: HTMLElement | null;
		matches: RosterMember[];
		activeIndex: number;
		onPick: (member: RosterMember) => void;
	}

	let { anchor, matches, activeIndex, onPick }: Props = $props();

	let menu = $state<HTMLDivElement | null>(null);

	$effect(() => {
		if (!menu || !anchor || matches.length === 0) return;
		const menuEl = menu;
		const anchorEl = anchor;

		return autoUpdate(anchorEl, menuEl, async () => {
			const { x, y } = await computePosition(anchorEl, menuEl, {
				strategy: 'fixed',
				placement: 'top-start',
				middleware: [offset(6), flip(), shift({ padding: 8 })]
			});
			Object.assign(menuEl.style, { left: `${x}px`, top: `${y}px` });
		});
	});
</script>

{#if matches.length > 0}
	<!-- Keyboard handling lives in the composer, which owns focus. This is a
	     listbox the composer drives, not a focus trap. -->
	<div
		bind:this={menu}
		role="listbox"
		aria-label="Mention a manager"
		class="fixed z-50 w-64 overflow-hidden rounded-lg border border-slate-600 bg-slate-800 py-1 shadow-xl"
	>
		{#each matches as member, index (member.managerKey)}
			<button
				type="button"
				role="option"
				aria-selected={index === activeIndex}
				onmousedown={(event) => {
					// mousedown, not click: the composer must not lose focus first, or
					// the caret position we are about to splice into is gone.
					event.preventDefault();
					onPick(member);
				}}
				class="flex w-full items-center gap-2 px-3 py-1.5 text-left text-sm transition-colors
					{index === activeIndex ? 'bg-slate-700 text-white' : 'text-slate-300 hover:bg-slate-700/60'}"
			>
				<ChatAvatar
					managerName={member.name}
					displayName={member.displayName}
					profileImageUrl={member.profileImageUrl}
					size="sm"
				/>
				<span class="truncate">{member.displayName}</span>
			</button>
		{/each}
	</div>
{/if}
