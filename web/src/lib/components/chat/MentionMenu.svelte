<script lang="ts">
	import { autoUpdate, computePosition, flip, offset, shift } from '@floating-ui/dom';
	import { MediaQuery } from 'svelte/reactivity';
	import ChatAvatar from './ChatAvatar.svelte';
	import type { RosterMember } from '$lib/chat/types';

	interface Props {
		/** The composer's box: the list opens above it, aligned to its left. */
		anchor: HTMLElement | null;
		matches: RosterMember[];
		activeIndex: number;
		onPick: (member: RosterMember) => void;
	}

	let { anchor, matches, activeIndex, onPick }: Props = $props();

	let menu = $state<HTMLDivElement | null>(null);

	// On a phone the list sits in the composer's flow, 8px above the box, and
	// the feed shrinks to make room, as the phone board draws it (WP12 S12.10).
	const phone = new MediaQuery('max-width: 639.98px');

	$effect(() => {
		if (phone.current || !menu || !anchor || matches.length === 0) return;
		const menuEl = menu;
		const anchorEl = anchor;

		const stop = autoUpdate(anchorEl, menuEl, async () => {
			const { x, y } = await computePosition(anchorEl, menuEl, {
				strategy: 'fixed',
				placement: 'top-start',
				middleware: [offset(8), flip(), shift({ padding: 8 })]
			});
			Object.assign(menuEl.style, { left: `${x}px`, top: `${y}px` });
		});
		return () => {
			stop();
			Object.assign(menuEl.style, { left: '', top: '' });
		};
	});
</script>

{#if matches.length > 0}
	<!-- Keyboard handling lives in the composer, which owns focus. This is a
	     listbox the composer drives, not a focus trap. -->
	<div bind:this={menu} role="listbox" aria-label="Mention a manager" class="menu">
		{#each matches as member, index (member.managerKey)}
			<button
				type="button"
				role="option"
				class="option"
				class:active={index === activeIndex}
				aria-selected={index === activeIndex}
				onmousedown={(event) => {
					// mousedown, not click: the composer must not lose focus first, or
					// the caret position we are about to splice into is gone.
					event.preventDefault();
					onPick(member);
				}}
			>
				<ChatAvatar
					managerName={member.name}
					displayName={member.displayName}
					profileImageUrl={member.profileImageUrl}
					size={26}
				/>
				<span class="label">{member.displayName}</span>
			</button>
		{/each}
	</div>
{/if}

<style>
	.menu {
		position: fixed;
		z-index: 50;
		box-sizing: border-box;
		width: 260px;
		padding: 6px;
		border: 1px solid var(--color-line-strong);
		border-radius: 10px;
		background: var(--color-card);
		box-shadow: var(--shadow-popover);
	}

	.option {
		display: flex;
		width: 100%;
		align-items: center;
		gap: 10px;
		padding: 8px 10px;
		border: 0;
		border-radius: 8px;
		background: transparent;
		text-align: left;
		font-family: var(--font-sans);
		font-size: 16px;
		font-weight: 600;
		color: var(--color-ink);
		cursor: pointer;
	}

	.option:hover {
		background: var(--color-field-wash);
	}

	.option.active {
		background: var(--color-field-tint);
	}

	.label {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	@media (max-width: 639.98px) {
		.menu {
			position: static;
			width: auto;
			max-width: 260px;
			margin-bottom: 8px;
		}

		.option {
			min-height: 44px;
			padding: 0 10px;
			font-size: 15px;
		}
	}
</style>
