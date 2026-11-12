<script lang="ts">
	/**
	 * A face-off name that opens a list of managers (League History · Head to
	 * head, S10.5). The name is a button in slab 700 with a dashed underline and
	 * a pennant-light chevron; it opens a listbox under it: a white panel 240px
	 * wide, 300px tall at most, one option per manager with a 26px Avatar, the
	 * current one in field-tint.
	 *
	 * Picking closes it, and so do Escape and a click outside. The arrow keys
	 * move through the options, Home and End jump to the ends. `open` is
	 * bindable so the page keeps only one list open at a time.
	 *
	 * Below 640px the list sits in the flow under the name, the full width of
	 * the card, and pushes the rest of the card down (the phone board).
	 */
	import { tick } from 'svelte';
	import { Avatar } from '$lib/components/ui';

	interface Props {
		name: string;
		options: string[];
		open: boolean;
		/** Which edge the list lines up with. */
		align?: 'start' | 'end';
		onpick: (name: string) => void;
		ontoggle: (open: boolean) => void;
	}

	let { name, options, open, align = 'start', onpick, ontoggle }: Props = $props();

	let root: HTMLElement | undefined = $state();
	let trigger: HTMLButtonElement | undefined = $state();
	let optionEls: HTMLButtonElement[] = $state([]);

	const listId = $props.id();

	async function toggle() {
		const next = !open;
		ontoggle(next);
		if (next) {
			await tick();
			const current = options.indexOf(name);
			optionEls[current >= 0 ? current : 0]?.focus();
		}
	}

	function close(refocus = false) {
		ontoggle(false);
		if (refocus) trigger?.focus();
	}

	function pick(option: string) {
		onpick(option);
		close(true);
	}

	function onListKey(event: KeyboardEvent) {
		const index = optionEls.findIndex((el) => el === document.activeElement);
		let next: number | null = null;
		if (event.key === 'ArrowDown') next = Math.min(options.length - 1, index + 1);
		else if (event.key === 'ArrowUp') next = Math.max(0, index - 1);
		else if (event.key === 'Home') next = 0;
		else if (event.key === 'End') next = options.length - 1;
		else if (event.key === 'Escape') {
			event.preventDefault();
			close(true);
			return;
		} else if (event.key === 'Tab') {
			close();
			return;
		}
		if (next == null) return;
		event.preventDefault();
		optionEls[next]?.focus();
	}

	function onWindowClick(event: MouseEvent) {
		if (open && root && !root.contains(event.target as Node)) close();
	}
</script>

<svelte:window onclick={onWindowClick} />

<div class="picker align-{align}" bind:this={root}>
	<button
		bind:this={trigger}
		type="button"
		class="name"
		aria-haspopup="listbox"
		aria-expanded={open}
		aria-controls={open ? listId : undefined}
		aria-label="Change manager, now {name}"
		onclick={toggle}
		>{name}<svg
			width="16"
			height="16"
			viewBox="0 0 24 24"
			fill="none"
			stroke-width="2.5"
			stroke-linecap="round"
			stroke-linejoin="round"
			aria-hidden="true"><path d="M6 9l6 6 6-6" /></svg
		></button
	>
	{#if open}
		<!-- svelte-ignore a11y_interactive_supports_focus -->
		<div id={listId} class="list" role="listbox" aria-label="Pick a manager" tabindex="-1" onkeydown={onListKey}>
			{#each options as option, i (option)}
				<button
					bind:this={optionEls[i]}
					type="button"
					role="option"
					class="option"
					class:on={option === name}
					aria-selected={option === name}
					onclick={() => pick(option)}
					><Avatar name={option} size={26} alt="" />{option}</button
				>
			{/each}
		</div>
	{/if}
</div>

<style>
	.picker {
		position: relative;
		display: inline-block;
	}

	.name {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		min-height: 44px;
		padding: 0 6px 0 0;
		border: 0;
		border-bottom: 2px dashed var(--color-clubhouse-line);
		background: transparent;
		color: var(--color-white);
		cursor: pointer;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
	}

	.align-end .name {
		padding: 0 0 0 6px;
	}

	.name svg {
		flex: none;
		stroke: var(--color-pennant-light);
	}

	.name:focus-visible {
		outline: 2px solid var(--color-white);
		outline-offset: 2px;
	}

	.list {
		position: absolute;
		top: calc(100% + 6px);
		left: 0;
		z-index: 3;
		/* The board's 240px and 300px are content-box, inside the 6px of padding. */
		box-sizing: content-box;
		width: 240px;
		max-height: 300px;
		overflow-y: auto;
		padding: 6px;
		border-radius: var(--radius-card);
		background: var(--color-card);
		color: var(--color-ink);
		box-shadow: var(--shadow-dark-pop);
		text-align: left;
	}

	.align-end .list {
		left: auto;
		right: 0;
	}

	.option {
		display: flex;
		width: 100%;
		align-items: center;
		gap: 10px;
		padding: 8px 10px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		color: var(--color-ink);
		cursor: pointer;
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		text-align: left;
	}

	.option:hover,
	.option:focus-visible {
		background: var(--color-field-wash);
		outline: none;
	}

	.option.on {
		background: var(--color-field-tint);
	}

	@media (max-width: 639.98px) {
		.picker {
			display: block;
		}

		.name {
			font-size: 26px;
			padding: 0 2px 0 6px;
		}

		.align-end .name {
			padding: 0 2px 0 6px;
		}

		.list {
			position: static;
			box-sizing: border-box;
			margin-top: 8px;
			width: auto;
		}

		.option {
			min-height: 44px;
			padding: 9px 10px;
		}
	}
</style>
