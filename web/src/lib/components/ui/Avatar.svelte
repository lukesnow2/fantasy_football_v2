<script lang="ts">
	/**
	 * A manager's photo as a circle, at any size in px (the boards use 20 to 128;
	 * take each one from its board). It wraps ManagerProfilePicture, so the photo
	 * list stays in one place. A manager with no photo gets a wash circle with
	 * their first initial in ink-2, weight 700, at 40% of the circle.
	 */
	import ManagerProfilePicture from '../ManagerProfilePicture.svelte';
	import { getManagerProfilePicture } from '$lib/utils/managerUtils';
	import { avatarInitial } from './parts';

	interface Props {
		name: string;
		size?: number;
		class?: string;
	}

	let { name, size = 28, class: className = '' }: Props = $props();

	const hasPhoto = $derived(getManagerProfilePicture(name) !== null);
</script>

{#if hasPhoto}
	<span class="avatar {className}" style:width="{size}px" style:height="{size}px">
		<ManagerProfilePicture managerName={name} />
	</span>
{:else}
	<span
		class="avatar initial {className}"
		role="img"
		aria-label={name}
		style:width="{size}px"
		style:height="{size}px"
		style:font-size="{size * 0.4}px">{avatarInitial(name)}</span
	>
{/if}

<style>
	.avatar {
		display: inline-flex;
		flex: none;
		align-items: center;
		justify-content: center;
		border-radius: 50%;
		overflow: hidden;
	}

	/* ManagerProfilePicture brings its own fixed size; the avatar's wins. */
	.avatar > :global(*) {
		width: 100%;
		height: 100%;
	}

	.initial {
		background: var(--color-wash);
		color: var(--color-ink-2);
		font-weight: 700;
		line-height: 1;
	}
</style>
