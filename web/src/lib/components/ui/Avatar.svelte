<script lang="ts">
	/**
	 * A manager's photo as a circle, at any size in px (the boards use 20 to 128;
	 * take each one from its board). It wraps ManagerProfilePicture, so the photo
	 * list stays in one place. A manager with no photo gets a wash circle with
	 * their first initial in ink-2, weight 700, at 40% of the circle.
	 *
	 * `ring` draws a border of that many px around the photo, outside its
	 * size, as the boards' `border: 4px solid` does (the Managers hero: 120px
	 * plus a 4px pennant-light ring). `ringStyle="shadow"` draws it as a
	 * box-shadow instead, which takes no room (the Managers rail's 2px ring on
	 * the selected manager). `alt` is the photo's alt text; pass "" where the
	 * name is already beside it, which also hides the initial circle from
	 * screen readers.
	 *
	 * `phoneSize` is the size below 640px, when the phone board draws the
	 * avatar smaller (League History's face-off: 88px, 64px on a phone).
	 */
	import ManagerProfilePicture from '../ManagerProfilePicture.svelte';
	import { getManagerProfilePicture } from '$lib/utils/managerUtils';
	import { avatarInitial } from './parts';

	interface Props {
		name: string;
		size?: number;
		/** The size in px below 640px, when the phone board draws another. */
		phoneSize?: number;
		ring?: number;
		/** The ring's color: white (card) unless the board draws another. */
		ringColor?: string;
		ringStyle?: 'border' | 'shadow';
		alt?: string;
		initialTone?: 'wash' | 'field';
		initialSize?: number;
		class?: string;
	}

	let {
		name,
		size = 28,
		phoneSize,
		ring = 0,
		ringColor,
		ringStyle = 'border',
		alt,
		initialTone = 'wash',
		initialSize,
		class: className = ''
	}: Props = $props();

	const hasPhoto = $derived(getManagerProfilePicture(name) !== null);
	const border = $derived(ring && ringStyle === 'border' ? `${ring}px` : undefined);
	const shadow = $derived(
		ring && ringStyle === 'shadow'
			? `0 0 0 ${ring}px ${ringColor ?? 'var(--color-card)'}`
			: undefined
	);
</script>

{#if hasPhoto}
	<span
		class="avatar {className}"
		style:--avatar-size="{size}px"
		style:--avatar-phone-size={phoneSize == null ? undefined : `${phoneSize}px`}
		style:border-width={border}
		style:border-color={border ? ringColor : undefined}
		style:box-shadow={shadow}
	>
		<ManagerProfilePicture managerName={name} {alt} />
	</span>
{:else}
	<span
		class="avatar initial {initialTone} {className}"
		role={alt === '' ? undefined : 'img'}
		aria-label={alt === '' ? undefined : (alt ?? name)}
		aria-hidden={alt === '' || undefined}
		style:--avatar-size="{size}px"
		style:--avatar-phone-size={phoneSize == null ? undefined : `${phoneSize}px`}
		style:border-width={border}
		style:border-color={border ? ringColor : undefined}
		style:box-shadow={shadow}
		style:font-size="{initialSize ?? size * 0.4}px">{avatarInitial(name)}</span
	>
{/if}

<style>
	/* Inline-block that clips, so it sits on a line the way the boards' <img>
	   does: its baseline is under the ring, not under the photo inside it
	   (build spec F2). The initial is centered by line height. */
	.avatar {
		width: var(--avatar-size);
		height: var(--avatar-size);
		display: inline-block;
		flex: none;
		vertical-align: baseline;
		box-sizing: content-box;
		border: 0 solid var(--color-card);
		border-radius: 50%;
		overflow: hidden;
	}

	/* ManagerProfilePicture brings its own fixed size; the avatar's wins. */
	.avatar > :global(*) {
		width: 100%;
		height: 100%;
	}

	.initial {
		line-height: var(--avatar-size);
		text-align: center;
		background: var(--color-wash);
		color: var(--color-ink-2);
		font-weight: 700;
	}

	@media (max-width: 639.98px) {
		.avatar {
			width: var(--avatar-phone-size, var(--avatar-size));
			height: var(--avatar-phone-size, var(--avatar-size));
		}

		.initial {
			line-height: var(--avatar-phone-size, var(--avatar-size));
		}
	}

	.initial.field {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}
</style>
