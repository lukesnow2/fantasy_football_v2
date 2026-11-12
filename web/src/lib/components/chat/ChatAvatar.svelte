<script lang="ts">
	/**
	 * A chat author's photo: the WP1 Avatar circle at the board's size (WP12
	 * S12.2), in place of the old rounded square with gradient initials. A
	 * warehouse photo URL wins when there is one (edw.dim_manager carries none
	 * today), then Avatar's checked-in photo, then its initial circle.
	 */
	import Avatar from '../ui/Avatar.svelte';

	interface Props {
		managerName: string | null;
		displayName: string;
		/** From edw.dim_manager. Currently NULL for every manager, hence the fallbacks. */
		profileImageUrl?: string | null;
		/** The size in px: 40 in the feed, 32 in a thread and on a phone, 26 in the mention list. */
		size?: number;
		/** The size below 640px, when the phone board draws another. */
		phoneSize?: number;
		class?: string;
	}

	let {
		managerName,
		displayName,
		profileImageUrl = null,
		size = 40,
		phoneSize,
		class: className = ''
	}: Props = $props();

	let broken = $state(false);
</script>

{#if profileImageUrl && !broken}
	<img
		src={profileImageUrl}
		alt=""
		class="photo {className}"
		style:--avatar-size="{size}px"
		style:--avatar-phone-size={phoneSize == null ? undefined : `${phoneSize}px`}
		loading="lazy"
		onerror={() => (broken = true)}
	/>
{:else}
	<Avatar name={managerName ?? displayName} {size} {phoneSize} alt="" class={className} />
{/if}

<style>
	.photo {
		flex: none;
		width: var(--avatar-size);
		height: var(--avatar-size);
		border-radius: 50%;
		object-fit: cover;
	}

	@media (max-width: 639.98px) {
		.photo {
			width: var(--avatar-phone-size, var(--avatar-size));
			height: var(--avatar-phone-size, var(--avatar-size));
		}
	}
</style>
