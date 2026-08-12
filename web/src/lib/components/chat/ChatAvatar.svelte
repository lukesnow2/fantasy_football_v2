<script lang="ts">
	import { getInitials, getManagerProfilePicture } from '$lib/utils/managerUtils';

	interface Props {
		managerName: string | null;
		displayName: string;
		/** From edw.dim_manager. Currently NULL for every manager, hence the fallbacks. */
		profileImageUrl?: string | null;
		size?: 'sm' | 'md';
		class?: string;
	}

	let { managerName, displayName, profileImageUrl = null, size = 'md', class: className = '' }: Props = $props();

	// Three steps down: the warehouse column, then the checked-in photo map in
	// /static/manager-profiles, then initials. The middle one carries all ten
	// active managers today; the first carries none of them.
	const localPhoto = $derived(getManagerProfilePicture(managerName ?? displayName));
	let broken = $state(false);
	const src = $derived(broken ? null : (profileImageUrl || localPhoto));

	const sizeClass = $derived(size === 'sm' ? 'h-6 w-6 text-[10px]' : 'h-9 w-9 text-xs');
</script>

<div class="shrink-0 overflow-hidden rounded-lg {sizeClass} {className}">
	{#if src}
		<img
			{src}
			alt=""
			class="h-full w-full object-cover"
			loading="lazy"
			onerror={() => (broken = true)}
		/>
	{:else}
		<div
			class="flex h-full w-full items-center justify-center bg-gradient-to-br from-blue-500 via-purple-600 to-pink-500 font-bold text-white"
			aria-hidden="true"
		>
			{getInitials(displayName)}
		</div>
	{/if}
</div>
