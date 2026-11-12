<script lang="ts">
	import { getInitials, getManagerProfilePicture } from '$lib/utils/managerUtils';

	export let managerName: string;
	export let size: 'small' | 'medium' | 'large' = 'medium';
	export let className: string = '';
	/** The photo's alt text. Pass "" where the name is already beside it. */
	export let alt: string | undefined = undefined;

	$: profilePicUrl = getManagerProfilePicture(managerName);
	$: initials = getInitials(managerName);

	// Size classes
	$: sizeClasses = {
		small: 'w-12 h-12 text-sm',
		medium: 'w-20 h-20 text-2xl',
		large: 'w-32 h-32 text-4xl'
	}[size];
</script>

<div class="overflow-hidden rounded-full {sizeClasses} {className}">
	{#if profilePicUrl}
		<!-- Actual profile picture -->
		<img
			src={profilePicUrl}
			alt={alt ?? `${managerName} profile picture`}
			class="h-full w-full object-cover"
			on:error={() => {
				// If image fails to load, fall back to gradient
				profilePicUrl = null;
			}}
		/>
	{:else}
		<!-- Gradient fallback with initials -->
		<div
			class="flex h-full w-full items-center justify-center bg-gradient-to-br from-blue-500 via-purple-600 to-pink-500"
		>
			<span class="font-bold text-white">{initials}</span>
		</div>
	{/if}
</div>
