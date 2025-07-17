<script lang="ts">
	import { getInitials, getManagerProfilePicture } from '$lib/utils/managerUtils';

	export let managerName: string;
	export let size: 'small' | 'medium' | 'large' = 'medium';
	export let className: string = '';

	$: profilePicUrl = getManagerProfilePicture(managerName);
	$: initials = getInitials(managerName);

	// Debug logging
	$: {
		console.log('ManagerProfilePicture Debug:', {
			managerName,
			profilePicUrl,
			normalizedName: managerName.toLowerCase().replace(/\s+/g, '-')
		});
	}

	// Size classes
	$: sizeClasses = {
		small: 'w-12 h-12 text-sm',
		medium: 'w-20 h-20 text-2xl',
		large: 'w-32 h-32 text-4xl'
	}[size];
</script>

<div class="rounded-full overflow-hidden {sizeClasses} {className}">
	{#if profilePicUrl}
		<!-- Actual profile picture -->
		<img 
			src={profilePicUrl} 
			alt="{managerName} profile picture"
			class="w-full h-full object-cover"
			on:error={() => {
				// If image fails to load, fall back to gradient
				profilePicUrl = null;
			}}
		/>
	{:else}
		<!-- Gradient fallback with initials -->
		<div class="w-full h-full bg-gradient-to-br from-blue-500 via-purple-600 to-pink-500 flex items-center justify-center">
			<span class="text-white font-bold">{initials}</span>
		</div>
	{/if}
</div> 