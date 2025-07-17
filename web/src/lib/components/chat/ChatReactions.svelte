<script lang="ts">
	import { createEventDispatcher, onMount } from 'svelte';
	import { Plus, Smile } from 'lucide-svelte';
	
	let isBrowser = false;
	
	export let messageId: string;
	export let reactions: Array<{
		emoji: string;
		emojiType: string;
		count: number;
		users: Array<{
			authorKey: number;
			authorName: string;
			authorDisplayName: string;
			authorProfileImage: string;
		}>;
	}> = [];
	export let currentUserKey: number;
	
	const dispatch = createEventDispatcher();
	
	let hoveredReaction: any = null;
	let tooltipPosition = { x: 0, y: 0 };
	
	// Set browser flag when component mounts
	onMount(() => {
		isBrowser = true;
	});
	
	// Check if current user has reacted with a specific emoji
	function hasUserReacted(reaction: any): boolean {
		return reaction.users.some((user: any) => user.authorKey === currentUserKey);
	}
	
	// Handle clicking on a reaction
	function handleReactionClick(reaction: any) {
		const hasReacted = hasUserReacted(reaction);
		
		if (hasReacted) {
			// Remove reaction
			dispatch('reaction', {
				messageId,
				emoji: reaction.emoji,
				action: 'remove'
			});
		} else {
			// Add reaction
			dispatch('reaction', {
				messageId,
				emoji: reaction.emoji,
				emojiType: reaction.emojiType,
				action: 'add'
			});
		}
	}
	
	// Handle mouse enter for tooltip
	function handleMouseEnter(event: MouseEvent, reaction: any) {
		hoveredReaction = reaction;
		const rect = (event.target as HTMLElement).getBoundingClientRect();
		tooltipPosition = {
			x: rect.left + rect.width / 2,
			y: rect.top - 10
		};
	}
	
	// Handle mouse leave for tooltip
	function handleMouseLeave() {
		hoveredReaction = null;
	}
	
	// Format tooltip text
	function formatTooltip(reaction: any): string {
		if (reaction.count === 1) {
			const user = reaction.users[0];
			return `${user.authorDisplayName || user.authorName} reacted with ${reaction.emoji}`;
		} else if (reaction.count <= 3) {
			const names = reaction.users.map((user: any) => user.authorDisplayName || user.authorName);
			return `${names.join(', ')} reacted with ${reaction.emoji}`;
		} else {
			const firstTwo = reaction.users.slice(0, 2).map((user: any) => user.authorDisplayName || user.authorName);
			const remaining = reaction.count - 2;
			return `${firstTwo.join(', ')} and ${remaining} others reacted with ${reaction.emoji}`;
		}
	}
</script>

{#if reactions.length > 0}
	<div class="reactions-container mt-3 flex items-center">
		<div class="flex items-center gap-2 flex-wrap">
			<!-- Existing reactions -->
			{#each reactions as reaction}
				<button
					class="reaction-button flex items-center gap-2 px-3 py-2 rounded-full transition-all duration-200 text-sm hover:scale-105"
					class:user-reacted={hasUserReacted(reaction)}
					on:click={() => handleReactionClick(reaction)}
					on:mouseenter={(e) => handleMouseEnter(e, reaction)}
					on:mouseleave={handleMouseLeave}
				>
					{#if reaction.emojiType === 'custom'}
						<img 
							src={reaction.emoji} 
							alt="custom emoji" 
							class="w-5 h-5 object-contain"
						/>
					{:else}
						<span class="emoji text-lg">{reaction.emoji}</span>
					{/if}
					<span class="count font-semibold">{reaction.count}</span>
				</button>
			{/each}
		</div>
	</div>
{/if}

<!-- Tooltip -->
{#if hoveredReaction}
	<div 
		class="fixed z-30 bg-slate-900/95 backdrop-blur-sm border border-slate-700/50 rounded-lg px-3 py-2 text-sm text-white shadow-xl pointer-events-none"
		style="left: {tooltipPosition.x}px; top: {tooltipPosition.y}px; transform: translate(-50%, -100%);"
	>
		{formatTooltip(hoveredReaction)}
	</div>
{/if}

<style>
	.reaction-button {
		background: linear-gradient(135deg, rgba(51, 65, 85, 0.4), rgba(71, 85, 105, 0.6));
		border: 1px solid rgba(100, 116, 139, 0.3);
		color: #e2e8f0;
		box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
	}
	
	.reaction-button:hover {
		background: linear-gradient(135deg, rgba(71, 85, 105, 0.6), rgba(100, 116, 139, 0.8));
		border-color: rgba(148, 163, 184, 0.4);
		box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
		transform: scale(1.05) translateY(-1px);
	}
	
	.reaction-button.user-reacted {
		background: linear-gradient(135deg, rgba(245, 158, 11, 0.2), rgba(245, 158, 11, 0.3));
		border: 1px solid rgba(245, 158, 11, 0.6);
		color: #fbbf24;
		box-shadow: 
			0 4px 8px rgba(245, 158, 11, 0.2),
			inset 0 1px 0 rgba(255, 255, 255, 0.1);
	}
	
	.reaction-button.user-reacted:hover {
		background: linear-gradient(135deg, rgba(245, 158, 11, 0.3), rgba(245, 158, 11, 0.4));
		border-color: rgba(245, 158, 11, 0.8);
		box-shadow: 
			0 6px 12px rgba(245, 158, 11, 0.3),
			inset 0 1px 0 rgba(255, 255, 255, 0.2);
		transform: scale(1.05) translateY(-1px);
	}
	
	.emoji {
		line-height: 1;
		filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.1));
	}
	
	.count {
		font-weight: 600;
		text-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
	}
</style> 