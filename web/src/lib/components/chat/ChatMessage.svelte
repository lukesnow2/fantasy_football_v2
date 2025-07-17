<script lang="ts">
	import { createEventDispatcher, onMount } from 'svelte';
	import { MessageSquare, MoreHorizontal, Edit3, Trash2, Reply, Plus } from 'lucide-svelte';
	import ChatReactions from './ChatReactions.svelte';
	import EmojiPicker from './EmojiPicker.svelte';
	import type { ChatMessage } from '$lib/types/chat';
	
	export let message: ChatMessage & {
		authorName: string;
		authorDisplayName: string;
		authorProfileImage: string;
		reactions?: any[];
		replyCount?: number;
	};
	export let currentUserKey: number;
	export let showActions = true;
	export let showReactions = true;
	export let showThreads = true;
	
	const dispatch = createEventDispatcher();
	
	let showMessageActions = false;
	let showEmojiPicker = false;
	let isEditing = false;
	let editContent = message.content;
	let isBrowser = false;
	
	// Check if current user is the author
	$: isAuthor = message.authorKey === currentUserKey;
	
	// Set browser flag when component mounts
	onMount(() => {
		isBrowser = true;
	});
	
	// Format timestamp
	function formatTime(timestamp: string) {
		const date = new Date(timestamp);
		const now = new Date();
		const diffInHours = (now.getTime() - date.getTime()) / (1000 * 60 * 60);
		
		if (diffInHours < 24) {
			return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
		} else if (diffInHours < 168) { // 7 days
			return date.toLocaleDateString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' });
		} else {
			return date.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
		}
	}
	
	function handleEdit() {
		isEditing = true;
		showMessageActions = false;
	}
	
	function handleSaveEdit() {
		if (editContent.trim() && editContent !== message.content) {
			dispatch('edit', {
				messageId: message.messageId,
				content: editContent.trim()
			});
		}
		isEditing = false;
	}
	
	function handleCancelEdit() {
		editContent = message.content;
		isEditing = false;
	}
	
	function handleDelete() {
		dispatch('delete', {
			messageId: message.messageId
		});
		showMessageActions = false;
	}
	
	function handleReply() {
		dispatch('reply', {
			messageId: message.messageId,
			messageKey: message.messageKey
		});
		showMessageActions = false;
	}
	
	function handleReaction(event: CustomEvent) {
		dispatch('reaction', event.detail);
	}
	
	function handleEmojiSelect(event: CustomEvent) {
		const { emoji, emojiType } = event.detail;
		
		dispatch('reaction', {
			messageId: message.messageId,
			emoji,
			emojiType,
			action: 'add'
		});
		
		showEmojiPicker = false;
	}
	
	function handleAddReactionClick(event: MouseEvent) {
		event.stopPropagation();
		event.preventDefault();
		showEmojiPicker = !showEmojiPicker;
	}
	
	function handleMoreActions() {
		showMessageActions = !showMessageActions;
	}
	
	function handleThreadClick() {
		dispatch('thread', {
			messageId: message.messageId,
			messageKey: message.messageKey
		});
	}
	
	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			handleSaveEdit();
		} else if (event.key === 'Escape') {
			handleCancelEdit();
		}
	}
</script>

<div class="message-container group relative p-4 hover:bg-slate-700/10 rounded-lg transition-colors">
	<!-- Message Header -->
	<div class="flex items-start gap-3">
		<!-- Avatar -->
		<div class="flex-shrink-0">
			{#if message.authorProfileImage}
				<img 
					src={message.authorProfileImage} 
					alt={message.authorDisplayName || message.authorName}
					class="w-8 h-8 rounded-full object-cover"
				/>
			{:else}
				<div class="w-8 h-8 rounded-full bg-amber-500 flex items-center justify-center text-black font-semibold text-xs">
					{(message.authorDisplayName || message.authorName).charAt(0).toUpperCase()}
				</div>
			{/if}
		</div>
		
		<!-- Message Content -->
		<div class="flex-1 min-w-0">
			<!-- Author and timestamp -->
			<div class="flex items-center gap-2 mb-1">
				<span class="font-medium text-white text-sm">
					{message.authorDisplayName || message.authorName}
				</span>
				<span class="text-slate-500 text-xs">
					{formatTime(message.createdAt)}
				</span>
				{#if message.editedAt}
					<span class="text-slate-500 text-xs italic">(edited)</span>
				{/if}
			</div>
			
			<!-- Message content -->
			<div class="text-white text-sm leading-relaxed mb-2">
				{#if isEditing}
					<div class="space-y-2">
						<textarea 
							bind:value={editContent}
							on:keydown={handleKeydown}
							class="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-400 focus:outline-none focus:border-amber-400 resize-none"
							rows="3"
							placeholder="Edit message..."
						/>
						<div class="flex gap-2">
							<button 
								on:click={handleSaveEdit}
								class="bg-amber-500 hover:bg-amber-600 text-black px-3 py-1 rounded text-xs font-medium"
							>
								Save
							</button>
							<button 
								on:click={handleCancelEdit}
								class="bg-slate-600 hover:bg-slate-500 text-white px-3 py-1 rounded text-xs font-medium"
							>
								Cancel
							</button>
						</div>
					</div>
				{:else}
					<div class="whitespace-pre-wrap break-words">
						{message.content}
					</div>
				{/if}
			</div>
			
			<!-- Reactions -->
			{#if showReactions}
				<ChatReactions 
					messageId={message.messageId}
					reactions={message.reactions || []}
					currentUserKey={currentUserKey}
					on:reaction={handleReaction}
				/>
			{/if}
			
			<!-- Thread indicator -->
			{#if showThreads && message.replyCount && message.replyCount > 0}
				<button 
					on:click={handleThreadClick}
					class="flex items-center gap-1 text-amber-400 hover:text-amber-300 text-xs mt-2 transition-colors"
				>
					<MessageSquare class="w-3 h-3" />
					<span>{message.replyCount} {message.replyCount === 1 ? 'reply' : 'replies'}</span>
				</button>
			{/if}
		</div>
		
		<!-- Message Actions -->
		{#if showActions && !isEditing}
			<div class="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity bg-slate-800 rounded-lg p-1 border border-slate-600 relative">
				<button 
					on:click={handleReply}
					class="p-1.5 rounded hover:bg-slate-700 text-slate-400 hover:text-white transition-colors"
					title="Reply in thread"
				>
					<Reply class="w-4 h-4" />
				</button>
				
				<!-- Add reaction button -->
				<div class="relative">
					<button 
						on:click={handleAddReactionClick}
						class="p-1.5 rounded hover:bg-slate-700 text-slate-400 hover:text-white transition-colors"
						title="Add reaction"
					>
						<Plus class="w-4 h-4" />
					</button>
					
					<!-- Emoji Picker positioned directly under the plus button -->
					{#if showEmojiPicker && isBrowser}
						<div class="absolute top-full right-0 mt-1 z-50">
							<EmojiPicker 
								on:select={handleEmojiSelect}
								on:close={() => showEmojiPicker = false}
							/>
						</div>
					{/if}
				</div>
				
				<!-- More actions -->
				<button 
					on:click={handleMoreActions}
					class="p-1.5 rounded hover:bg-slate-700 text-slate-400 hover:text-white transition-colors"
					title="More actions"
				>
					<MoreHorizontal class="w-4 h-4" />
				</button>
			</div>
		{/if}
	</div>
	
	<!-- Actions dropdown -->
	{#if showMessageActions}
		<div class="absolute right-0 top-12 bg-slate-800 border border-slate-700 rounded-lg shadow-lg py-2 z-10">
			<button 
				on:click={handleReply}
				class="w-full px-4 py-2 text-left text-sm text-slate-300 hover:bg-slate-700 flex items-center gap-2"
			>
				<Reply class="w-4 h-4" />
				Reply in thread
			</button>
			
			{#if isAuthor}
				<button 
					on:click={handleEdit}
					class="w-full px-4 py-2 text-left text-sm text-slate-300 hover:bg-slate-700 flex items-center gap-2"
				>
					<Edit3 class="w-4 h-4" />
					Edit message
				</button>
				
				<button 
					on:click={handleDelete}
					class="w-full px-4 py-2 text-left text-sm text-red-400 hover:bg-slate-700 flex items-center gap-2"
				>
					<Trash2 class="w-4 h-4" />
					Delete message
				</button>
			{/if}
		</div>
	{/if}
</div>

<style>
	.message-container:last-child {
		margin-bottom: 0;
	}
</style> 