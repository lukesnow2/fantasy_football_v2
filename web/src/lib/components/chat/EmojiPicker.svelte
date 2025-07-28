<script lang="ts">
	import { createEventDispatcher, onMount, onDestroy } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import { Search, X } from 'lucide-svelte';
	
	export let showCustomEmojis = true;
	
	const dispatch = createEventDispatcher();
	
	// Get user from page data
	$: user = $page.data.user;
	
	let searchTerm = '';
	let activeTab = 'recent';
	let customEmojis: any[] = [];
	let recentEmojis: string[] = [];
	let filteredEmojis: any[] = [];
	
	// Common Unicode emojis organized by category
	const unicodeEmojis = {
		people: ['😀', '😃', '😄', '😁', '😆', '😅', '😂', '🤣', '😊', '😇', '🙂', '😉', '😌', '😍', '🥰', '😘', '😗', '😙', '😚', '😋', '😛', '😜', '🤪', '😝', '🤑', '🤗', '🤭', '🤫', '🤔', '🤐', '🤨', '😐', '😑', '😶', '😏', '😒', '🙄', '😬', '🤥', '😔', '😪', '🤤', '😴', '😷', '🤒', '🤕', '🤢', '🤮', '🤧', '🥵', '🥶', '🥴', '😵', '🤯', '🤠', '🥳', '😎', '🤓', '🧐'],
		nature: ['🐶', '🐱', '🐭', '🐹', '🐰', '🦊', '🐻', '🐼', '🐨', '🐯', '🦁', '🐮', '🐷', '🐽', '🐸', '🐵', '🙈', '🙉', '🙊', '🐒', '🐔', '🐧', '🐦', '🐤', '🐣', '🐥', '🦆', '🦅', '🦉', '🦇', '🐺', '🐗', '🐴', '🦄', '🐝', '🐛', '🦋', '🐌', '🐞', '🐜', '🦟', '🦗', '🕷', '🕸', '🦂', '🐢', '🐍', '🦎', '🦖', '🦕', '🐙', '🦑', '🦐', '🦞', '🦀', '🐡', '🐠', '🐟', '🐬', '🐳', '🐋', '🦈', '🐊', '🐅', '🐆', '🦓', '🦍', '🐘', '🦏', '🐪', '🐫', '🦒', '🐃', '🐂', '🐄', '🐎', '🐖', '🐏', '🐑', '🐐', '🦌', '🐕', '🐩', '🐈', '🐓', '🦃', '🕊', '🐇', '🐁', '🐀', '🐿', '🦔'],
		food: ['🍏', '🍎', '🍐', '🍊', '🍋', '🍌', '🍉', '🍇', '🍓', '🍈', '🍒', '🍑', '🥭', '🍍', '🥥', '🥝', '🍅', '🍆', '🥑', '🥦', '🥕', '🌶', '🌽', '🥒', '🥬', '🥕', '🍠', '🥔', '🍄', '🥜', '🌰', '🍞', '🥐', '🥖', '🥨', '🥯', '🥞', '🧀', '🍖', '🍗', '🥩', '🥓', '🍔', '🍟', '🍕', '🌭', '🥪', '🌮', '🌯', '🥙', '🍳', '🥘', '🍲', '🥣', '🥗', '🍿', '🧈', '🧂', '🥫', '🍱', '🍘', '🍙', '🍚', '🍛', '🍜', '🍝', '🍠', '🍢', '🍣', '🍤', '🍥', '🥮', '🍡', '🥟', '🥠', '🥡', '🍦', '🍧', '🍨', '🍩', '🍪', '🎂', '🍰', '🧁', '🥧', '🍫', '🍬', '🍭', '🍮', '🍯'],
		activities: ['⚽', '🏀', '🏈', '⚾', '🎾', '🏐', '🏉', '🎱', '🏓', '🏸', '🏒', '🥊', '⛳', '🏹', '🎣', '🥊', '🥋', '🎽', '⛷', '🏂', '🏄', '🚣', '🏊', '⛹', '🏋', '🚴', '🚵', '🏇', '🕴', '🏌', '🏄', '🚣', '🏊', '🚴', '🚵', '🧗', '🤺', '🏇', '⛷', '🏂', '🏌', '🏄', '🚣', '🏊', '🚴', '🚵', '🧗', '🤺', '🏇', '⛷', '🏂', '🏌', '🏄', '🚣', '🏊', '🚴', '🚵', '🧗', '🤺', '🏇', '⛷', '🏂', '🏌', '🏄', '🚣', '🏊', '🚴', '🚵', '🧗', '🤺', '🏇', '⛷', '🏂', '🏌', '🏄', '🚣', '🏊', '🚴', '🚵', '🧗', '🤺', '🏇', '⛷', '🏂'],
		objects: ['🔧', '🔨', '⚒', '🛠', '⛏', '🔩', '⚙', '🔗', '⛓', '🔫', '💣', '🔪', '🗡', '⚔', '🛡', '🚬', '⚰', '⚱', '🏺', '🔮', '📿', '🧿', '💈', '⚗', '🔭', '🔬', '🕳', '💊', '💉', '🧬', '🦠', '🧫', '🧪', '🌡', '🧹', '🧺', '🧻', '🚽', '🚿', '🛁', '🧴', '🧷', '🧸', '🧵', '🧶', '🧮', '🎁', '🎀', '🎊', '🎉', '🎈', '🎂', '🍰', '🧁', '🥧', '🍫', '🍬', '🍭', '🍮', '🍯']
	};
	
	// Load recent emojis from localStorage
	onMount(() => {
		// Check if we're in the browser before accessing localStorage
		if (typeof window !== 'undefined' && window.localStorage) {
			const saved = localStorage.getItem('chat-recent-emojis');
			if (saved) {
				try {
					recentEmojis = JSON.parse(saved);
				} catch (error) {
					console.warn('Failed to parse recent emojis from localStorage:', error);
					recentEmojis = [];
				}
			}
		}
		
		if (showCustomEmojis) {
			loadCustomEmojis();
		}
		
		updateFilteredEmojis();
		
		// Add click outside listener
		if (typeof window !== 'undefined') {
			document.addEventListener('click', handleClickOutside);
		}
	});
	
	onDestroy(() => {
		// Remove click outside listener
		if (typeof window !== 'undefined') {
			document.removeEventListener('click', handleClickOutside);
		}
	});
	
	// Load custom emojis from API
	async function loadCustomEmojis() {
		try {
			const response = await fetch('/api/chat/custom-emojis');
			if (response.ok) {
				const data = await response.json();
				customEmojis = data.emojis || [];
				updateFilteredEmojis();
			} else {
				console.warn('Custom emojis API returned:', response.status);
				customEmojis = []; // Set to empty array if API fails
			}
		} catch (error) {
			console.warn('Failed to load custom emojis, using Unicode only:', error);
			customEmojis = []; // Set to empty array if API fails
		}
	}
	
	// Update filtered emojis based on search and active tab
	function updateFilteredEmojis() {
		let emojis: any[] = [];
		
		if (activeTab === 'recent') {
			emojis = recentEmojis.map(emoji => ({ emoji, type: 'unicode' }));
		} else if (activeTab === 'custom' && showCustomEmojis) {
			emojis = customEmojis.map(emoji => ({ 
				emoji: emoji.imageUrl, 
				type: 'custom', 
				name: emoji.name 
			}));
		} else if (unicodeEmojis[activeTab as keyof typeof unicodeEmojis]) {
			emojis = unicodeEmojis[activeTab as keyof typeof unicodeEmojis].map(emoji => ({ emoji, type: 'unicode' }));
		}
		
		// Filter by search term
		if (searchTerm) {
			const term = searchTerm.toLowerCase();
			emojis = emojis.filter(item => {
				if (item.type === 'custom' && item.name) {
					return item.name.toLowerCase().includes(term);
				}
				return true; // For unicode emojis, we'd need a more complex search
			});
		}
		
		filteredEmojis = emojis;
	}
	
	// Handle tab change
	function handleTabChange(tab: string) {
		activeTab = tab;
		updateFilteredEmojis();
	}
	
	// Handle emoji selection
	function handleEmojiSelect(emoji: any) {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		// Add to recent emojis
		if (emoji.type === 'unicode') {
			recentEmojis = [emoji.emoji, ...recentEmojis.filter(e => e !== emoji.emoji)].slice(0, 20);
			// Only save to localStorage if we're in the browser
			if (typeof window !== 'undefined' && window.localStorage) {
				localStorage.setItem('chat-recent-emojis', JSON.stringify(recentEmojis));
			}
		}
		
		// Dispatch selection event
		dispatch('select', {
			emoji: emoji.emoji,
			emojiType: emoji.type
		});
	}
	
	// Handle close
	function handleClose() {
		dispatch('close');
	}
	
	// Handle search input
	function handleSearch() {
		updateFilteredEmojis();
	}
	
	// Handle click outside
	function handleClickOutside(event: MouseEvent) {
		// Only handle clicks in browser environment
		if (typeof window !== 'undefined' && event.target) {
			if (!(event.target as HTMLElement).closest('.emoji-picker')) {
				handleClose();
			}
		}
	}
	
	// Tab configuration
	const tabs = [
		{ id: 'recent', label: '🕐', title: 'Recent' },
		{ id: 'people', label: '😊', title: 'People' },
		{ id: 'nature', label: '🐶', title: 'Nature' },
		{ id: 'food', label: '🍎', title: 'Food' },
		{ id: 'activities', label: '⚽', title: 'Activities' },
		{ id: 'objects', label: '🔧', title: 'Objects' }
	];
	
	// Add custom tab if enabled
	if (showCustomEmojis) {
		tabs.push({ id: 'custom', label: '⚡', title: 'Custom' });
	}
	
	// Update filtered emojis when search term changes
	$: if (typeof window !== 'undefined' && searchTerm !== undefined) {
		updateFilteredEmojis();
	}
</script>

{#if typeof window !== 'undefined'}
<div class="emoji-picker bg-slate-800 border border-slate-600 rounded-lg shadow-xl p-3 w-64 h-72 flex flex-col">
	<!-- Header -->
	<div class="flex items-center justify-between mb-3">
		<h3 class="text-sm font-medium text-white">Add Reaction</h3>
		<button 
			on:click={handleClose}
			class="text-slate-400 hover:text-white p-1 rounded hover:bg-slate-700 transition-colors"
		>
			<X class="w-4 h-4" />
		</button>
	</div>
	
	<!-- Search -->
	<div class="relative mb-3">
		<Search class="absolute left-2 top-1/2 transform -translate-y-1/2 w-3 h-3 text-slate-400" />
		<input
			bind:value={searchTerm}
			on:input={handleSearch}
			placeholder="Search emojis..."
			class="w-full pl-7 pr-3 py-1.5 bg-slate-700 border border-slate-600 rounded text-sm text-white placeholder-slate-400 focus:outline-none focus:border-amber-500"
		/>
	</div>
	
	<!-- Tabs -->
	<div class="flex gap-1 mb-3 border-b border-slate-600 pb-2">
		{#each tabs as tab}
			<button
				class="px-2 py-1 rounded text-xs transition-colors {activeTab === tab.id ? 'bg-amber-500 text-black font-medium' : 'text-slate-300 hover:text-white hover:bg-slate-700'}"
				class:active={activeTab === tab.id}
				on:click={() => handleTabChange(tab.id)}
				title={tab.title}
			>
				{tab.label}
			</button>
		{/each}
	</div>
	
	<!-- Emoji Grid -->
	<div class="flex-1 overflow-y-auto">
		<div class="grid grid-cols-7 gap-1">
			{#each filteredEmojis as emoji}
				<button
					class="emoji-button w-8 h-8 flex items-center justify-center rounded hover:bg-slate-700 transition-colors text-sm"
					on:click={() => handleEmojiSelect(emoji)}
					title={emoji.name || emoji.emoji}
				>
					{#if emoji.type === 'custom'}
						<img 
							src={emoji.emoji} 
							alt={emoji.name || 'custom emoji'} 
							class="w-5 h-5 object-contain"
						/>
					{:else}
						<span class="text-sm">{emoji.emoji}</span>
					{/if}
				</button>
			{/each}
		</div>
		
		{#if filteredEmojis.length === 0}
			<div class="text-center text-slate-400 py-4 text-xs">
				{searchTerm ? 'No emojis found' : 'No emojis in this category'}
			</div>
		{/if}
	</div>
</div>
{/if}

<style>
	.emoji-picker {
		box-shadow: 0 10px 25px rgba(0, 0, 0, 0.3);
	}
	
	.emoji-button:hover {
		background-color: rgb(51 65 85);
	}
</style> 