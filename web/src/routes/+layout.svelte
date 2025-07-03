<script lang="ts">
	import '../app.css';
	import { page } from '$app/stores';
	import { Trophy, BarChart3, Calendar, Users, Crown, BookOpen, Target, Settings } from 'lucide-svelte';

	const navigation = [
		{ name: 'This Season', href: '/this-season', icon: Calendar },
		{ name: 'Historical Deep Dive', href: '/historical', icon: BarChart3 },
		{ name: 'Hall of Fame', href: '/hall-of-fame', icon: Crown },
		{ name: 'Manager Profiles', href: '/managers', icon: Users },
		{ name: 'Trade Center', href: '/trades', icon: Target },
		{ name: 'Draft Central', href: '/draft', icon: Trophy },
		{ name: 'Constitution', href: '/constitution', icon: BookOpen },
	];
</script>

<div class="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900">
	<!-- Header -->
	<header class="border-b border-slate-700/50 bg-slate-900/80 backdrop-blur-md">
		<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
			<div class="flex h-16 items-center justify-between">
				<div class="flex items-center">
					<Trophy class="h-8 w-8 text-amber-400" />
					<h1 class="ml-2 text-xl font-bold text-white">The League</h1>
				</div>
				
				<!-- Navigation -->
				<nav class="hidden md:flex space-x-8">
					{#each navigation as item}
						<a 
							href={item.href}
							class="flex items-center px-3 py-2 text-sm font-medium transition-colors rounded-md
								{$page.url.pathname === item.href 
									? 'text-amber-400 bg-amber-400/10' 
									: 'text-slate-300 hover:text-white hover:bg-slate-800'}"
						>
							<svelte:component this={item.icon} class="w-4 h-4 mr-2" />
							{item.name}
						</a>
					{/each}
				</nav>

				<!-- Mobile menu button -->
				<div class="md:hidden">
					<button type="button" class="text-slate-300 hover:text-white">
						<Settings class="h-6 w-6" />
					</button>
				</div>
			</div>
		</div>
	</header>

	<!-- Main content -->
	<main class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
		<slot />
	</main>

	<!-- Footer -->
	<footer class="border-t border-slate-700/50 bg-slate-900/50 mt-20">
		<div class="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
			<div class="text-center text-slate-400">
				<p>&copy; 2024 The League. Where legends are made and dreams are crushed.</p>
			</div>
		</div>
	</footer>
</div>
