<script lang="ts">
	import { Lock } from 'lucide-svelte';

	interface Props {
		/** Signed in but not on the roster — a different problem with a different fix. */
		reason?: 'signed-out' | 'not-a-member';
		redirectTo?: string;
	}

	let { reason = 'signed-out', redirectTo = '/this-season' }: Props = $props();
</script>

<!-- Rendered *instead of* the chat, and the store makes no requests in this
     state. The previous panel fetched regardless, swallowed the 401 behind
     `if (response.ok)`, and left a silently empty box with no explanation. -->
<div class="flex flex-1 flex-col items-center justify-center gap-3 px-6 py-12 text-center">
	<Lock class="h-8 w-8 text-slate-600" />

	{#if reason === 'not-a-member'}
		<p class="text-sm font-medium text-slate-300">Your account isn't on the league roster yet.</p>
		<p class="max-w-xs text-xs text-slate-500">
			Ask the commissioner to add you, then sign in again.
		</p>
	{:else}
		<p class="text-sm font-medium text-slate-300">League chat is for managers.</p>
		<p class="max-w-xs text-xs text-slate-500">
			Sign in with your league email and you'll land right back here.
		</p>
		<a
			href="/login?redirect={encodeURIComponent(redirectTo)}"
			class="mt-1 rounded-lg bg-amber-500 px-4 py-2 text-sm font-medium text-black transition-colors hover:bg-amber-400"
		>
			Sign in
		</a>
	{/if}
</div>
