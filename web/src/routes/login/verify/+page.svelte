<script lang="ts">
	import { AlertCircle } from 'lucide-svelte';
	import type { PageData } from './$types';

	export let data: PageData;

	// Specific wording is safe here. Reaching this page at all requires holding a
	// token, so there is no roster to protect — and a manager who is told "this
	// link was already used" stops retrying the same link.
	const MESSAGES: Record<string, { title: string; detail: string }> = {
		missing: {
			title: 'No sign-in link found',
			detail: 'That URL is missing its token. Try the link in your email again, or request a new one.'
		},
		not_found: {
			title: "This link isn't valid",
			detail: 'It may have been mistyped or truncated by your email client. Request a fresh one below.'
		},
		expired: {
			title: 'This link has expired',
			detail: 'Sign-in links last 15 minutes. Request a new one and it should arrive within a moment.'
		},
		consumed: {
			title: 'This link was already used',
			detail:
				'Each link works exactly once. If you are not already signed in — some email apps open links automatically — request a new one.'
		},
		not_allowed: {
			title: 'This address is no longer on the roster',
			detail: 'Ask the commissioner to reactivate your account, then try again.'
		}
	};

	$: message = MESSAGES[data.failed ?? 'not_found'] ?? MESSAGES.not_found;
</script>

<svelte:head>
	<title>Sign-in link problem | The League</title>
</svelte:head>

<div class="flex min-h-[70vh] items-center justify-center px-4">
	<div class="w-full max-w-md text-center">
		<AlertCircle class="mx-auto mb-4 h-12 w-12 text-amber-400" />
		<h1 class="text-2xl font-bold text-white">{message.title}</h1>
		<p class="mt-3 text-slate-400">{message.detail}</p>

		<a
			href="/login"
			class="mt-8 inline-block rounded-lg bg-amber-500 px-5 py-2.5 font-semibold text-slate-900 transition-colors hover:bg-amber-400"
		>
			Send me a new link
		</a>
	</div>
</div>
