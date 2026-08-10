<script lang="ts">
	import { MailCheck } from 'lucide-svelte';
</script>

<svelte:head>
	<title>Check your email | The League</title>
</svelte:head>

<!--
	Deliberately static and unconditional.

	This page is reached identically whether a link was actually sent, the address
	is not on the roster, or the sender hit a rate limit. It must not echo the
	submitted address back or vary its wording, or it becomes the roster-
	enumeration oracle the login action exists to close.
-->
<div class="flex min-h-[70vh] items-center justify-center px-4">
	<div class="w-full max-w-md text-center">
		<MailCheck class="mx-auto mb-4 h-12 w-12 text-amber-400" />
		<h1 class="text-2xl font-bold text-white">Check your email</h1>
		<p class="mt-3 text-slate-400">
			If that address is on the league roster, a sign-in link is on its way. It expires in 15
			minutes and can only be used once.
		</p>
		<p class="mt-6 text-sm text-slate-500">
			Didn't get it? Check your spam folder, then
			<a href="/login" class="text-amber-400 underline hover:text-amber-300">try again</a>.
		</p>
	</div>
</div>
