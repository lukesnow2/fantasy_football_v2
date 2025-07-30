<script lang="ts">
	import FirstTimeSetup from '$lib/components/webauthn/FirstTimeSetup.svelte';

	export let data: {
		userId: string;
		username: string;
		email: string | null;
		managerKey: string;
	};
</script>

<svelte:head>
	<title>Set Up Passkey - The League</title>
	<meta name="description" content="Set up your passkey for secure authentication" />
</svelte:head>

<div class="setup-passkey-page">
	<div class="container">
		<FirstTimeSetup 
			userId={data.userId}
			username={data.username}
			managerKey={data.managerKey}
		/>
	</div>
</div>

<style>
	.setup-passkey-page {
		min-height: 100vh;
		background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 2rem;
	}

	.container {
		width: 100%;
		max-width: 600px;
		margin: 0 auto;
	}

	@media (max-width: 768px) {
		.setup-passkey-page {
			padding: 1rem;
		}
	}
</style> 