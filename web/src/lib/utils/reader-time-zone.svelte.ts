import { onMount } from 'svelte';
import { TIME_ZONE_COOKIE, browserTimeZone } from './time-zone';

/**
 * The reader's time zone on a page whose dates render on the server.
 *
 * It starts as the server's pick (the saved `tz` cookie, then Vercel's guess,
 * then Mountain time), so hydration matches the server's HTML, then switches
 * to the browser's own zone once the page loads. When the two differ, the
 * browser's zone is saved in the cookie, so the server gets it right from
 * then on. Call it during component setup:
 *
 *   const zone = readerTimeZone(data.timeZone);
 *   formatShortDate(value, zone.current)
 */
export function readerTimeZone(serverZone: string): { readonly current: string } {
	let zone = $state(serverZone);

	onMount(() => {
		const own = browserTimeZone();
		if (!own) return;
		zone = own;
		if (own !== serverZone) {
			document.cookie = `${TIME_ZONE_COOKIE}=${encodeURIComponent(own)}; path=/; max-age=31536000; samesite=lax`;
		}
	});

	return {
		get current() {
			return zone;
		}
	};
}
