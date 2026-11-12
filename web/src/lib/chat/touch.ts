import { MediaQuery } from 'svelte/reactivity';

/**
 * Whether this is a touch screen with no hover (WP12 S12.10): the toolbar
 * opens on a tap and its menu flips inside the feed. One query for the whole
 * chat, not one per row.
 */
export const touchScreen = new MediaQuery('hover: none');
