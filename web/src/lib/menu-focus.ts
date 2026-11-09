/**
 * Keyboard focus for the shell's menus, the nav dropdowns and the account menu
 * (WP2, S2.2 and S2.3), so both move the same way. The index rule itself is
 * nextMenuIndex in nav.ts, which has the tests.
 */
import { nextMenuIndex } from './nav';

/** The items in a menu, in order. */
export function menuItems(menu: HTMLElement | null | undefined): HTMLElement[] {
	return [...(menu?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? [])];
}

/** Focus the menu's first item, as a keyboard open does. */
export function focusFirstMenuItem(menu: HTMLElement | null | undefined): void {
	menuItems(menu)[0]?.focus();
}

/** Move focus for ↓ or ↑ inside the menu, wrapping at both ends. Other keys pass. */
export function moveMenuFocus(menu: HTMLElement | null | undefined, event: KeyboardEvent): void {
	const items = menuItems(menu);
	const next = nextMenuIndex(
		items.indexOf(document.activeElement as HTMLElement),
		items.length,
		event.key
	);
	if (next === null) return;
	event.preventDefault();
	items[next].focus();
}
