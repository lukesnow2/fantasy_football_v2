interface ClickOutsideOptions {
	/** Called on a pointer event outside the node and outside every ignored element. */
	onOutside: () => void;
	/** Usually the trigger that opened the popover — clicking it should toggle, not double-fire. */
	ignore?: (HTMLElement | null | undefined)[];
}

/**
 * Dismiss-on-outside-click for popovers and menus.
 *
 * Listens on `pointerdown` in the capture phase rather than `click`: a `click`
 * listener fires after the target has already reacted, which means a click on
 * another message's action button would open that popover and then immediately
 * be closed by the previous one's handler.
 */
export function clickOutside(node: HTMLElement, options: ClickOutsideOptions) {
	let current = options;

	function onPointerDown(event: PointerEvent) {
		const target = event.target as Node | null;
		if (!target) return;
		if (node.contains(target)) return;
		if (current.ignore?.some((el) => el?.contains(target))) return;
		current.onOutside();
	}

	document.addEventListener('pointerdown', onPointerDown, true);

	return {
		update(next: ClickOutsideOptions) {
			current = next;
		},
		destroy() {
			document.removeEventListener('pointerdown', onPointerDown, true);
		}
	};
}
