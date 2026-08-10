/**
 * Clause labels by depth, matching the constitution's existing convention:
 * depth 0 uses "I. II. III.", depth 1 uses "a. b. c.", depth 2 uses "i. ii. iii."
 *
 * Labels are stored rather than derived at render time so that a clause keeps a
 * stable label across a version clone, but they are regenerated for a whole
 * sibling group whenever one is inserted or deleted — otherwise deleting clause
 * II leaves the document numbered I, III, IV.
 */

const ROMAN: Array<[number, string]> = [
	[1000, 'M'],
	[900, 'CM'],
	[500, 'D'],
	[400, 'CD'],
	[100, 'C'],
	[90, 'XC'],
	[50, 'L'],
	[40, 'XL'],
	[10, 'X'],
	[9, 'IX'],
	[5, 'V'],
	[4, 'IV'],
	[1, 'I']
];

/** 1 -> "I", 4 -> "IV", 9 -> "IX" */
export function toRoman(n: number): string {
	if (n < 1) return '';
	let remaining = n;
	let out = '';
	for (const [value, numeral] of ROMAN) {
		while (remaining >= value) {
			out += numeral;
			remaining -= value;
		}
	}
	return out;
}

/** 1 -> "a", 26 -> "z", 27 -> "aa" */
export function toAlpha(n: number): string {
	if (n < 1) return '';
	let remaining = n;
	let out = '';
	while (remaining > 0) {
		const rem = (remaining - 1) % 26;
		out = String.fromCharCode(97 + rem) + out;
		remaining = Math.floor((remaining - 1) / 26);
	}
	return out;
}

export function toRomanLower(n: number): string {
	return toRoman(n).toLowerCase();
}

/** Ordinal is 1-based position within the sibling group. */
export function labelFor(depth: number, ordinal: number): string {
	if (depth <= 0) return toRoman(ordinal);
	if (depth === 1) return toAlpha(ordinal);
	return toRomanLower(ordinal);
}
