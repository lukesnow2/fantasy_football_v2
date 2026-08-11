/**
 * Escape text before it goes into an HTML email body.
 *
 * Proposal titles and rationales are free text written by one manager and mailed
 * to the other nine inside a message that genuinely is from the league. Without
 * this, a rationale containing an anchor tag is a phishing link wearing the
 * league's own branding.
 *
 * The ampersand must be replaced FIRST. Move it later in the chain and every
 * entity the other rules produce gets double-escaped, so a manager called
 * O'Brien reads "O&amp;#39;Brien" in their mail client.
 *
 * Lives in its own module rather than beside the templates so that importing a
 * seven-line string helper does not drag in the transports, env.ts and the
 * console noise validateEnv() emits at load.
 */
export function escapeHtml(value: string): string {
	return value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');
}
