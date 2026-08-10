// See https://svelte.dev/docs/kit/types#app.d.ts
// for information about these interfaces
declare global {
	namespace App {
		/** The caller's league membership, resolved alongside their session. */
		interface SessionMember {
			managerKey: number;
			role: 'member' | 'commissioner';
			active: boolean;
		}

		interface Locals {
			user: import('$lib/server/auth').SessionValidationResult['user'];
			session: import('$lib/server/auth').SessionValidationResult['session'];
			/**
			 * Null when the caller is signed out, or signed in but no longer on the
			 * league roster. Both cases must be refused by write paths — see
			 * `requireManagerKey` in lib/server/auth-manager.ts.
			 */
			member: SessionMember | null;
		}
	} // interface Error {}
	// interface Locals {}
} // interface PageData {}
// interface PageState {}

// interface Platform {}
export {};
