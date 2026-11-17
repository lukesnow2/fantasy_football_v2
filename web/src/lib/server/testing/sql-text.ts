/**
 * Reading drizzle `sql` objects in tests, for route tests whose database is a
 * stub that answers by what each query reads from.
 */

/** The SQL text of a drizzle `sql` object, parameters as `?`. */
export function textOf(query: unknown): string {
	const chunks = (query as { queryChunks?: unknown[] })?.queryChunks;
	if (!chunks) return '?';
	return chunks
		.map((chunk) => {
			if (chunk && typeof chunk === 'object' && 'value' in chunk) {
				return ([] as string[]).concat((chunk as { value: string | string[] }).value).join('');
			}
			if (chunk && typeof chunk === 'object' && 'queryChunks' in chunk) return textOf(chunk);
			return '?';
		})
		.join('');
}

/** The bound parameters of a drizzle `sql` object, in order. */
export function paramsOf(query: unknown): unknown[] {
	const chunks = (query as { queryChunks?: unknown[] })?.queryChunks ?? [];
	return chunks.flatMap((chunk) => {
		if (chunk && typeof chunk === 'object' && 'queryChunks' in chunk) return paramsOf(chunk);
		if (chunk && typeof chunk === 'object' && 'value' in chunk) return [];
		return [chunk];
	});
}
