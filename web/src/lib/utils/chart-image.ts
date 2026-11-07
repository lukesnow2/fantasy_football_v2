/**
 * Save an SVG chart as a PNG, with its title, legend and a source line, for
 * sharing outside the site. Images inside the SVG (manager photos) are inlined
 * first: an SVG drawn to a canvas can't load external files.
 */

export interface LegendItem {
	kind: 'line' | 'rect' | 'dot' | 'ring';
	color: string;
	label: string;
	opacity?: number;
}

export interface ChartImageOptions {
	title: string;
	subtitle?: string;
	legend?: LegendItem[];
	footer?: string;
	filename: string;
	background?: string;
	fontFamily?: string;
	/** Pixel density of the PNG (2 = sharp on most screens). */
	scale?: number;
	/** false returns the PNG without downloading it. */
	download?: boolean;
}

const TEXT = '#f1f5f9';
const MUTED = '#94a3b8';

async function toDataUrl(href: string): Promise<string | null> {
	try {
		const res = await fetch(href);
		if (!res.ok) return null;
		const blob = await res.blob();
		return await new Promise((resolve) => {
			const reader = new FileReader();
			reader.onload = () => resolve(String(reader.result));
			reader.onerror = () => resolve(null);
			reader.readAsDataURL(blob);
		});
	} catch {
		return null;
	}
}

function loadImage(src: string): Promise<HTMLImageElement> {
	return new Promise((resolve, reject) => {
		const img = new Image();
		img.onload = () => resolve(img);
		img.onerror = () => reject(new Error('The chart could not be drawn as an image.'));
		img.src = src;
	});
}

/** Wraps `text` to lines no wider than `width`. */
function wrap(ctx: CanvasRenderingContext2D, text: string, width: number): string[] {
	const words = text.split(/\s+/);
	const lines: string[] = [];
	let line = '';
	for (const w of words) {
		const next = line ? `${line} ${w}` : w;
		if (ctx.measureText(next).width > width && line) {
			lines.push(line);
			line = w;
		} else line = next;
	}
	if (line) lines.push(line);
	return lines;
}

export async function saveChartImage(svg: SVGSVGElement, opts: ChartImageOptions): Promise<Blob> {
	const scale = opts.scale ?? 2;
	const font = opts.fontFamily ?? 'system-ui, sans-serif';
	const box = svg.viewBox.baseVal;
	const w = box?.width || svg.clientWidth;
	const h = box?.height || svg.clientHeight;

	const clone = svg.cloneNode(true) as SVGSVGElement;
	clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
	clone.setAttribute('width', String(w));
	clone.setAttribute('height', String(h));
	clone.style.fontFamily = font;
	// Tooltips and hover marks aren't part of the picture.
	clone.querySelectorAll('.hot').forEach((el) => el.classList.remove('hot'));
	// The view being switched away from is still fading out; it isn't the chart.
	clone.querySelectorAll('.frame-old').forEach((el) => el.remove());
	// One fetch per distinct photo (a manager can appear many times).
	const photos = new Map<string, Promise<string | null>>();
	await Promise.all(
		[...clone.querySelectorAll('image')].map(async (img) => {
			const href = img.getAttribute('href');
			if (!href || href.startsWith('data:')) return;
			if (!photos.has(href)) photos.set(href, toDataUrl(href));
			const data = await photos.get(href)!;
			if (data) img.setAttribute('href', data);
			else img.remove();
		})
	);
	const svgUrl = URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(clone)], { type: 'image/svg+xml' }));

	try {
		const chart = await loadImage(svgUrl);
		const pad = 24;
		const canvas = document.createElement('canvas');
		const ctx = canvas.getContext('2d')!;

		// Measure the header and footer first, so the canvas fits them.
		ctx.font = `12px ${font}`;
		const subtitle = opts.subtitle ? wrap(ctx, opts.subtitle, w) : [];
		// Legend items flow into rows no wider than the chart.
		const legendRows: { item: LegendItem; x: number }[][] = [];
		let lx = 0;
		for (const item of opts.legend ?? []) {
			const width = 20 + ctx.measureText(item.label).width;
			if (!legendRows.length || (lx > 0 && lx + width > w)) {
				legendRows.push([]);
				lx = 0;
			}
			legendRows[legendRows.length - 1].push({ item, x: lx });
			lx += width + 18;
		}
		ctx.font = `11px ${font}`;
		const footer = opts.footer ? wrap(ctx, opts.footer, w) : [];
		const headerH = 26 + subtitle.length * 17 + legendRows.length * 22 + (legendRows.length ? 4 : 0) + 12;
		const footerH = footer.length ? 12 + footer.length * 15 : 8;
		canvas.width = Math.round((w + pad * 2) * scale);
		canvas.height = Math.round((pad + headerH + h + footerH + pad) * scale);
		ctx.scale(scale, scale);

		ctx.fillStyle = opts.background ?? '#162036';
		ctx.fillRect(0, 0, w + pad * 2, pad + headerH + h + footerH + pad);

		let y = pad + 16;
		ctx.fillStyle = TEXT;
		ctx.font = `600 17px ${font}`;
		ctx.fillText(opts.title, pad, y);
		y += 10;
		ctx.font = `12px ${font}`;
		ctx.fillStyle = MUTED;
		for (const line of subtitle) {
			y += 17;
			ctx.fillText(line, pad, y);
		}
		for (const row of legendRows) {
			y += 22;
			for (const { item, x: offset } of row) {
				const x = pad + offset;
				ctx.globalAlpha = item.opacity ?? 1;
				ctx.fillStyle = item.color;
				ctx.strokeStyle = item.color;
				if (item.kind === 'line') ctx.fillRect(x, y - 5, 14, 2);
				else if (item.kind === 'rect') ctx.fillRect(x, y - 9, 12, 10);
				else {
					ctx.beginPath();
					ctx.arc(x + 6, y - 4, 4.5, 0, Math.PI * 2);
					if (item.kind === 'dot') ctx.fill();
					else {
						ctx.lineWidth = 2;
						ctx.stroke();
					}
				}
				ctx.globalAlpha = 1;
				ctx.fillStyle = '#cbd5e1';
				ctx.fillText(item.label, x + 20, y);
			}
		}
		const top = pad + headerH;
		ctx.drawImage(chart, pad, top, w, h);
		ctx.fillStyle = MUTED;
		ctx.font = `11px ${font}`;
		footer.forEach((line, i) => ctx.fillText(line, pad, top + h + 22 + i * 15));

		const blob: Blob = await new Promise((resolve, reject) =>
			canvas.toBlob((b) => (b ? resolve(b) : reject(new Error('The image could not be created.'))), 'image/png')
		);
		if (opts.download !== false) {
			const a = document.createElement('a');
			a.href = URL.createObjectURL(blob);
			a.download = opts.filename;
			document.body.appendChild(a);
			a.click();
			a.remove();
			setTimeout(() => URL.revokeObjectURL(a.href), 2000);
		}
		return blob;
	} finally {
		URL.revokeObjectURL(svgUrl);
	}
}
