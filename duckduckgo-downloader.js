// Simple Puppeteer loader for DuckDuckGo Images
// Usage: node duckduckgo-downloader.js [url] [screenshotPath]
// Example:
//   node duckduckgo-downloader.js "https://duckduckgo.com/?t=ffab&q=science&ia=images&iax=images" ddg-science.png

const query = 'microbial life';

async function main() {
	const url = `https://duckduckgo.com/?q=${query}&iar=images`;
	// const screenshotPath = process.argv[3] || 'duckduckgo-science.png';
	const limit = parseInt(process.argv[4] || process.env.LIMIT || '0', 10) || 0; // 0 = no explicit limit

	const puppeteer = require('puppeteer');
	const browser = await puppeteer.launch({
		headless: true,
		// Extra args improve compatibility on some environments
		args: ['--no-sandbox', '--disable-setuid-sandbox'],
		defaultViewport: { width: 1366, height: 900 },
	});

	const page = await browser.newPage();

	// Set a realistic UA to avoid some anti-bot heuristics
	await page.setUserAgent(
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
	);

	try {
		console.log('Navigating to:', url);
		const startTime = Date.now();
		await page.goto(url, { waitUntil: 'networkidle2', timeout: 60_000 });
		console.log(`page loaded in ${(Date.now() - startTime) / 1000}s`);

		// Try to accept potential consent popups if present (region dependent)
		const consentSelectors = [
			'#onetrust-accept-btn-handler',
			"button[aria-label='Accept all']",
			"button[aria-label='Accept All']",
			"button:has-text('Accept all')",
			"button:has-text('Accept All')",
			'#consent-banner button.accept',
		];
		for (const sel of consentSelectors) {
			const el = await page.$(sel).catch(() => null);
			if (el) {
				try {
					await el.click({ delay: 50 });
					console.log('Clicked consent button:', sel);
					break;
				} catch {}
			}
		}

		// Wait for images to appear in the grid; be flexible on selector
		const imageSelectors = [
			'div.tile--img img',
			'div.js-images img',
			'#zci-images img',
			'img[loading]',
			'img',
		];

		let isFound = false;
		for (const sel of imageSelectors) {
			try {
				await page.waitForSelector(sel, { timeout: 15_000 });
				const count = await page.$$eval(sel, (imgs) => imgs.length);
				if (count > 0) {
					console.log(`Found ${count} images with selector: ${sel}`);
					isFound = true;
					break;
				}
			} catch {
				// try next selector
			}
		}
		if (!isFound) {
			console.warn('No images found within timeout');
		}

		// Optional: quick scroll to trigger lazy loading of more thumbs
		await autoScroll(page, 2000);

		const title = await page.title();
		console.log('Page title:', title);

		// // Take a full-page screenshot for verification
		// await page.screenshot({ path: screenshotPath, fullPage: true });
		// console.log('Saved screenshot:', screenshotPath);

		// Ensure output directory exists
		const outDir = path.resolve(__dirname, 'images-duckduckgo');
		ensureDir(outDir);

		// Iterative harvest: collect direct URLs, download, then scroll to load more
		const seen = new Set();
		let numDownloaded = 0;
		const maxRounds = parseInt(process.env.ROUNDS || '15', 10) || 15; // how many harvest rounds
		const pagesPerRound = parseInt(process.env.PAGES || '4', 10) || 4; // viewport heights per round
		const pauseMs = parseInt(process.env.PAUSE || '600', 10) || 600; // pause between scroll steps

		let noGrowth = 0;
		for (let round = 1; round <= maxRounds; round++) {
			if (limit && numDownloaded >= limit) break;

			const allUrls = await collectOriginalImageUrls(page, 0);
			const newUrls = allUrls.filter((u) => !seen.has(u));
			newUrls.forEach((u) => seen.add(u));
			if (newUrls.length > 0) {
				console.log(`Round ${round}: ${newUrls.length} new direct URL(s).`);
				for (const u of newUrls) {
					if (limit && numDownloaded >= limit) break;
					const ok = await downloadImage(u, outDir, { referer: url });
					if (ok) numDownloaded++;
				}
			} else {
				console.log(`Round ${round}: 0 new URLs discovered.`);
			}

			if (limit && numDownloaded >= limit) break;

			// Scroll a few pages to trigger more thumbnails
			const beforeCount = await countThumbnails(page);
			await scrollPages(page, pagesPerRound, pauseMs);
			const afterCount = await countThumbnails(page);

			if (afterCount <= beforeCount) {
				noGrowth++;
				if (noGrowth >= 2) {
					console.log('No more thumbnails are loading. Stopping.');
					break;
				}
			} else {
				noGrowth = 0;
			}
		}

		// If still nothing, fallback to click-based extraction
		if (numDownloaded === 0) {
			const clickedDownloaded = await downloadAllVisibleImages(page, {
				limit,
				referer: url,
			});
			numDownloaded += clickedDownloaded;
		}

		console.log(`Downloaded ${numDownloaded} file(s) to images-duckduckgo/`);
	} finally {
		await browser.close().catch(() => {});
	}
}

async function autoScroll(page, maxPixels = 2000) {
	try {
		await page.evaluate(async (max) => {
			await new Promise((resolve) => {
				let totalHeight = 0;
				const distance = 400;
				const timer = setInterval(() => {
					const scrollHeight = document.body.scrollHeight;
					window.scrollBy(0, distance);
					totalHeight += distance;
					if (totalHeight >= Math.min(max, scrollHeight - window.innerHeight)) {
						clearInterval(timer);
						resolve();
					}
				}, 150);
			});
		}, maxPixels);
	} catch {}
}

async function countThumbnails(page) {
	try {
		return await page.evaluate(() => {
			return document.querySelectorAll(
				'div.tile--img img, div.js-images img, img'
			).length;
		});
	} catch {
		return 0;
	}
}

async function scrollPages(page, pages = 3, pauseMs = 600) {
	try {
		for (let i = 0; i < pages; i++) {
			await page.evaluate(() => {
				window.scrollBy(0, window.innerHeight * 0.9);
			});
			await delay(pauseMs);
		}
		// Nudge down to bottom once more and wait a bit for network
		await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
		await delay(Math.max(400, Math.floor(pauseMs / 2)));
	} catch {}
}

if (require.main === module) {
	main().catch((err) => {
		console.error('Error:', err && err.stack ? err.stack : err);
		process.exitCode = 1;
	});
}

// ---- Helpers for image extraction and download ----
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// Extract original image URLs directly from DuckDuckGo proxy URLs present in thumbnails
async function collectOriginalImageUrls(page, limit = 0) {
	const urls = await page.evaluate(() => {
		const out = new Set();
		const imgs = Array.from(
			document.querySelectorAll('div.tile--img img, div.js-images img, img')
		);
		const extractFrom = (raw) => {
			try {
				if (!raw) return null;
				const url = new URL(raw, location.href);
				// DDG proxy forms
				if (url.hostname.includes('duckduckgo.com')) {
					const u = url.searchParams.get('u');
					if (u) return decodeURIComponent(u);
					const uddg = url.searchParams.get('uddg');
					if (uddg) return decodeURIComponent(uddg);
				}
				if (url.hostname.includes('external-content.duckduckgo.com')) {
					const u = url.searchParams.get('u');
					if (u) return decodeURIComponent(u);
				}
			} catch {}
			return null;
		};

		for (const img of imgs) {
			const cands = [
				img.getAttribute('src'),
				img.getAttribute('data-src'),
				img.getAttribute('srcset'),
			];
			for (const cand of cands) {
				if (!cand) continue;
				// srcset may contain multiple URLs separated by commas
				const parts = cand
					.split(',')
					.map((s) => s.trim().split(' ')[0])
					.filter(Boolean);
				for (const p of parts) {
					const or = extractFrom(p);
					if (or) out.add(or);
				}
			}
			// Also check a closest anchor
			const a = img.closest('a');
			if (a && a.href) {
				const or = extractFrom(a.href);
				if (or) out.add(or);
			}
		}
		return Array.from(out);
	});
	// Apply limit and de-dup
	const unique = Array.from(new Set(urls));
	return limit && limit > 0 ? unique.slice(0, limit) : unique;
}

async function downloadAllVisibleImages(page, options = {}) {
	const { limit = 0, referer } = options;
	const outDir = path.resolve(__dirname, 'images-duckduckgo');
	ensureDir(outDir);

	// Prefer clickable anchors within tiles, then tiles, then plain images
	const preferredSelectors = [
		'div.tile--img a',
		'div.js-images .tile--img a',
		'div.tile--img',
		'img[loading]',
		'img',
	];

	// Snapshot an initial count for user-friendly logging
	let initialCount = 0;
	for (const sel of preferredSelectors) {
		const list = await page.$$(sel);
		if (list && list.length) {
			initialCount = list.length;
			break;
		}
	}

	const toAttempt =
		limit && limit > 0 ? Math.min(limit, initialCount) : initialCount;
	console.log(
		`Processing up to ${toAttempt} image(s) from ${initialCount} found.`
	);

	let downloadedCount = 0;
	for (let i = 0; i < toAttempt; i++) {
		if (limit && downloadedCount >= limit) break;
		// Re-query each loop to avoid stale handles and cross-world mismatches
		let el = null;
		for (const sel of preferredSelectors) {
			const list = await page.$$(sel);
			if (list && list.length > i) {
				el = list[i];
				break;
			}
		}
		if (!el) break;
		try {
			// Scroll into view
			await el.evaluate((node) =>
				node.scrollIntoView({ behavior: 'instant', block: 'center' })
			);
			await delay(120);

			// Try clicking via in-page event on closest clickable ancestor
			const clicked = await el.evaluate((node) => {
				const cand =
					node.closest(
						'a,button,[role="button"],div.tile--img,article.tile--img'
					) || node;
				try {
					const evt = new MouseEvent('click', {
						bubbles: true,
						cancelable: true,
						view: window,
					});
					cand.dispatchEvent(evt);
					return true;
				} catch {
					try {
						cand.click();
						return true;
					} catch {
						return false;
					}
				}
			});

			if (!clicked) {
				const box = await el.boundingBox();
				if (box) {
					await page.mouse.click(
						box.x + box.width / 2,
						box.y + box.height / 2,
						{ delay: 30 }
					);
				} else {
					try {
						await el.focus();
						await page.keyboard.press('Enter');
					} catch {}
				}
			}

			// Wait for the detail overlay/panel to show a "View file" link
			const viewUrl = await waitAndGetViewFileUrl(page, 12_000);
			if (!viewUrl) {
				// Try a small extra wait and attempt again
				await delay(400);
				const retryUrl = await waitAndGetViewFileUrl(page, 4_000);
				if (!retryUrl) {
					console.warn(`[${i + 1}/${toAttempt}] No "View file" URL found.`);
				} else {
					const saved = await downloadImage(retryUrl, outDir, { referer });
					if (saved) downloadedCount++;
				}
			} else {
				const saved = await downloadImage(viewUrl, outDir, { referer });
				if (saved) downloadedCount++;
			}
		} catch (e) {
			console.warn(
				`[${i + 1}/${toAttempt}] Error handling image:`,
				e.message || e
			);
		} finally {
			// Try to close overlay with Escape so next clicks work reliably
			try {
				await page.keyboard.press('Escape');
			} catch {}
			await delay(120);
		}
	}

	return downloadedCount;
}

// Replaces earlier simple matcher with a more robust implementation
async function waitAndGetViewFileUrl(page, timeoutMs = 10_000) {
	// Briefly wait for an overlay/dialog to appear
	const overlaySelectors = [
		'div[role="dialog"]',
		'div.modal',
		'#image_detail',
		'#image_modal',
		'.c-detail',
	];
	for (const sel of overlaySelectors) {
		try {
			await page.waitForSelector(sel, { timeout: Math.min(1500, timeoutMs) });
			break;
		} catch {}
	}

	// Strategy 1: common label patterns
	const patterns = [
		/view\s*file/i,
		/view\s*image/i,
		/open\s*image/i,
		/open\s*file/i,
		/show\s*image/i,
	];
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		const href = await page.evaluate(
			(patternsSerialized) => {
				const patterns = patternsSerialized.map(
					(p) => new RegExp(p.pattern, p.flags)
				);
				const textOf = (el) =>
					(el && (el.textContent || el.innerText || '')).toLowerCase().trim();
				const links = Array.from(document.querySelectorAll('a[href], button'));
				for (const el of links) {
					const txt = textOf(el);
					if (patterns.some((re) => re.test(txt))) {
						if (el.tagName.toLowerCase() === 'a') return el.href || null;
					}
					const aria = (
						el.getAttribute &&
						(el.getAttribute('aria-label') || el.getAttribute('title') || '')
					)
						.toLowerCase()
						.trim();
					if (aria && patterns.some((re) => re.test(aria))) {
						if (el.tagName.toLowerCase() === 'a') return el.href || null;
					}
				}
				return null;
			},
			patterns.map((re) => ({ pattern: re.source, flags: re.flags }))
		);
		if (href) return href;
		await delay(200);
	}

	// Strategy 2: heuristic — find a direct image link in a visible overlay
	const heuristicHref = await page.evaluate(() => {
		const isVisible = (el) => !!(el && el.offsetWidth && el.offsetHeight);
		const isImgUrl = (u) =>
			/\.(avif|webp|jpe?g|png|gif|bmp|svg)(\?.*)?$/i.test(u || '');
		const overlays = Array.from(
			document.querySelectorAll(
				'div[role="dialog"], div.modal, #image_detail, #image_modal, .c-detail'
			)
		).filter(isVisible);
		for (const o of overlays) {
			const a = Array.from(o.querySelectorAll('a[href]')).find((x) =>
				isImgUrl(x.href)
			);
			if (a) return a.href;
		}
		// Global fallback: first image-like anchor on page
		const a = Array.from(document.querySelectorAll('a[href]')).find((x) =>
			isImgUrl(x.href)
		);
		return a ? a.href : null;
	});
	return heuristicHref || null;
}
function ensureDir(dir) {
	if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function toFilenameFromUrl(u, fallbackExt = '.jpg') {
	try {
		const urlObj = new URL(u);
		let name = path.basename(urlObj.pathname);
		if (!name || name === '/' || name.startsWith('?')) {
			name = `image${fallbackExt}`;
		}
		// Strip querystring leftovers
		name = name.replace(/[\?#].*$/, '');
		if (!path.extname(name)) name += fallbackExt;
		// Clean dangerous characters
		name = name.replace(/[^a-zA-Z0-9_\-\.]/g, '_');
		return name;
	} catch {
		return `image${fallbackExt}`;
	}
}

async function downloadImage(fileUrl, outDir, opts = {}) {
	return new Promise((resolve) => {
		try {
			const urlObj = new URL(fileUrl);
			const mod = urlObj.protocol === 'http:' ? http : https;

			const req = mod.get(
				{
					hostname: urlObj.hostname,
					path: urlObj.pathname + (urlObj.search || ''),
					protocol: urlObj.protocol,
					headers: {
						'User-Agent':
							'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
						Referer: opts.referer || 'https://duckduckgo.com/',
						Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
					},
				},
				(res) => {
					if (
						res.statusCode &&
						res.statusCode >= 300 &&
						res.statusCode < 400 &&
						res.headers.location
					) {
						// Handle redirects
						const redirectUrl = new URL(
							res.headers.location,
							urlObj
						).toString();
						res.resume(); // drain
						downloadImage(redirectUrl, outDir, opts).then(resolve);
						return;
					}
					if (res.statusCode !== 200) {
						console.warn(`Failed to download (${res.statusCode}): ${fileUrl}`);
						res.resume();
						return resolve(false);
					}

					// Decide filename now that we can inspect Content-Type
					const contentType = (res.headers['content-type'] || '').toLowerCase();
					const extFromHeader = getExtFromContentType(contentType) || '.jpg';
					const baseName = toFilenameFromUrl(fileUrl, ''); // avoid adding fallback here

					const known = new Set([
						'.jpg',
						'.jpeg',
						'.png',
						'.webp',
						'.avif',
						'.gif',
						'.bmp',
						'.svg',
						'.tif',
						'.tiff',
					]);
					const currentExt = path.extname(baseName).toLowerCase();
					let finalName;
					if (currentExt && known.has(currentExt)) {
						finalName = baseName; // trust known ext from URL
					} else {
						// Keep full baseName as-is and append the inferred extension
						finalName = baseName + extFromHeader;
					}

					const filePath = uniquePath(path.join(outDir, finalName));

					const ws = fs.createWriteStream(filePath);
					res.pipe(ws);
					ws.on('finish', () => {
						ws.close(() => {
							console.log('Saved:', path.basename(filePath));
							resolve(true);
						});
					});
					ws.on('error', (err) => {
						console.warn('Write error:', err.message || err);
						try {
							fs.unlinkSync(filePath);
						} catch {}
						resolve(false);
					});
				}
			);

			req.on('error', (err) => {
				console.warn('Request error:', err.message || err);
				resolve(false);
			});
		} catch (e) {
			console.warn('Download error:', e.message || e);
			resolve(false);
		}
	});
}

function getExtFromContentType(ct) {
	if (!ct) return null;
	if (ct.includes('image/jpeg') || ct.includes('image/jpg')) return '.jpg';
	if (ct.includes('image/png')) return '.png';
	if (ct.includes('image/webp')) return '.webp';
	if (ct.includes('image/avif')) return '.avif';
	if (ct.includes('image/gif')) return '.gif';
	if (ct.includes('image/bmp')) return '.bmp';
	if (ct.includes('image/svg')) return '.svg';
	if (ct.includes('image/tiff')) return '.tiff';
	if (ct.includes('image/heic')) return '.heic';
	return null;
}

function uniquePath(p) {
	if (!fs.existsSync(p)) return p;
	const dir = path.dirname(p);
	const ext = path.extname(p);
	const name = path.basename(p, ext);
	let idx = 1;
	let candidate;
	do {
		candidate = path.join(dir, `${name}_${idx}${ext}`);
		idx++;
	} while (fs.existsSync(candidate));
	return candidate;
}

function delay(ms) {
	return new Promise((r) => setTimeout(r, ms));
}
