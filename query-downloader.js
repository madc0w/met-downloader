// DuckDuckGo Image Downloader using queries.json
// Downloads up to 8 images per query and saves metadata to image-metadata.json
// Resizes images to max 800KB before saving

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const puppeteer = require('puppeteer');

const QUERIES_FILE = path.join(__dirname, 'queries.json');
const METADATA_FILE = path.join(__dirname, 'image-metadata.json');
const OUTPUT_DIR = path.join(__dirname, 'images-queries');
const MAX_FILE_SIZE = 800 * (1 << 10); // 800KB in bytes

let sharp;
try {
	sharp = require('sharp');
} catch (e) {
	console.error('sharp module not found. Please run: npm install sharp');
	process.exit(1);
}

async function main() {
	// Load queries
	const queriesData = JSON.parse(fs.readFileSync(QUERIES_FILE, 'utf-8'));

	// Flatten all queries into a single array
	const allQueries = [];
	for (const category of Object.keys(queriesData)) {
		for (const query of queriesData[category]) {
			allQueries.push({ category, query });
		}
	}

	console.log(
		`Loaded ${allQueries.length} queries from ${
			Object.keys(queriesData).length
		} categories`
	);

	// Ensure output directory exists
	ensureDir(OUTPUT_DIR);

	// Load existing metadata if present
	let metadata = [];
	if (fs.existsSync(METADATA_FILE)) {
		try {
			metadata = JSON.parse(fs.readFileSync(METADATA_FILE, 'utf-8'));
			console.log(`Loaded ${metadata.length} existing metadata entries`);
		} catch (e) {
			console.warn('Could not parse existing metadata file, starting fresh');
			metadata = [];
		}
	}

	// Track already processed queries
	const processedQueries = new Set(metadata.map((m) => m.query));

	// Launch browser
	const browser = await puppeteer.launch({
		headless: true,
		args: ['--no-sandbox', '--disable-setuid-sandbox'],
		defaultViewport: { width: 1366, height: 900 },
	});

	const page = await browser.newPage();
	await page.setUserAgent(
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
	);

	try {
		for (let i = 0; i < allQueries.length; i++) {
			const { category, query } = allQueries[i];

			// Skip if already processed
			if (processedQueries.has(query)) {
				console.log(
					`[${i + 1}/${
						allQueries.length
					}] Skipping "${query}" (already processed)`
				);
				continue;
			}

			console.log(
				`[${i + 1}/${allQueries.length}] Processing: "${query}" (${category})`
			);

			const results = await downloadImagesForQuery(page, query, category, 8);

			if (results.length > 0) {
				metadata.push(...results);
				processedQueries.add(query);

				// Save metadata after each successful download
				fs.writeFileSync(METADATA_FILE, JSON.stringify(metadata, null, 2));
				console.log(
					`  ✓ Saved ${results.length} image(s): ${results
						.map((r) => r.filename)
						.join(', ')}`
				);
			} else {
				console.log(`  ✗ Failed to download images for "${query}"`);
			}

			// Small delay between queries to be polite
			await delay(1000 + Math.random() * 1000);
		}
	} finally {
		await browser.close().catch(() => {});
	}

	console.log(`\nDone! Downloaded ${metadata.length} images.`);
	console.log(`Metadata saved to: ${METADATA_FILE}`);
	console.log(`Images saved to: ${OUTPUT_DIR}`);
}

async function downloadImagesForQuery(page, query, category, maxImages = 8) {
	const url = `https://duckduckgo.com/?q=${encodeURIComponent(
		query
	)}&iar=images&iax=images&ia=images`;

	try {
		await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

		// Wait for images to load
		await delay(2000);

		// Try to accept consent popups
		const consentSelectors = [
			'#onetrust-accept-btn-handler',
			"button[aria-label='Accept all']",
			"button[aria-label='Accept All']",
		];
		for (const sel of consentSelectors) {
			const el = await page.$(sel).catch(() => null);
			if (el) {
				try {
					await el.click({ delay: 50 });
					await delay(500);
					break;
				} catch {}
			}
		}

		// Scroll a bit to trigger lazy loading
		await autoScroll(page, 1000);
		await delay(1000);

		// Collect image URLs (request more than needed in case some fail)
		const imageUrls = await collectOriginalImageUrls(page, maxImages * 2);

		if (imageUrls.length === 0) {
			console.log(`  No images found for "${query}"`);
			return [];
		}

		// Try to download up to maxImages images
		const results = [];
		let imageIndex = 1;
		for (const imageUrl of imageUrls) {
			if (results.length >= maxImages) {
				break;
			}

			try {
				const result = await downloadAndResizeImage(
					imageUrl,
					query,
					category,
					url,
					imageIndex
				);
				if (result) {
					results.push(result);
					imageIndex++;
				}
			} catch (e) {
				// Silently continue to next image
			}
		}

		return results;
	} catch (e) {
		console.error(`  Error processing query "${query}":`, e.message);
		return [];
	}
}

async function collectOriginalImageUrls(page, limit = 10) {
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
				const parts = cand
					.split(',')
					.map((s) => s.trim().split(' ')[0])
					.filter(Boolean);
				for (const p of parts) {
					const or = extractFrom(p);
					if (or) out.add(or);
				}
			}
			const a = img.closest('a');
			if (a && a.href) {
				const or = extractFrom(a.href);
				if (or) out.add(or);
			}
		}
		return Array.from(out);
	});

	return urls.slice(0, limit);
}

async function downloadAndResizeImage(
	imageUrl,
	query,
	category,
	searchUrl,
	imageIndex = 1
) {
	return new Promise((resolve, reject) => {
		try {
			const urlObj = new URL(imageUrl);
			const mod = urlObj.protocol === 'http:' ? http : https;

			const req = mod.get(
				{
					hostname: urlObj.hostname,
					path: urlObj.pathname + (urlObj.search || ''),
					protocol: urlObj.protocol,
					timeout: 30000,
					headers: {
						'User-Agent':
							'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
						Referer: 'https://duckduckgo.com/',
						Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
					},
				},
				(res) => {
					// Handle redirects
					if (
						res.statusCode >= 300 &&
						res.statusCode < 400 &&
						res.headers.location
					) {
						const redirectUrl = new URL(
							res.headers.location,
							urlObj
						).toString();
						res.resume();
						downloadAndResizeImage(
							redirectUrl,
							query,
							category,
							searchUrl,
							imageIndex
						)
							.then(resolve)
							.catch(reject);
						return;
					}

					if (res.statusCode !== 200) {
						res.resume();
						reject(new Error(`HTTP ${res.statusCode}`));
						return;
					}

					const chunks = [];
					res.on('data', (chunk) => chunks.push(chunk));
					res.on('end', async () => {
						try {
							const buffer = Buffer.concat(chunks);

							// Resize image to fit within 800KB
							const resizedBuffer = await resizeToMaxSize(
								buffer,
								MAX_FILE_SIZE
							);

							// Generate filename from query with index
							const safeQuery = query
								.replace(/[^a-zA-Z0-9]/g, '_')
								.substring(0, 50);
							const filename = `${safeQuery}_${String(imageIndex).padStart(
								2,
								'0'
							)}.jpg`;
							const filePath = path.join(OUTPUT_DIR, filename);

							// Handle duplicate filenames
							const finalPath = uniquePath(filePath);
							const finalFilename = path.basename(finalPath);

							fs.writeFileSync(finalPath, resizedBuffer);

							resolve({
								filename: finalFilename,
								query: query,
								category: category,
								sourceUrl: imageUrl,
							});
						} catch (e) {
							reject(e);
						}
					});
					res.on('error', reject);
				}
			);

			req.on('error', reject);
			req.on('timeout', () => {
				req.destroy();
				reject(new Error('Request timeout'));
			});
		} catch (e) {
			reject(e);
		}
	});
}

async function resizeToMaxSize(buffer, maxBytes) {
	let quality = 90;
	let resizedBuffer = buffer;

	// First, convert to JPEG and get initial size
	try {
		resizedBuffer = await sharp(buffer).jpeg({ quality }).toBuffer();
	} catch (e) {
		// If sharp fails, return original buffer
		console.log(`  Warning: Could not process image with sharp: ${e.message}`);
		return buffer;
	}

	// If already under max size, return as is
	if (resizedBuffer.length <= maxBytes) {
		return resizedBuffer;
	}

	// Get image metadata
	const metadata = await sharp(buffer).metadata();
	let width = metadata.width;
	let height = metadata.height;

	// Progressively reduce quality and/or dimensions
	while (resizedBuffer.length > maxBytes && (quality > 20 || width > 200)) {
		// First try reducing quality
		if (quality > 20) {
			quality -= 10;
			resizedBuffer = await sharp(buffer)
				.resize(width, height, { fit: 'inside', withoutEnlargement: true })
				.jpeg({ quality })
				.toBuffer();

			if (resizedBuffer.length <= maxBytes) {
				break;
			}
		}

		// If still too big, reduce dimensions
		if (resizedBuffer.length > maxBytes && width > 200) {
			width = Math.floor(width * 0.8);
			height = Math.floor(height * 0.8);
			resizedBuffer = await sharp(buffer)
				.resize(width, height, { fit: 'inside', withoutEnlargement: true })
				.jpeg({ quality })
				.toBuffer();
		}
	}

	return resizedBuffer;
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

function ensureDir(dir) {
	if (!fs.existsSync(dir)) {
		fs.mkdirSync(dir, { recursive: true });
	}
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

if (require.main === module) {
	main().catch((err) => {
		console.error('Error:', err.stack || err);
		process.exitCode = 1;
	});
}

module.exports = { main };
