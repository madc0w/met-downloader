const cloudinary = require('cloudinary').v2;
const fs = require('fs');
const path = require('path');

// Configure Cloudinary
cloudinary.config({
	cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
	api_key: process.env.CLOUDINARY_API_KEY,
	api_secret: process.env.CLOUDINARY_API_SECRET,
});

const IMAGES_DIR = path.join(__dirname, 'images-queries');
const CHECKPOINT_FILE = path.join(__dirname, '.cloudinary-upload.ckpt.json');

// Load checkpoint if exists
function loadCheckpoint() {
	try {
		if (fs.existsSync(CHECKPOINT_FILE)) {
			return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
		}
	} catch (err) {
		console.error('Error loading checkpoint:', err.message);
	}
	return { uploaded: [], failed: [] };
}

// Save checkpoint
function saveCheckpoint(checkpoint) {
	fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
}

// Upload a single image
async function uploadImage(filePath, fileName) {
	try {
		const publicId = path.basename(fileName, path.extname(fileName));

		const result = await cloudinary.uploader.upload(filePath, {
			public_id: publicId,
			folder: 'images-queries',
			overwrite: false,
			resource_type: 'image',
		});

		return {
			success: true,
			url: result.secure_url,
			publicId: result.public_id,
		};
	} catch (err) {
		return { success: false, error: err.message };
	}
}

// Main upload function
async function uploadAllImages() {
	const checkpoint = loadCheckpoint();
	const uploadedSet = new Set(checkpoint.uploaded);

	// Get all image files
	const files = fs.readdirSync(IMAGES_DIR).filter((file) => {
		const ext = path.extname(file).toLowerCase();
		return ['.jpg', '.jpeg', '.png', '.gif', '.webp'].includes(ext);
	});

	console.log(`Found ${files.length} images in images-queries folder`);
	console.log(`Already uploaded: ${checkpoint.uploaded.length}`);

	const toUpload = files.filter((f) => !uploadedSet.has(f));
	console.log(`Images to upload: ${toUpload.length}`);

	if (toUpload.length === 0) {
		console.log('All images already uploaded!');
		return;
	}

	let successCount = 0;
	let failCount = 0;

	for (let i = 0; i < toUpload.length; i++) {
		const file = toUpload[i];
		const filePath = path.join(IMAGES_DIR, file);

		console.log(`[${i + 1}/${toUpload.length}] Uploading: ${file}`);

		const result = await uploadImage(filePath, file);

		if (result.success) {
			console.log(`  ✓ Uploaded: ${result.url}`);
			checkpoint.uploaded.push(file);
			successCount++;
		} else {
			console.log(`  ✗ Failed: ${result.error}`);
			checkpoint.failed.push({ file, error: result.error });
			failCount++;
		}

		// Save checkpoint every 10 uploads
		if ((i + 1) % 10 === 0) {
			saveCheckpoint(checkpoint);
		}

		// Small delay to avoid rate limiting
		await new Promise((resolve) => setTimeout(resolve, 100));
	}

	// Final checkpoint save
	saveCheckpoint(checkpoint);

	console.log('\n--- Upload Summary ---');
	console.log(`Total uploaded: ${checkpoint.uploaded.length}`);
	console.log(`Success this run: ${successCount}`);
	console.log(`Failed this run: ${failCount}`);

	if (checkpoint.failed.length > 0) {
		console.log('\nFailed uploads:');
		checkpoint.failed.forEach((f) => console.log(`  - ${f.file}: ${f.error}`));
	}
}

// Run the upload
uploadAllImages().catch(console.error);
