//! Bounded previews derived only from the bytes covered by the reviewed hash.

use std::io::Cursor;

use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, Limits};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::error::{AppError, AppResult};

pub(super) const MAX_INPUT: usize = 64 * 1024 * 1024;
pub(super) const MAX_AVIF_INPUT: usize = 4 * 1024 * 1024;
const MAX_PIXELS: u64 = 60_000_000;
const MAX_DECODED_BYTES: u64 = 256 * 1024 * 1024;
const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const EDGE: u32 = 1024;
static PREVIEWS: Semaphore = Semaphore::const_new(2);

pub(super) fn acquire() -> AppResult<SemaphorePermit<'static>> {
    PREVIEWS.try_acquire().map_err(|_| {
        AppError::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "preview_busy",
            "Too many previews are being prepared. Try again shortly.",
        )
    })
}

pub(super) async fn render(
    bytes: Vec<u8>,
    expected: String,
    permit: SemaphorePermit<'static>,
    avif: bool,
) -> AppResult<(&'static str, Vec<u8>)> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if crate::crypto::sha256_hex(&bytes) != expected {
            return Err(AppError::conflict(
                "preview_changed",
                "refresh current versions",
            ));
        }
        if avif {
            return Ok(("image/avif", bytes));
        }
        decode(&bytes)
    })
    .await
    .map_err(AppError::internal)?
}

fn decode(bytes: &[u8]) -> AppResult<(&'static str, Vec<u8>)> {
    let invalid = || {
        AppError::conflict(
            "preview_unavailable",
            "Image is corrupt, unsupported, or exceeds preview limits.",
        )
    };
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| invalid())?;
    if !matches!(
        reader.format(),
        Some(ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::WebP)
    ) {
        return Err(invalid());
    }
    let mut limits = Limits::default();
    limits.max_image_width = Some(12_000);
    limits.max_image_height = Some(12_000);
    limits.max_alloc = Some(MAX_DECODED_BYTES);
    reader.limits(limits.clone());
    let mut decoder = reader.into_decoder().map_err(|_| invalid())?;
    let (width, height) = decoder.dimensions();
    if width == 0
        || height == 0
        || u64::from(width) * u64::from(height) > MAX_PIXELS
        || decoder.total_bytes() > MAX_DECODED_BYTES
    {
        return Err(invalid());
    }
    limits
        .reserve(decoder.total_bytes())
        .map_err(|_| invalid())?;
    decoder.set_limits(limits).map_err(|_| invalid())?;
    let orientation = decoder.orientation().map_err(|_| invalid())?;
    let image = DynamicImage::from_decoder(decoder).map_err(|_| invalid())?;
    let mut thumbnail = if width > EDGE || height > EDGE {
        let thumbnail = image.thumbnail(EDGE, EDGE);
        drop(image);
        thumbnail
    } else {
        image
    };
    thumbnail.apply_orientation(orientation);
    let (mime, format, thumbnail) = if thumbnail.color().has_alpha() {
        (
            "image/png",
            ImageFormat::Png,
            DynamicImage::ImageRgba8(thumbnail.to_rgba8()),
        )
    } else {
        (
            "image/jpeg",
            ImageFormat::Jpeg,
            DynamicImage::ImageRgb8(thumbnail.to_rgb8()),
        )
    };
    let mut output = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut output, format)
        .map_err(|_| invalid())?;
    let output = output.into_inner();
    if output.len() > MAX_OUTPUT {
        return Err(invalid());
    }
    Ok((mime, output))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refuses_changed_or_corrupt_content() {
        static TEST_PREVIEWS: Semaphore = Semaphore::const_new(1);
        let bytes = b"not an image".to_vec();
        let error = render(
            bytes.clone(),
            "different".into(),
            TEST_PREVIEWS.try_acquire().unwrap(),
            false,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "preview_changed");
        let hash = crate::crypto::sha256_hex(&bytes);
        let error = render(bytes, hash, TEST_PREVIEWS.try_acquire().unwrap(), false)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "preview_unavailable");
    }

    #[tokio::test]
    async fn legacy_avif_passthrough_still_checks_reviewed_hash() {
        static TEST_PREVIEWS: Semaphore = Semaphore::const_new(1);
        let bytes = b"synthetic avif".to_vec();
        let hash = crate::crypto::sha256_hex(&bytes);
        let (mime, returned) = render(
            bytes.clone(),
            hash,
            TEST_PREVIEWS.try_acquire().unwrap(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(mime, "image/avif");
        assert_eq!(returned, bytes);
        assert_eq!(
            render(
                bytes,
                "different".into(),
                TEST_PREVIEWS.try_acquire().unwrap(),
                true
            )
            .await
            .unwrap_err()
            .code(),
            "preview_changed"
        );
    }

    #[test]
    fn rejects_oversized_dimensions_before_decoding() {
        let mut jpeg = Vec::new();
        let image = image::RgbImage::new(12_001, 1);
        DynamicImage::ImageRgb8(image)
            .write_to(&mut Cursor::new(&mut jpeg), ImageFormat::Jpeg)
            .unwrap();
        assert_eq!(decode(&jpeg).unwrap_err().code(), "preview_unavailable");
    }

    #[test]
    fn preserves_transparency_and_rejects_truncated_images() {
        let mut png = Vec::new();
        let image = image::RgbaImage::from_pixel(8, 8, image::Rgba([40, 50, 60, 100]));
        DynamicImage::ImageRgba8(image)
            .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
            .unwrap();
        let (mime, preview) = decode(&png).unwrap();
        assert_eq!(mime, "image/png");
        assert_eq!(
            image::load_from_memory(&preview).unwrap().to_rgba8()[(0, 0)].0,
            [40, 50, 60, 100]
        );
        png.truncate(png.len() / 2);
        assert_eq!(decode(&png).unwrap_err().code(), "preview_unavailable");
    }
}
