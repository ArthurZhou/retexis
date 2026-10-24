//! Write the frame the renderer just drew to a file.
//!
//! The window is drawn by the software renderer, so its framebuffer is a plain
//! RGBA8 buffer that this process owns. Asking the windowing system for a
//! screenshot (`PrintWindow`, `BitBlt`) is a round trip through DWM that may
//! hand back a stale or partially painted surface — especially on a remote
//! session, where nothing is composited at all. Reading the buffer directly is
//! the one answer that cannot be wrong: it is exactly what the user sees.
//!
//! Enabled by `RETEXIS_SHOT=<file.bmp>`; BMP rather than PNG so that writing a
//! screenshot needs no compression crate. `scripts/gui_demo.py` converts it.

use slint::ComponentHandle;
use std::io::Write;
use std::path::Path;

/// Saves the current frame as a 32-bit BMP. Returns the size written.
///
/// Written to a sibling `.tmp` and renamed, so a reader that polls the file
/// never sees half a frame.
pub fn save(path: &Path, ui: &crate::ChatWindow) -> std::io::Result<(u32, u32)> {
    let buffer = ui
        .window()
        .take_snapshot()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let (width, height) = (buffer.width(), buffer.height());
    let stride = width as usize * 4;
    let image = buffer.as_bytes();

    // BMP rows are bottom-up and BGRA; the buffer is top-down RGBA.
    let mut pixels = Vec::with_capacity(stride * height as usize);
    for row in image.chunks_exact(stride).rev() {
        for pixel in row.chunks_exact(4) {
            pixels.extend_from_slice(&[pixel[2], pixel[1], pixel[0], pixel[3]]);
        }
    }

    let temp = path.with_extension("bmp.tmp");
    let file_size = 14 + 40 + pixels.len() as u32;
    let mut out = std::io::BufWriter::new(std::fs::File::create(&temp)?);
    out.write_all(b"BM")?;
    out.write_all(&file_size.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?;
    out.write_all(&(14u32 + 40).to_le_bytes())?;
    out.write_all(&40u32.to_le_bytes())?;
    out.write_all(&(width as i32).to_le_bytes())?;
    out.write_all(&(height as i32).to_le_bytes())?;
    out.write_all(&1u16.to_le_bytes())?;
    out.write_all(&32u16.to_le_bytes())?;
    out.write_all(&0u32.to_le_bytes())?;
    out.write_all(&(pixels.len() as u32).to_le_bytes())?;
    out.write_all(&2835i32.to_le_bytes())?;
    out.write_all(&2835i32.to_le_bytes())?;
    out.write_all(&0u32.to_le_bytes())?;
    out.write_all(&0u32.to_le_bytes())?;
    out.write_all(&pixels)?;
    out.flush()?;
    drop(out);
    std::fs::rename(&temp, path)?;
    Ok((width, height))
}
