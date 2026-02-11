// use std::fs;
// use std::fs::OpenOptions;
// use std::io;
use std::io::Read;
// use std::path::Path;

// use anyhow::Context;

// pub fn read_or_create_atomic<F>(path: &Path, default_fn: F) -> anyhow::Result<String>
// where
//     F: FnOnce() -> anyhow::Result<String>,
// {
//     // Try to create exclusively (fails if exists)
//     match OpenOptions::new()
//         .write(true)
//         .create_new(true) // Fails if file exists
//         .open(path)
//     {
//         Ok(mut file) => {
//             // File was created, write default
//             let contents = default_fn()?;
//             file.write_all(contents.as_bytes())?;
//             Ok(contents)
//         }
//         Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
//             // File exists, read it
//             let file = fs::File::open(path)?;
//             io::read_to_string(file).with_context(|| "Read error")
//         }
//         Err(e) => Err(e.into()),
//     }
// }

pub fn read_retry_on_intr<'a, R: Read>(
    reader: &mut R,
    out: &'a mut [u8],
) -> std::io::Result<&'a mut [u8]> {
    let mut total: usize = 0;
    let out_len = out.len();

    while total < out_len {
        match reader.read(&mut out[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
    }

    Ok(&mut out[..total])
}
