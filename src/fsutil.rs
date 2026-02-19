use std::{
    io::{Read, Write},
    path::Path,
};

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

pub struct MultiWriter<W: Write> {
    writers: Vec<W>,
}

impl<W: Write> MultiWriter<W> {
    pub fn new(writers: Vec<W>) -> Self {
        MultiWriter { writers }
    }
}

impl<W: Write> Write for MultiWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for w in &mut self.writers {
            w.write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        for w in &mut self.writers {
            w.flush()?;
        }
        Ok(())
    }
}

pub fn sync_paths<I, P>(paths: I) -> std::io::Result<()>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    for path in paths {
        let file = std::fs::File::open(path.as_ref())?;
        file.sync_all()?;
    }
    Ok(())
}

pub fn ignore_errorkind(
    result: std::io::Result<()>,
    kind: std::io::ErrorKind,
) -> std::io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == kind => Ok(()),
        Err(e) => Err(e),
    }
}
