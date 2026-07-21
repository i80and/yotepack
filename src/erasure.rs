/// Erasure coder wrapper around reed-solomon-simd.
use crate::errors::StorageResult;
use crate::config::Config;
use crate::errors::StorageError;

/// Encodes K data shards into N total shards (K data + C parity).
#[derive(Debug, Clone)]
pub struct ErasureCoder {
    k: usize,
    c: usize,
    n: usize,
}

impl ErasureCoder {
    pub fn new(config: &Config) -> Self {
        let k = config.data_shards();
        let c = config.parity_shards();
        let n = k + c;
        Self { k, c, n }
    }

    pub fn data_shards(&self) -> usize {
        self.k
    }

    pub fn parity_shards(&self) -> usize {
        self.c
    }

    pub fn total_shards(&self) -> usize {
        self.n
    }

    /// Encode K data shards into N total shards (K data + C parity).
    /// Returns all N shards.
    pub fn encode(&self, data_shards: &[&[u8]]) -> StorageResult<Vec<Vec<u8>>> {
        assert_eq!(data_shards.len(), self.k, "Must provide exactly K data shards");
        for shard in data_shards.iter() {
            assert!(!shard.is_empty(), "Shard data must not be empty");
        }

        // All shards must be the same size for encode
        let shard_size = data_shards[0].len();
        for shard in data_shards.iter().skip(1) {
            if shard.len() != shard_size {
                return Err(StorageError::ErasureCoding(
                    "all data shards must be the same size".into(),
                ));
            }
        }

        // reed_solomon_simd::encode returns only the C recovery shards
        let parity_shards = reed_solomon_simd::encode(self.k, self.c, data_shards)?;

        // Combine original data shards + parity shards to get all N shards
        let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(self.n);
        for shard in data_shards {
            all_shards.push(shard.to_vec());
        }
        for shard in &parity_shards {
            all_shards.push(shard.clone());
        }

        Ok(all_shards)
    }

    /// Decode: given shard results (some may be missing), reconstruct all N shards.
    /// `present[i]` indicates whether shard i is available.
    pub fn decode(
        &self,
        shards: &[Option<Vec<u8>>],
        present: &[bool],
    ) -> StorageResult<Vec<Vec<u8>>> {
        assert_eq!(shards.len(), self.n, "Must provide N shard slots");
        assert_eq!(present.len(), self.n, "Must provide N presence flags");

        let shard_size = shards
            .iter()
            .filter_map(|s| s.as_ref().map(|v| v.len()))
            .next()
            .ok_or(StorageError::TooManyFailures)?;

        let surviving = present
            .iter()
            .zip(shards.iter())
            .filter(|(p, s)| **p && s.is_some())
            .count();

        if surviving < self.k {
            return Err(StorageError::TooManyFailures);
        }

        let mut decoder = reed_solomon_simd::ReedSolomonDecoder::new(
            self.k, self.c, shard_size,
        )?;

        for (i, (&p, shard)) in present.iter().zip(shards.iter()).enumerate() {
            if p && shard.is_some() {
                decoder.add_original_shard(i, shard.as_ref().unwrap().as_slice())?;
            }
        }

        let result = decoder.decode()?;

        let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(self.n);
        let restored_map: std::collections::HashMap<usize, &[u8]> =
            result.restored_original_iter().collect();

        for i in 0..self.n {
            if let Some(ref data) = shards[i] {
                all_shards.push(data.clone());
            } else if let Some(restored) = restored_map.get(&i) {
                all_shards.push(restored.to_vec());
            } else {
                return Err(StorageError::TooManyFailures);
            }
        }

        Ok(all_shards)
    }
}
