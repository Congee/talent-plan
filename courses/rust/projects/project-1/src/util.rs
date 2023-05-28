use once_cell::sync::Lazy;
use std::hash::Hash;
use std::hash::Hasher;

pub static NUM_THREADS: Lazy<usize> = Lazy::new(|| std::cmp::max(1, num_cpus::get_physical() - 1));

#[allow(dead_code)]
pub fn thread_of(key: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() % *&*NUM_THREADS as u64
}

pub trait ParseId {
    fn parse_id(&self) -> u64;
}

impl ParseId for std::path::Path {
    fn parse_id(&self) -> u64 {
        self.file_stem()
            .map(|x| x.to_str())
            .flatten()
            .expect("bad file name")
            .parse::<u64>()
            .expect("failed to parse number")
    }
}
