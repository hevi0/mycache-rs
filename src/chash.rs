use std::hash::{DefaultHasher, Hash, Hasher};

// Alternatively could implement a balanced tree structure
// for vnode lookup.
fn binary_search<T: std::cmp::PartialOrd>(v: &Vec<T>, q: T) -> usize {
    let mut start: usize = 0;
    let mut end = v.len();
    
    while start < end {
        let mid = (start + end) / 2;
        
        if v[mid] == q {
            return mid;
        }
        
        if q < v[mid] {
            end = mid;
        }
        else {
            start = mid + 1;
        }
    }
    
    start % v.len()
}

fn hash_bytes(s: u32, val: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    
    hasher.write_u32(s);
    for b in val {
        hasher.write_u8(*b);
    }
    hasher.finish()
}

fn hash_type<T: Hash>(obj: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

pub(crate) struct Chash {
    salts: Vec<u32>,
    vnodes: Vec<String>,
    hashes: Vec<u64>
}

impl Chash {
    pub fn new(salts: Vec<u32>) -> Self {
        Chash {
            salts: salts,
            vnodes: Vec::new(),
            hashes: Vec::new()
        }
    }

    pub fn add_node(&mut self, id: &str) {
        let hashes = self.salts.iter().map(|s| {
            hash_bytes(s.clone(), id.as_bytes())
        });

        for h in hashes {
            let mut i = 0;
            while i < self.hashes.len() && self.hashes[i] < h {
                i += 1;
            }
            self.hashes.insert(i, h);
            self.vnodes.insert(i, id.to_string());
        }
    }

    pub fn drop_node(&mut self, id: &str) {
        let hashes = self.salts.iter().map(|s| {
            hash_bytes(s.clone(), id.as_bytes())
        });

        for h in hashes {
            let idx = binary_search(&self.hashes, h);
            if self.vnodes[idx] == id {
                self.vnodes.remove(idx);
                self.hashes.remove(idx);
            }
        }
    }

    pub fn calculate_node<T: Hash>(&self, obj: &T) -> &str {
        let h = hash_type(obj);
        let idx = binary_search(&self.hashes, h);

        &self.vnodes[idx]
    }
    
}

