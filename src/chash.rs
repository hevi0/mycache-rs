use crate::common::*;
use std::fmt;
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

fn hash_type<T: Hash>(s: u32, obj: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u32(s);
    obj.hash(&mut hasher);
    hasher.finish()
}

pub(crate) struct Chash {
    pub(crate) salts: Vec<u32>,
    pub(crate) vnodes: Vec<IdType>,
    pub(crate) vnode_hashes: Vec<u64>
}

impl Chash {
    pub fn new(salts: Vec<u32>) -> Self {
        Chash {
            salts: salts,
            vnodes: Vec::new(),
            vnode_hashes: Vec::new()
        }
    }

    pub fn add_node(&mut self, id: &IdType) {
        let hashes = self.salts.iter().map(|s| {
            hash_bytes(s.clone(), &id.to_be_bytes())
        });

        for h in hashes {
            let mut i = 0;
            while i < self.vnode_hashes.len() && self.vnode_hashes[i] < h {
                i += 1;
            }
            if i < self.vnode_hashes.len() && h == self.vnode_hashes[i] {
                if self.vnodes[i] == id.to_owned() {
                    // already added this node in the past
                    continue;
                }
                panic!("Collision between hashes! (Should be rare)");
            }
            self.vnode_hashes.insert(i, h);
            self.vnodes.insert(i, id.to_owned());
        }
    }

    pub fn drop_node(&mut self, id: &IdType) {
        let hashes = self.salts.iter().map(|s| {
            hash_bytes(s.clone(), &id.to_be_bytes())
        });

        for h in hashes {
            let idx = binary_search(&self.vnode_hashes, h);
            if self.vnodes[idx] == *id {
                self.vnodes.remove(idx);
                self.vnode_hashes.remove(idx);
            }
        }
    }

    pub fn calculate_vnodes<T: Hash>(&self, obj: T) -> Vec<IdType> {
        let hashes = self.salts.iter().map(|s| {
            hash_type(s.clone(), &obj)
        });

        let vnode_ids = hashes.map(|h| {
            self.vnodes[binary_search(&self.vnode_hashes, h)]
        }).collect();

        vnode_ids
    }

}

impl fmt::Debug for Chash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", &self.vnodes)
    }
}

