//! Ref membership bitsets shared by the backends, generic over the backend's OID type.

use std::collections::HashMap;
use std::hash::Hash;

pub(super) enum RefBits {
    Inline(u64),
    Words(Box<[u64]>),
}

impl RefBits {
    pub(super) fn new(word_count: usize) -> Self {
        if word_count <= 1 {
            RefBits::Inline(0)
        } else {
            RefBits::Words(vec![0u64; word_count].into_boxed_slice())
        }
    }

    fn as_slice(&self) -> &[u64] {
        match self {
            RefBits::Inline(word) => std::slice::from_ref(word),
            RefBits::Words(words) => words,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u64] {
        match self {
            RefBits::Inline(word) => std::slice::from_mut(word),
            RefBits::Words(words) => words,
        }
    }

    pub(super) fn set(&mut self, bit: usize) {
        self.as_mut_slice()[bit / 64] |= 1u64 << (bit % 64);
    }

    pub(super) fn or_assign(&mut self, other: &RefBits) {
        for (a, b) in self.as_mut_slice().iter_mut().zip(other.as_slice()) {
            *a |= b;
        }
    }
}

fn iter_ones(words: &[u64]) -> impl Iterator<Item = usize> + '_ {
    words.iter().enumerate().flat_map(|(wi, &word)| {
        let mut remaining = word;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let bit = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            Some(wi * 64 + bit)
        })
    })
}

pub(super) struct ContainedIndex<K> {
    pub(super) branch_names: Vec<String>,
    pub(super) tag_names: Vec<String>,
    pub(super) branch_words: usize,
    pub(super) bits: HashMap<K, RefBits>,
}

impl<K: Eq + Hash> ContainedIndex<K> {
    pub(super) fn empty() -> Self {
        ContainedIndex {
            branch_names: Vec::new(),
            tag_names: Vec::new(),
            branch_words: 0,
            bits: HashMap::new(),
        }
    }

    pub(super) fn branches_of(&self, oid: &K) -> impl Iterator<Item = &str> {
        let branch_words = self.branch_words;
        self.bits.get(oid).into_iter().flat_map(move |bits| {
            iter_ones(&bits.as_slice()[..branch_words]).map(|i| self.branch_names[i].as_str())
        })
    }

    pub(super) fn tags_of(&self, oid: &K) -> impl Iterator<Item = &str> {
        let branch_words = self.branch_words;
        self.bits.get(oid).into_iter().flat_map(move |bits| {
            iter_ones(&bits.as_slice()[branch_words..]).map(|i| self.tag_names[i].as_str())
        })
    }
}
