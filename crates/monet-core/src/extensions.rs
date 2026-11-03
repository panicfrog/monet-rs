//! A typed key-value store attached to requests and responses.
//!
//! Hooks use it to carry per-request state (e.g. retry counters, trace ids)
//! without changing the core types.

use std::any::{Any, TypeId};
use std::fmt;

/// A collection of at most one value per type.
#[derive(Default)]
pub struct Extensions {
    entries: Vec<(TypeId, Box<dyn Any + Send + Sync>)>,
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("types", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl Extensions {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a value for its type, returning the previous one if present.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<T> {
        let key = TypeId::of::<T>();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some((_, slot)) => {
                let old = std::mem::replace(slot, Box::new(value));
                old.take_any_downcast::<T>()
            }
            None => {
                self.entries.push((key, Box::new(value)));
                None
            }
        }
    }

    /// Returns a shared reference to the value of type `T`, if any.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        let key = TypeId::of::<T>();
        self.entries
            .iter()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v.downcast_ref::<T>())
    }

    /// Returns an exclusive reference to the value of type `T`, if any.
    pub fn get_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        let key = TypeId::of::<T>();
        self.entries
            .iter_mut()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v.downcast_mut::<T>())
    }

    /// Removes the value of type `T`, returning it if present.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<T> {
        let key = TypeId::of::<T>();
        let idx = self.entries.iter().position(|(k, _)| *k == key);
        idx.and_then(|i| self.entries.remove(i).1.take_any_downcast::<T>())
    }

    /// Returns `true` if no values are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of stored values.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

// Downcast-by-value helper: `Any::downcast` needs `Self: Sized`, which the
// boxed trait object loses; `Box::downcast` on the inner box does the job.
trait TakeAnyDowncast {
    fn take_any_downcast<T: Any + Send + Sync>(self) -> Option<T>;
}

impl TakeAnyDowncast for Box<dyn Any + Send + Sync> {
    fn take_any_downcast<T: Any + Send + Sync>(self) -> Option<T> {
        self.downcast::<T>().ok().map(|boxed| *boxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Marker(u32);

    #[test]
    fn insert_get_remove() {
        let mut ext = Extensions::new();
        assert!(ext.get::<Marker>().is_none());
        assert_eq!(ext.insert(Marker(1)), None);
        assert_eq!(ext.get::<Marker>(), Some(&Marker(1)));
        assert_eq!(ext.insert(Marker(2)), Some(Marker(1)));
        assert_eq!(ext.get_mut::<Marker>().map(|m| m.0), Some(2));
        assert_eq!(ext.len(), 1);
        assert_eq!(ext.remove::<Marker>(), Some(Marker(2)));
        assert!(ext.is_empty());
    }

    #[test]
    fn distinct_types_coexist() {
        #[derive(PartialEq, Debug)]
        struct A(&'static str);
        let mut ext = Extensions::new();
        ext.insert(A("a"));
        ext.insert(7_u32);
        assert_eq!(ext.get::<A>(), Some(&A("a")));
        assert_eq!(ext.get::<u32>(), Some(&7));
        assert_eq!(ext.len(), 2);
    }
}
