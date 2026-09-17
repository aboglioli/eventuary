use std::num::NonZeroUsize;

use eventuary_core::{Error, Result};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct BatchLimits {
    max_items: NonZeroUsize,
    max_weight: NonZeroUsize,
}

impl BatchLimits {
    pub(crate) const fn new(max_items: NonZeroUsize, max_weight: NonZeroUsize) -> Self {
        Self {
            max_items,
            max_weight,
        }
    }

    pub(crate) fn max_items(&self) -> usize {
        self.max_items.get()
    }

    pub(crate) fn max_weight(&self) -> usize {
        self.max_weight.get()
    }
}

#[must_use = "a sealed batch must be sent; dropping it discards its items"]
#[derive(Debug)]
pub(crate) struct Batch<T> {
    items: Vec<T>,
}

impl<T> Batch<T> {
    pub(crate) fn into_items(self) -> Vec<T> {
        self.items
    }
}

pub(crate) struct Batcher<T> {
    limits: BatchLimits,
    items: Vec<T>,
    weight: usize,
}

impl<T> Batcher<T> {
    pub(crate) fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            items: Vec::new(),
            weight: 0,
        }
    }

    /// `build` receives the item's index inside the batch it lands in, which is
    /// zero whenever this push seals the previous one.
    pub(crate) fn push(
        &mut self,
        weight: usize,
        build: impl FnOnce(usize) -> Result<T>,
    ) -> Result<Option<Batch<T>>> {
        if weight > self.limits.max_weight() {
            return Err(Error::InvalidPayload(format!(
                "item weighing {weight} cannot fit a batch limited to {}",
                self.limits.max_weight()
            )));
        }
        let seals = self.items.len() == self.limits.max_items()
            || self.weight + weight > self.limits.max_weight();
        let item = build(if seals { 0 } else { self.items.len() })?;
        let sealed = if seals { Some(self.seal()) } else { None };
        self.items.push(item);
        self.weight += weight;
        Ok(sealed)
    }

    pub(crate) fn finish(mut self) -> Option<Batch<T>> {
        if self.items.is_empty() {
            return None;
        }
        Some(self.seal())
    }

    fn seal(&mut self) -> Batch<T> {
        self.weight = 0;
        Batch {
            items: std::mem::take(&mut self.items),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_items: usize, max_weight: usize) -> BatchLimits {
        BatchLimits::new(
            NonZeroUsize::new(max_items).unwrap(),
            NonZeroUsize::new(max_weight).unwrap(),
        )
    }

    fn seal_all(limits: BatchLimits, count: usize, weight: usize) -> Vec<Vec<usize>> {
        let mut batcher = Batcher::new(limits);
        let mut sealed = Vec::new();
        for i in 0..count {
            if let Some(batch) = batcher.push(weight, |_| Ok(i)).unwrap() {
                sealed.push(batch.into_items());
            }
        }
        sealed.extend(batcher.finish().map(Batch::into_items));
        sealed
    }

    #[test]
    fn pushing_without_checking_never_exceeds_the_limits() {
        for (max_items, max_weight, weight) in [(10, 256, 16), (10, 256, 256), (1, 8, 1), (3, 5, 2)]
        {
            let bounds = limits(max_items, max_weight);
            for count in [0, 1, 2, 9, 10, 11, 25, 101] {
                let batches = seal_all(bounds, count, weight);
                for batch in &batches {
                    assert!(
                        batch.len() <= max_items && batch.len() * weight <= max_weight,
                        "{count} items of {weight} under {max_items}/{max_weight} \
                         produced a batch of {}",
                        batch.len()
                    );
                    assert!(!batch.is_empty());
                }
                let total: usize = batches.iter().map(Vec::len).sum();
                assert_eq!(total, count, "every pushed item must reach a batch");
            }
        }
    }

    #[test]
    fn items_are_numbered_against_the_batch_they_land_in() {
        let mut batcher: Batcher<usize> = Batcher::new(limits(2, 1_000));
        let mut sealed = Vec::new();
        for _ in 0..5 {
            if let Some(batch) = batcher.push(1, Ok).unwrap() {
                sealed.push(batch.into_items());
            }
        }
        sealed.extend(batcher.finish().map(Batch::into_items));

        assert_eq!(sealed, vec![vec![0, 1], vec![0, 1], vec![0]]);
    }

    #[test]
    fn weight_alone_can_seal_a_batch_below_the_item_limit() {
        assert_eq!(seal_all(limits(10, 4), 5, 2).len(), 3);
    }

    #[test]
    fn finish_on_an_empty_batcher_yields_nothing() {
        let batcher: Batcher<usize> = Batcher::new(limits(10, 10));
        assert!(batcher.finish().is_none());
    }

    #[test]
    fn an_item_heavier_than_the_whole_batch_is_rejected() {
        let mut batcher: Batcher<usize> = Batcher::new(limits(10, 64));

        let err = batcher.push(65, |_| Ok(0)).unwrap_err();

        assert!(matches!(err, Error::InvalidPayload(_)), "{err}");
        assert!(
            batcher.finish().is_none(),
            "a rejected item must not be kept"
        );
    }

    #[test]
    fn a_failing_builder_leaves_the_batcher_untouched() {
        let mut batcher: Batcher<usize> = Batcher::new(limits(2, 1_000));
        batcher.push(1, |_| Ok(7)).unwrap();

        let err = batcher
            .push(1, |_| Err(Error::Store("no entry".to_owned())))
            .unwrap_err();

        assert!(matches!(err, Error::Store(_)), "{err}");
        let remaining = batcher.finish().expect("the earlier item survives");
        assert_eq!(remaining.into_items(), vec![7]);
    }
}
