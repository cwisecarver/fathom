//! The aggregate UDFs: `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`, `ANY_VALUE`,
//! `BIT_AND`, `BIT_OR`, `BIT_XOR`.
//!
//! ## Two deliberate divergences from Django, both in the safe direction
//!
//! Django implements these as `ListAggregate(list)` with `step = list.append` and a `finalize`
//! from the `statistics` module. That has two consequences we do **not** reproduce:
//!
//! 1. **NULLs.** SQLite calls the step function for every row including NULL ones, so Django's
//!    list accumulates `None` and `statistics.pstdev` then raises `TypeError`. Every other SQL
//!    engine — and the SQL standard — ignores NULLs in aggregates. We skip them. Reproducing the
//!    crash faithfully would mean a `STDDEV_POP` over a nullable column errors the tenant's query.
//! 2. **Degenerate groups.** `statistics.variance` raises `StatisticsError` on fewer than two
//!    values. We return NULL, matching PostgreSQL's `var_samp`, so a one-row group is not a query
//!    failure.
//!
//! ## Memory
//!
//! Django keeps every value in a Python list. We use Welford's online algorithm instead: O(1)
//! memory and numerically stabler than the naive sum-of-squares form. That matters here because a
//! shard's aggregate runs inside a tenant query that `:query_max_rows` bounds on the *result*,
//! not on rows scanned — an aggregate over a large table would otherwise buffer the whole column.

/// Online mean/variance accumulator (Welford). `m2` is the running sum of squared deviations.
#[derive(Debug, Default, Clone)]
pub struct Variance {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Variance {
    pub fn step(&mut self, x: f64) {
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// Population variance — `statistics.pvariance`. NULL on an empty group.
    pub fn var_pop(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.m2 / self.count as f64)
        }
    }

    /// Sample variance — `statistics.variance`. NULL on fewer than two values.
    pub fn var_samp(&self) -> Option<f64> {
        if self.count < 2 {
            None
        } else {
            Some(self.m2 / (self.count - 1) as f64)
        }
    }

    pub fn stddev_pop(&self) -> Option<f64> {
        self.var_pop().map(f64::sqrt)
    }

    pub fn stddev_samp(&self) -> Option<f64> {
        self.var_samp().map(f64::sqrt)
    }
}

/// Bitwise reduction over the non-NULL integers in a group. NULL on an empty group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    And,
    Or,
    Xor,
}

#[derive(Debug, Default, Clone)]
pub struct BitAgg {
    acc: Option<i64>,
}

impl BitAgg {
    pub fn step(&mut self, op: BitOp, x: i64) {
        self.acc = Some(match (self.acc, op) {
            (None, _) => x,
            (Some(a), BitOp::And) => a & x,
            (Some(a), BitOp::Or) => a | x,
            (Some(a), BitOp::Xor) => a ^ x,
        });
    }

    pub fn value(&self) -> Option<i64> {
        self.acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variance_of(xs: &[f64]) -> Variance {
        let mut v = Variance::default();
        for x in xs {
            v.step(*x);
        }
        v
    }

    #[test]
    fn variance_matches_python_statistics() {
        // statistics.pvariance([1, 3]) == 1.0 ; statistics.variance([1, 3]) == 2.0
        let v = variance_of(&[1.0, 3.0]);
        assert!((v.var_pop().unwrap() - 1.0).abs() < 1e-12);
        assert!((v.var_samp().unwrap() - 2.0).abs() < 1e-12);
        assert!((v.stddev_pop().unwrap() - 1.0).abs() < 1e-12);
        assert!((v.stddev_samp().unwrap() - 2.0_f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn variance_of_a_known_sample() {
        // statistics.pvariance([2, 4, 4, 4, 5, 5, 7, 9]) == 4.0, pstdev == 2.0
        let v = variance_of(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!((v.var_pop().unwrap() - 4.0).abs() < 1e-12);
        assert!((v.stddev_pop().unwrap() - 2.0).abs() < 1e-12);
        // statistics.variance(...) == 32/7
        assert!((v.var_samp().unwrap() - 32.0 / 7.0).abs() < 1e-12);
    }

    #[test]
    fn degenerate_groups_are_null_not_an_error() {
        let empty = variance_of(&[]);
        assert_eq!(empty.var_pop(), None);
        assert_eq!(empty.var_samp(), None);

        // One value: population variance is 0, sample variance is undefined.
        let one = variance_of(&[42.0]);
        assert_eq!(one.var_pop(), Some(0.0));
        assert_eq!(one.var_samp(), None, "sample variance needs two values");
        assert_eq!(one.stddev_samp(), None);
    }

    #[test]
    fn variance_is_numerically_stable_on_a_large_offset() {
        // The naive sum-of-squares form catastrophically cancels here and can even return a
        // negative variance. Welford holds. Values are 1e9 + {1,2,3,4,5}: variance is the same as
        // for {1,2,3,4,5}, i.e. pvariance == 2.0.
        let xs: Vec<f64> = (1..=5).map(|i| 1e9 + i as f64).collect();
        let v = variance_of(&xs);
        assert!(v.var_pop().unwrap() >= 0.0);
        assert!(
            (v.var_pop().unwrap() - 2.0).abs() < 1e-6,
            "got {:?}",
            v.var_pop()
        );
    }

    #[test]
    fn constant_input_has_zero_variance() {
        let v = variance_of(&[7.0; 100]);
        assert!(v.var_pop().unwrap().abs() < 1e-12);
        assert!(v.var_samp().unwrap().abs() < 1e-12);
    }

    #[test]
    fn bit_aggregates_reduce() {
        let run = |op: BitOp, xs: &[i64]| {
            let mut a = BitAgg::default();
            for x in xs {
                a.step(op, *x);
            }
            a.value()
        };
        assert_eq!(run(BitOp::And, &[0b1100, 0b1010]), Some(0b1000));
        assert_eq!(run(BitOp::Or, &[0b1100, 0b1010]), Some(0b1110));
        assert_eq!(run(BitOp::Xor, &[0b1100, 0b1010]), Some(0b0110));
        assert_eq!(run(BitOp::Xor, &[1, 3]), Some(2));
        // A single value reduces to itself, whatever the operator.
        assert_eq!(run(BitOp::And, &[5]), Some(5));
        assert_eq!(run(BitOp::Or, &[5]), Some(5));
        assert_eq!(run(BitOp::Xor, &[5]), Some(5));
        // An empty group is NULL, not 0 or -1 — 0 would be a plausible-looking wrong answer for
        // OR/XOR and -1 for AND.
        assert_eq!(run(BitOp::And, &[]), None);
        assert_eq!(run(BitOp::Or, &[]), None);
        assert_eq!(run(BitOp::Xor, &[]), None);
    }

    #[test]
    fn xor_is_its_own_inverse() {
        let mut a = BitAgg::default();
        for x in [9_i64, 4, 9] {
            a.step(BitOp::Xor, x);
        }
        assert_eq!(a.value(), Some(4));
    }
}
