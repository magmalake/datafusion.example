//! DataFusion filter expressions → the Iceberg filter DSL the Mojo scan reads.
//!
//! The DSL is JSON S-expressions — `["and", ["=", "region", "eu"], [">",
//! "amount", 2.5]]` — the grammar `iceberg.expressions.parse_filter` parses
//! (and iceberg-rs.mojo's before it).
//!
//! Everything pushed down is reported to DataFusion as `Inexact`, so
//! DataFusion still evaluates the predicate on the rows that come back. That
//! makes pruning an optimisation and never a source of wrong answers, and it
//! sets the one rule this module has to keep: **a translation may only ever
//! keep more rows than the expression would, never fewer**. An `AND` may drop
//! a conjunct it cannot express; an `OR` or a `NOT` cannot, so they translate
//! whole or not at all.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// The DSL text for `expr`, or `None` if no part of it can be pushed down.
pub fn to_dsl(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            match (to_dsl(left), to_dsl(right)) {
                (Some(l), Some(r)) => Some(format!(r#"["and",{l},{r}]"#)),
                // Dropping one side of an AND only widens it.
                (Some(x), None) | (None, Some(x)) => Some(x),
                (None, None) => None,
            }
        }
        _ => exact(expr),
    }
}

/// Whether `expr` can be pushed down at all; what `supports_filters_pushdown`
/// asks.
pub fn supported(expr: &Expr) -> bool {
    to_dsl(expr).is_some()
}

/// All of `filters` as one DSL expression, empty for none.
pub fn conjunction(filters: &[Expr]) -> String {
    let parts: Vec<String> = filters.iter().filter_map(to_dsl).collect();
    match parts.len() {
        0 => String::new(),
        1 => parts.into_iter().next().unwrap(),
        _ => parts
            .into_iter()
            .reduce(|acc, p| format!(r#"["and",{acc},{p}]"#))
            .unwrap(),
    }
}

/// A translation that keeps exactly the rows `expr` keeps, or `None`.
fn exact(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                let name = if *op == Operator::And { "and" } else { "or" };
                Some(format!(r#"["{name}",{},{}]"#, exact(left)?, exact(right)?))
            }
            _ => comparison(left, *op, right),
        },
        Expr::Not(inner) => Some(format!(r#"["not",{}]"#, exact(inner)?)),
        Expr::IsNull(e) => Some(format!(r#"["is-null",{}]"#, column(e)?)),
        Expr::IsNotNull(e) => Some(format!(r#"["not-null",{}]"#, column(e)?)),
        Expr::InList(list) => {
            let col = column(&list.expr)?;
            let lits = list
                .list
                .iter()
                .map(|e| match e {
                    Expr::Literal(v, _) => literal(v),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let op = if list.negated { "not-in" } else { "in" };
            Some(format!(r#"["{op}",{col},[{}]]"#, lits.join(",")))
        }
        _ => None,
    }
}

/// `column <op> literal`, in either order.
fn comparison(left: &Expr, op: Operator, right: &Expr) -> Option<String> {
    let (col, lit, op) = match (left, right) {
        (Expr::Column(_), Expr::Literal(v, _)) => (column(left)?, v, op),
        (Expr::Literal(v, _), Expr::Column(_)) => (column(right)?, v, op.swap()?),
        _ => return None,
    };
    let name = match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    };
    // A NULL literal makes `literal` return None: `x = NULL` is never true,
    // and DataFusion folds it away before it gets here anyway.
    Some(format!(r#"["{name}",{col},{}]"#, literal(lit)?))
}

fn column(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(json_string(&c.name)),
        _ => None,
    }
}

/// One literal as the DSL spells it.
///
/// Dates and timestamps go as the integer Iceberg stores — days, and
/// microseconds — which the Mojo side takes as-is on those columns. Anything
/// that would lose precision getting there (nanoseconds that are not whole
/// microseconds, non-finite floats) is not pushed down.
fn literal(v: &ScalarValue) -> Option<String> {
    use ScalarValue::*;
    Some(match v {
        Boolean(Some(b)) => b.to_string(),
        Int8(Some(x)) => x.to_string(),
        Int16(Some(x)) => x.to_string(),
        Int32(Some(x)) => x.to_string(),
        Int64(Some(x)) => x.to_string(),
        UInt8(Some(x)) => x.to_string(),
        UInt16(Some(x)) => x.to_string(),
        UInt32(Some(x)) => x.to_string(),
        UInt64(Some(x)) => x.to_string(),
        Float32(Some(x)) if x.is_finite() => format!("{x:?}"),
        Float64(Some(x)) if x.is_finite() => format!("{x:?}"),
        Utf8(Some(s)) | LargeUtf8(Some(s)) | Utf8View(Some(s)) => json_string(s),
        Date32(Some(d)) => d.to_string(),
        TimestampSecond(Some(t), _) => t.checked_mul(1_000_000)?.to_string(),
        TimestampMillisecond(Some(t), _) => t.checked_mul(1_000)?.to_string(),
        TimestampMicrosecond(Some(t), _) => t.to_string(),
        TimestampNanosecond(Some(t), _) if t % 1_000 == 0 => (t / 1_000).to_string(),
        _ => return None,
    })
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, not};

    #[test]
    fn comparisons_either_way_round() {
        assert_eq!(to_dsl(&col("id").gt(lit(3i64))).unwrap(), r#"[">","id",3]"#);
        assert_eq!(to_dsl(&lit(3i64).gt(col("id"))).unwrap(), r#"["<","id",3]"#);
        assert_eq!(
            to_dsl(&col("region").eq(lit("e\"u"))).unwrap(),
            r#"["=","region","e\"u"]"#
        );
    }

    #[test]
    fn and_drops_what_it_cannot_say_but_or_and_not_do_not() {
        let unsupported = col("id").like(lit("1%"));
        let ok = col("id").eq(lit(1i64));
        assert_eq!(
            to_dsl(&ok.clone().and(unsupported.clone())).unwrap(),
            r#"["=","id",1]"#
        );
        assert!(to_dsl(&ok.clone().or(unsupported.clone())).is_none());
        assert!(to_dsl(&not(ok.clone().and(unsupported))).is_none());
        assert_eq!(to_dsl(&not(ok)).unwrap(), r#"["not",["=","id",1]]"#);
    }

    #[test]
    fn lists_nulls_and_timestamps() {
        assert_eq!(
            to_dsl(&col("id").in_list(vec![lit(1i64), lit(4i64)], false)).unwrap(),
            r#"["in","id",[1,4]]"#
        );
        assert_eq!(
            to_dsl(&col("amount").is_null()).unwrap(),
            r#"["is-null","amount"]"#
        );
        let ts = Expr::Literal(ScalarValue::TimestampNanosecond(Some(1_500), None), None);
        assert!(to_dsl(&col("ts").gt(ts)).is_none());
        let ts = Expr::Literal(ScalarValue::TimestampMillisecond(Some(2), None), None);
        assert_eq!(to_dsl(&col("ts").gt(ts)).unwrap(), r#"[">","ts",2000]"#);
    }

    #[test]
    fn filters_combine_into_one_and() {
        let f = [col("id").gt(lit(1i64)), col("ok").eq(lit(true))];
        assert_eq!(conjunction(&f), r#"["and",[">","id",1],["=","ok",true]]"#);
        assert_eq!(conjunction(&[]), "");
    }
}
