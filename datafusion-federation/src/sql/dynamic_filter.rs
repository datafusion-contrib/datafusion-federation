//! Translation of runtime filters (e.g. `DynamicFilterPhysicalExpr` produced by joins and TopK)
//! into a logical predicate that can be unparsed into the SQL sent to the remote.
//!
//! # Use case
//!
//! Imagine the following use case:
//!
//! Two remote data sources, `orders`:
//!
//! ```csv
//! order_id,customer_id,order_name
//! 1,2,"order 1"
//! 2,1,"order 2"
//! 3,2,"order 3"
//! 4,3,"order 4"
//! ```
//!
//! and `customers`:
//!
//! ```csv
//! customer_id,customer_name,customer_type
//! 1,"customer 1","normal"
//! 2,"customer 2","bronze"
//! 3,"customer 3","silver"
//! 4,"customer 4","gold"
//! ```
//!
//! And you want to get all the details about customers with type `bronze` and their orders:
//!
//! ```sql
//! SELECT c.*, o.* FROM customers c
//! INNER JOIN orders o ON o.customer_id = c.customer_id
//! WHERE c.customer_type = 'bronze'
//! ```
//!
//! Without dynamic filters DataFusion Federation will make a scan over the whole `orders` table
//! and then filter out the not needed rows in memory when doing the join by customer id.
//! With dynamic filters only the orders for the customers with type `bronze` will be returned.
//!
//! # Implementation details
//!
//! The filters pushed down into a `VirtualExecutionPlan` are physical expressions over its output
//! columns. For example, after a hash join has built its hash table from
//! `SELECT * FROM customers WHERE customer_type = 'bronze'`, the filter it pushes to the
//! `orders` scan may read:
//!
//! ```text
//! DynamicFilter [ customer_id@1 >= 2 AND customer_id@1 <= 2 AND customer_id@1 IN (SET) ([2]) ]
//! ```
//!
//! [`filters_to_predicate`] turns this into the logical expression
//!
//! ```text
//! federated_filtered.customer_id >= 2 AND federated_filtered.customer_id <= 2
//!     AND federated_filtered.customer_id IN (2)
//! ```
//!
//! which `VirtualExecutionPlan` adds as a `WHERE` clause on top of the federated query.
//!
//! Not every physical expression has a SQL equivalent, so the translation is a *relaxation* (see
//! [`relax`]): untranslatable parts are widened until the predicate can be expressed, and the
//! remote may return more rows than the filters would keep, but never fewer.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::Schema,
    common::{Column, DFSchema, ScalarValue},
    error::Result,
    logical_expr::{
        expr::{Cast, InList, Like, TryCast},
        lit, BinaryExpr, Expr, Operator,
    },
    physical_expr::expressions::{self as phys},
    physical_expr_common::physical_expr::snapshot_physical_expr,
    physical_plan::PhysicalExpr,
};

/// Converts `filters`, whose columns refer to `input`, into a single predicate over `output`, or
/// `None` if nothing could be translated. `output` must have the same fields in the same order as
/// `input`, though their names may differ (e.g. when deduplicated by a derived table).
///
/// Each filter is translated with [`relax`], and the translated filters are combined with `AND`.
/// Filters that relax to `true` are left out, so the result is `None` if there are no filters or
/// none of them constrains anything.
///
/// The predicate is a relaxation: every row for which all `filters` are true also satisfies the
/// predicate, but not necessarily the other way around. Parts that cannot be expressed in SQL
/// (such as hash table lookups or the hash routing of partitioned joins) are widened to `true`.
/// This is sound because the remote only gets to drop rows the filters would drop anyway, while
/// DataFusion still evaluates the original filters locally.
///
/// Dynamic filters are snapshotted, so the result reflects their state at the time of the call:
/// an empty `DynamicFilter` (still `true`) translates to nothing, and the same filter translates
/// to its current expression once the join or TopK producing it has updated it.
///
/// Columns are resolved by index, and are only translated if their name matches the field of
/// `input` at that index; columns out of sync with `input` are treated as untranslatable.
///
/// # Example
///
/// With `input` = `[a, a]` (e.g. both sides of a self join), `output` = the derived table
/// `federated_filtered` with columns `[a, "a:1"]`, and the filters:
///
/// ```text
/// [ a@1 > 5,  DynamicFilter [ a@0 >= 1 AND a@0 <= 9 AND hash_lookup(a@0) ] ]
/// ```
///
/// the result is
///
/// ```text
/// Some(federated_filtered."a:1" > 5 AND (federated_filtered.a >= 1 AND federated_filtered.a <= 9))
/// ```
///
/// The `hash_lookup` has no SQL equivalent and is dropped from its conjunction, and `a@1`
/// resolves to the deduplicated `"a:1"` column of `output`.
pub(crate) fn filters_to_predicate(
    filters: &[Arc<dyn PhysicalExpr>],
    input: &Schema,
    output: &DFSchema,
) -> Result<Option<Expr>> {
    let columns: Vec<_> = input
        .fields()
        .iter()
        .zip(output.iter())
        .map(|(field, column)| (field.name().as_str(), Column::from(column)))
        .collect();

    let mut predicates = vec![];
    for filter in filters {
        let filter = snapshot_physical_expr(Arc::clone(filter))?;
        if let Some(predicate) = relax(&filter, &columns) {
            predicates.push(predicate);
        }
    }
    Ok(predicates.into_iter().reduce(Expr::and))
}

/// The columns the filters refer to, by index: their name in the filters and the column to use in
/// the predicate.
///
/// For a physical `Column { name: "a", index: 1 }`, entry `1` must be `("a", <column>)`, and the
/// column is translated to `Expr::Column(<column>)`. For example, `[("a", t.a), ("a", t."a:1")]`
/// maps `a@0` to `t.a` and `a@1` to `t."a:1"`.
type Columns<'a> = [(&'a str, Column)];

/// Returns a predicate implied by `expr`, where `None` stands for `true` (no constraint).
///
/// "Implied" means that every row for which `expr` is true also satisfies the returned predicate,
/// so filtering with it never drops a row that `expr` keeps. `expr` is widened where needed:
///
/// - `true` relaxes to `None`; any other literal (e.g. `false` from a join with an empty build
///   side) is kept as is.
/// - `l AND r` relaxes both sides and drops the ones that relax to `true`.
/// - `l OR r` relaxes both sides, and is `true` if either side is.
/// - `CASE [operand] WHEN .. THEN t1 WHEN .. THEN t2 .. [ELSE e] END` relaxes to `t1 OR t2 OR .. OR
///   e`: whichever branch is picked must be true for the `CASE` to be true. The operand and `WHEN`
///   conditions, such as the `hash_repartition(key) % n` routing of partitioned joins, are
///   dropped, as are `false` branches. A `CASE` whose branches are all `false` relaxes to `false`.
/// - Everything else must be translated as a whole by [`exact`], or relaxes to `true`.
///
/// Relaxing only happens at the top and through `AND`, `OR` and `CASE`. Below other operators,
/// widening would be wrong: e.g. widening `x` in `NOT x` would narrow down the `NOT`.
///
/// # Examples
///
/// ```text
/// a@0 > 1 AND hash_lookup(a@0)             =>  Some(a > 1)
/// a@0 > 1 OR hash_lookup(a@0)              =>  None
/// NOT (a@0 > 1 AND hash_lookup(a@0))       =>  None
/// CASE hash_repartition(a@0) % 2
///     WHEN 0 THEN a@0 IN (1, 3)
///     WHEN 1 THEN a@0 IN (2)
///     ELSE false
/// END                                      =>  Some(a IN (1, 3) OR a IN (2))
/// CASE .. WHEN 0 THEN a@0 = 1 ELSE true    =>  None
/// false                                    =>  Some(false)
/// ```
fn relax(expr: &Arc<dyn PhysicalExpr>, columns: &Columns) -> Option<Expr> {
    if let Some(literal) = expr.downcast_ref::<phys::Literal>() {
        return match literal.value() {
            ScalarValue::Boolean(Some(true)) => None,
            value => Some(lit(value.clone())),
        };
    }

    if let Some(binary) = expr.downcast_ref::<phys::BinaryExpr>() {
        match binary.op() {
            Operator::And => {
                return match (
                    relax(binary.left(), columns),
                    relax(binary.right(), columns),
                ) {
                    (Some(left), Some(right)) => Some(left.and(right)),
                    (left, right) => left.or(right),
                };
            }
            Operator::Or => {
                let left = relax(binary.left(), columns)?;
                let right = relax(binary.right(), columns)?;
                return Some(left.or(right));
            }
            _ => {}
        }
    }

    // A CASE can only be true if the branch it picked is true, so it implies the disjunction of
    // all branches. The routing operand (e.g. `hash_repartition(key) % n`) is dropped. A missing
    // ELSE yields NULL, which never passes a filter.
    if let Some(case) = expr.downcast_ref::<phys::CaseExpr>() {
        let mut branches = vec![];
        let thens = case.when_then_expr().iter().map(|(_, then)| then);
        for branch in thens.chain(case.else_expr()) {
            // A branch widened to `true` makes the whole disjunction `true`.
            let branch = relax(branch, columns)?;
            // `false` branches (e.g. empty join partitions) add nothing to the disjunction.
            if branch != lit(false) {
                branches.push(branch);
            }
        }
        return Some(branches.into_iter().reduce(Expr::or).unwrap_or(lit(false)));
    }

    exact(expr, columns)
}

/// Returns a logical expression equivalent to `expr`, or `None` if it cannot be translated.
///
/// Unlike [`relax`], nothing is widened: `expr` is translated node by node, and if any node is
/// not supported, the whole expression is `None`. Supported are:
///
/// - columns, resolved through `columns` (see [`Columns`]), and literals;
/// - binary expressions with any operator, e.g. comparisons, `AND`/`OR`, arithmetic;
/// - `IN` / `NOT IN` lists, `CAST`, `TRY_CAST`, `[NOT] [I]LIKE`, `NOT`, `IS NULL`,
///   `IS NOT NULL` and negation.
///
/// Anything else, e.g. `CASE`, scalar functions or a hash table lookup, is not supported.
///
/// # Examples
///
/// With `columns` = `[("a", t.a), ("s", t.s)]`:
///
/// ```text
/// CAST(a@0 AS Int32) = 1          =>  Some(CAST(t.a AS Int32) = 1)
/// s@1 NOT ILIKE 'x%'              =>  Some(t.s NOT ILIKE 'x%')
/// NOT a@0 IN (1, 2)               =>  Some(NOT t.a IN (1, 2))
/// a@0 > 1 AND hash_lookup(a@0)    =>  None
/// b@0 > 1                         =>  None  (the column at index 0 is `a`)
/// ```
fn exact(expr: &Arc<dyn PhysicalExpr>, columns: &Columns) -> Option<Expr> {
    if let Some(column) = expr.downcast_ref::<phys::Column>() {
        let (name, output) = columns.get(column.index())?;
        if *name != column.name() {
            return None;
        }
        return Some(Expr::Column(output.clone()));
    }

    if let Some(literal) = expr.downcast_ref::<phys::Literal>() {
        return Some(lit(literal.value().clone()));
    }

    if let Some(binary) = expr.downcast_ref::<phys::BinaryExpr>() {
        return Some(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(exact(binary.left(), columns)?),
            *binary.op(),
            Box::new(exact(binary.right(), columns)?),
        )));
    }

    if let Some(in_list) = expr.downcast_ref::<phys::InListExpr>() {
        let list = in_list
            .list()
            .iter()
            .map(|item| exact(item, columns))
            .collect::<Option<Vec<_>>>()?;
        return Some(Expr::InList(InList::new(
            Box::new(exact(in_list.expr(), columns)?),
            list,
            in_list.negated(),
        )));
    }

    if let Some(cast) = expr.downcast_ref::<phys::CastExpr>() {
        return Some(Expr::Cast(Cast::new(
            Box::new(exact(cast.expr(), columns)?),
            cast.cast_type().clone(),
        )));
    }

    if let Some(cast) = expr.downcast_ref::<phys::TryCastExpr>() {
        return Some(Expr::TryCast(TryCast::new(
            Box::new(exact(cast.expr(), columns)?),
            cast.cast_type().clone(),
        )));
    }

    if let Some(like) = expr.downcast_ref::<phys::LikeExpr>() {
        return Some(Expr::Like(Like::new(
            like.negated(),
            Box::new(exact(like.expr(), columns)?),
            Box::new(exact(like.pattern(), columns)?),
            None,
            like.case_insensitive(),
        )));
    }

    if let Some(not) = expr.downcast_ref::<phys::NotExpr>() {
        return Some(Expr::Not(Box::new(exact(not.arg(), columns)?)));
    }

    if let Some(is_null) = expr.downcast_ref::<phys::IsNullExpr>() {
        return Some(Expr::IsNull(Box::new(exact(is_null.arg(), columns)?)));
    }

    if let Some(is_not_null) = expr.downcast_ref::<phys::IsNotNullExpr>() {
        return Some(Expr::IsNotNull(Box::new(exact(
            is_not_null.arg(),
            columns,
        )?)));
    }

    if let Some(negative) = expr.downcast_ref::<phys::NegativeExpr>() {
        return Some(Expr::Negative(Box::new(exact(negative.arg(), columns)?)));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        logical_expr::col,
        physical_expr::expressions::{
            binary, col as phys_col, in_list, lit as phys_lit, DynamicFilterPhysicalExpr,
        },
    };

    fn schemas() -> (Schema, DFSchema) {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]);
        let df_schema = DFSchema::try_from_qualified_schema("t", &schema).unwrap();
        (schema, df_schema)
    }

    fn translate(filters: &[Arc<dyn PhysicalExpr>]) -> Option<Expr> {
        let (schema, df_schema) = schemas();
        filters_to_predicate(filters, &schema, &df_schema).unwrap()
    }

    #[test]
    fn translates_comparisons_and_in_lists() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        let range = binary(
            binary(Arc::clone(&a), Operator::GtEq, phys_lit(1i64), &schema).unwrap(),
            Operator::And,
            binary(Arc::clone(&a), Operator::LtEq, phys_lit(3i64), &schema).unwrap(),
            &schema,
        )
        .unwrap();
        let members = in_list(a, vec![phys_lit(1i64), phys_lit(3i64)], &false, &schema).unwrap();

        assert_eq!(
            translate(&[range, members]),
            Some(
                col("t.a")
                    .gt_eq(lit(1i64))
                    .and(col("t.a").lt_eq(lit(3i64)))
                    .and(col("t.a").in_list(vec![lit(1i64), lit(3i64)], false))
            )
        );
    }

    #[test]
    fn widens_untranslatable_conjuncts_to_true() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        // An unknown expression standing in for e.g. a hash table lookup.
        let opaque: Arc<dyn PhysicalExpr> = Arc::new(phys::NoOp::new());
        let filter = binary(
            binary(a, Operator::Gt, phys_lit(1i64), &schema).unwrap(),
            Operator::And,
            Arc::clone(&opaque),
            &schema,
        )
        .unwrap();

        assert_eq!(translate(&[filter]), Some(col("t.a").gt(lit(1i64))));
        assert_eq!(translate(&[opaque]), None);
    }

    #[test]
    fn widens_disjunctions_with_untranslatable_side_to_true() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        let filter = binary(
            binary(a, Operator::Gt, phys_lit(1i64), &schema).unwrap(),
            Operator::Or,
            Arc::new(phys::NoOp::new()),
            &schema,
        )
        .unwrap();

        assert_eq!(translate(&[filter]), None);
    }

    #[test]
    fn relaxes_case_to_disjunction_of_branches() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        let b = phys_col("b", &schema).unwrap();
        let case = phys::CaseExpr::try_new(
            Some(binary(Arc::clone(&b), Operator::Modulo, phys_lit(2i64), &schema).unwrap()),
            vec![
                (
                    phys_lit(0i64),
                    binary(Arc::clone(&a), Operator::Eq, phys_lit(1i64), &schema).unwrap(),
                ),
                (
                    phys_lit(1i64),
                    binary(a, Operator::Eq, phys_lit(2i64), &schema).unwrap(),
                ),
            ],
            Some(phys_lit(false)),
        )
        .unwrap();

        assert_eq!(
            translate(&[Arc::new(case)]),
            Some(col("t.a").eq(lit(1i64)).or(col("t.a").eq(lit(2i64))))
        );
    }

    #[test]
    fn snapshots_dynamic_filters() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&a)],
            phys_lit(true),
        ));
        let filters: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&dynamic) as _];

        assert_eq!(
            translate(&filters),
            None,
            "an empty dynamic filter is `true`"
        );

        dynamic
            .update(binary(a, Operator::Lt, phys_lit(10i64), &schema).unwrap())
            .unwrap();
        assert_eq!(translate(&filters), Some(col("t.a").lt(lit(10i64))));
    }

    #[test]
    fn rejects_columns_out_of_sync_with_the_schema() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(phys::Column::new("b", 0));
        assert_eq!(translate(&[column]), None);
    }

    fn opaque() -> Arc<dyn PhysicalExpr> {
        // An expression without SQL equivalent, standing in for e.g. a hash table lookup.
        Arc::new(phys::NoOp::new())
    }

    fn a_eq(schema: &Schema, value: i64) -> Arc<dyn PhysicalExpr> {
        binary(
            phys_col("a", schema).unwrap(),
            Operator::Eq,
            phys_lit(value),
            schema,
        )
        .unwrap()
    }

    #[test]
    fn keeps_false_so_the_remote_returns_nothing() {
        // E.g. the filter of a join whose build side is empty.
        assert_eq!(translate(&[phys_lit(false)]), Some(lit(false)));
    }

    #[test]
    fn combines_several_filters_with_and() {
        let (schema, _) = schemas();
        assert_eq!(
            translate(&[a_eq(&schema, 1), opaque(), phys_lit(true), a_eq(&schema, 2)]),
            Some(col("t.a").eq(lit(1i64)).and(col("t.a").eq(lit(2i64))))
        );
        assert_eq!(translate(&[]), None);
    }

    #[test]
    fn translates_unary_and_cast_expressions() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        let s = phys_col("s", &schema).unwrap();
        let filters: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(phys::IsNullExpr::new(Arc::clone(&s))),
            Arc::new(phys::IsNotNullExpr::new(Arc::clone(&s))),
            binary(
                Arc::new(phys::NegativeExpr::new(Arc::clone(&a))),
                Operator::Lt,
                phys_lit(0i64),
                &schema,
            )
            .unwrap(),
            binary(
                Arc::new(phys::CastExpr::new(Arc::clone(&a), DataType::Int32, None)),
                Operator::Eq,
                phys_lit(1i32),
                &schema,
            )
            .unwrap(),
            binary(
                Arc::new(phys::TryCastExpr::new(Arc::clone(&s), DataType::Int64)),
                Operator::Eq,
                phys_lit(1i64),
                &schema,
            )
            .unwrap(),
            Arc::new(phys::LikeExpr::new(true, true, s, phys_lit("x%"))),
        ];

        let expected = [
            col("t.s").is_null(),
            col("t.s").is_not_null(),
            Expr::Negative(Box::new(col("t.a"))).lt(lit(0i64)),
            Expr::Cast(Cast::new(Box::new(col("t.a")), DataType::Int32)).eq(lit(1i32)),
            Expr::TryCast(TryCast::new(Box::new(col("t.s")), DataType::Int64)).eq(lit(1i64)),
            Expr::Like(Like::new(
                true,
                Box::new(col("t.s")),
                Box::new(lit("x%")),
                None,
                true,
            )),
        ]
        .into_iter()
        .reduce(Expr::and);

        assert_eq!(translate(&filters), expected);
    }

    #[test]
    fn translates_not_only_when_its_argument_is_exact() {
        let (schema, _) = schemas();
        let not = Arc::new(phys::NotExpr::new(a_eq(&schema, 1)));
        assert_eq!(
            translate(&[not]),
            Some(Expr::Not(Box::new(col("t.a").eq(lit(1i64)))))
        );

        // Widening the argument to `true` would turn `NOT` into `false`, dropping rows.
        let and = binary(a_eq(&schema, 1), Operator::And, opaque(), &schema).unwrap();
        let not = Arc::new(phys::NotExpr::new(and));
        assert_eq!(translate(&[not]), None);
    }

    #[test]
    fn widens_in_list_with_untranslatable_items_to_true() {
        let (schema, _) = schemas();
        let a = phys_col("a", &schema).unwrap();
        // A `CASE` has no exact translation; only top-level ones are relaxed.
        let item = case(&schema, vec![phys_lit(1i64)], None);
        let list = in_list(a, vec![phys_lit(1i64), item], &false, &schema).unwrap();
        assert_eq!(translate(&[list]), None);
    }

    #[test]
    fn translates_nested_conjunctions_in_disjunctions() {
        let (schema, _) = schemas();
        let filter = binary(
            binary(a_eq(&schema, 1), Operator::And, opaque(), &schema).unwrap(),
            Operator::Or,
            a_eq(&schema, 2),
            &schema,
        )
        .unwrap();
        assert_eq!(
            translate(&[filter]),
            Some(col("t.a").eq(lit(1i64)).or(col("t.a").eq(lit(2i64))))
        );
    }

    fn case(
        schema: &Schema,
        branches: Vec<Arc<dyn PhysicalExpr>>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn PhysicalExpr> {
        let b = phys_col("b", schema).unwrap();
        let when_then = branches
            .into_iter()
            .enumerate()
            .map(|(i, then)| (phys_lit(i as i64), then))
            .collect();
        Arc::new(
            phys::CaseExpr::try_new(
                Some(binary(b, Operator::Modulo, phys_lit(2i64), schema).unwrap()),
                when_then,
                else_expr,
            )
            .unwrap(),
        )
    }

    #[test]
    fn relaxes_case_without_else_to_its_branches() {
        let (schema, _) = schemas();
        let case = case(&schema, vec![a_eq(&schema, 1), a_eq(&schema, 2)], None);
        assert_eq!(
            translate(&[case]),
            Some(col("t.a").eq(lit(1i64)).or(col("t.a").eq(lit(2i64))))
        );
    }

    #[test]
    fn relaxes_case_with_only_false_branches_to_false() {
        // A partitioned join whose build side is empty in every partition.
        let (schema, _) = schemas();
        let case = case(&schema, vec![phys_lit(false)], Some(phys_lit(false)));
        assert_eq!(translate(&[case]), Some(lit(false)));
    }

    #[test]
    fn widens_case_with_true_or_untranslatable_branch_to_true() {
        let (schema, _) = schemas();
        // A partitioned join with a canceled partition uses `ELSE true`.
        let case_else_true = case(&schema, vec![a_eq(&schema, 1)], Some(phys_lit(true)));
        assert_eq!(translate(&[case_else_true]), None);

        let case_opaque = case(&schema, vec![a_eq(&schema, 1), opaque()], None);
        assert_eq!(translate(&[case_opaque]), None);
    }

    #[test]
    fn relaxes_case_inside_conjunction() {
        let (schema, _) = schemas();
        let filter = binary(
            Arc::new(phys::IsNotNullExpr::new(phys_col("s", &schema).unwrap())),
            Operator::And,
            case(&schema, vec![a_eq(&schema, 1)], Some(phys_lit(false))),
            &schema,
        )
        .unwrap();
        assert_eq!(
            translate(&[filter]),
            Some(col("t.s").is_not_null().and(col("t.a").eq(lit(1i64))))
        );
    }

    #[test]
    fn uses_the_qualifier_of_each_column() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let df_schema = DFSchema::try_from_qualified_schema("other", &schema).unwrap();
        let filter = a_eq(&schema, 1);
        assert_eq!(
            filters_to_predicate(&[filter], &schema, &df_schema).unwrap(),
            Some(col("other.a").eq(lit(1i64)))
        );
    }

    #[test]
    fn resolves_columns_renamed_in_the_output() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ]);
        let output = DFSchema::try_from_qualified_schema(
            "t",
            &Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("a:1", DataType::Int64, false),
            ]),
        )
        .unwrap();
        let second_a: Arc<dyn PhysicalExpr> = Arc::new(phys::Column::new("a", 1));
        let filter = binary(second_a, Operator::Eq, phys_lit(1i64), &schema).unwrap();
        assert_eq!(
            filters_to_predicate(&[filter], &schema, &output).unwrap(),
            Some(Expr::Column(Column::new(Some("t"), "a:1")).eq(lit(1i64)))
        );
    }

    #[test]
    fn rejects_columns_out_of_range() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(phys::Column::new("a", 42));
        let filter = Arc::new(phys::IsNullExpr::new(column));
        assert_eq!(translate(&[filter]), None);
    }
}
