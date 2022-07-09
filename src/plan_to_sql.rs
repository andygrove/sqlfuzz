// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::SQLRelation;
use datafusion::{common::Result, logical_expr::Expr};

/// Generate a SQL string from a SQLRelation struct
pub fn plan_to_sql(plan: &SQLRelation) -> Result<String> {
    match plan {
        SQLRelation::Select {
            projection,
            filter,
            input,
        } => {
            let expr: Vec<String> = projection.iter().map(expr_to_sql).collect();
            let input = plan_to_sql(input)?;
            let where_clause = if let Some(predicate) = filter {
                format!(" WHERE {}", expr_to_sql(predicate))
            } else {
                "".to_string()
            };
            Ok(format!(
                "SELECT {} FROM {}{}",
                expr.join(", "),
                input,
                where_clause
            ))
        }
        SQLRelation::TableScan(scan) => Ok(scan.table_name.clone()),
        SQLRelation::Join {
            left,
            right,
            on,
            join_type,
            ..
        } => {
            let l = plan_to_sql(left)?;
            let r = plan_to_sql(right)?;
            let join_condition = on
                .iter()
                .map(|(l, r)| format!("{} = {}", l.flat_name(), r.flat_name()))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(format!(
                "{} {} JOIN {} ON {}",
                l, join_type, r, join_condition
            ))
        }
        SQLRelation::SubqueryAlias { input, alias, .. } => {
            Ok(format!("({}) {}", plan_to_sql(input)?, alias))
        }
    }
}

/// Generate a SQL string from an expression
fn expr_to_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => col.flat_name(),
        Expr::BinaryExpr { left, op, right } => {
            format!("{} {} {}", expr_to_sql(left), op, expr_to_sql(right))
        }
        other => other.to_string(),
    }
}
