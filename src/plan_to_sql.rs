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

use crate::generator::{SQLExpr, SQLJoin, SQLSelect, SQLSubqueryAlias};
use crate::SQLRelation;
use datafusion::common::Result;

/// Generate a SQL string from a SQLRelation struct
pub fn plan_to_sql(plan: &SQLRelation, indent: usize) -> Result<String> {
    let indent_str = "  ".repeat(indent);
    match plan {
        SQLRelation::Select(SQLSelect {
            projection,
            filter,
            input,
        }) => {
            let expr: Vec<String> = projection
                .iter()
                .map(|e| expr_to_sql(e, indent))
                .collect::<Result<Vec<_>>>()?;
            let input = plan_to_sql(input, indent + 1)?;
            let where_clause = if let Some(predicate) = filter {
                let predicate = expr_to_sql(predicate, indent)?;
                format!("\n{}WHERE {}", indent_str, predicate)
            } else {
                "".to_string()
            };
            Ok(format!(
                "SELECT {}\n{}FROM ({}){}",
                expr.join(", "),
                indent_str,
                input,
                where_clause
            ))
        }
        SQLRelation::TableScan(scan) => Ok(scan.table_name.clone()),
        SQLRelation::Join(SQLJoin {
            left,
            right,
            on,
            join_type,
            ..
        }) => {
            let l = plan_to_sql(left, indent + 1)?;
            let r = plan_to_sql(right, indent + 1)?;
            let join_condition = on
                .iter()
                .map(|(l, r)| format!("{} = {}", l.flat_name(), r.flat_name()))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(format!(
                "\n{}{}\n{}{} JOIN\n{}{}\n{}ON {}",
                indent_str, l, indent_str, join_type, indent_str, r, indent_str, join_condition
            ))
        }
        SQLRelation::SubqueryAlias(SQLSubqueryAlias { input, alias, .. }) => {
            let sql = plan_to_sql(input, indent + 1)?;
            Ok(format!("({}) {}", sql, alias))
        }
    }
}

/// Generate a SQL string from an expression
fn expr_to_sql(expr: &SQLExpr, indent: usize) -> Result<String> {
    Ok(match expr {
        SQLExpr::Column(col) => col.flat_name(),
        SQLExpr::BinaryExpr { left, op, right } => {
            let l = expr_to_sql(left, indent)?;
            let r = expr_to_sql(right, indent)?;
            format!("{} {} {}", l, op, r)
        }
        SQLExpr::Exists { subquery, negated } => {
            let sql = plan_to_sql(&SQLRelation::Select(subquery.as_ref().clone()), indent)?;
            if *negated {
                format!("NOT EXISTS ({})", sql)
            } else {
                format!("EXISTS ({})", sql)
            }
        }
    })
}
