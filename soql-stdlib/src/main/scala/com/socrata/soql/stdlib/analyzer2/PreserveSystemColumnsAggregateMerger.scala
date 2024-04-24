package com.socrata.soql.stdlib.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions}
import com.socrata.soql.types.{SoQLType, SoQLValue}

// A merger that can be passed to SoQLAnalyzer#preserveSystemColumns
// which knows about standard SoQL system columns
object PreserveSystemColumnsAggregateMerger {
  def apply[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue})]() =
    new PreserveSystemColumnsAggregateMerger[MT]

  private val standardSystemColumns: Set[ColumnName] =
    Set(":id", ":version", ":created_at", ":updated_at").map(ColumnName)
}

class PreserveSystemColumnsAggregateMerger[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends ((ColumnName, Expr[MT]) => Option[Expr[MT]]) with StatementUniverse[MT]
{
  import PreserveSystemColumnsAggregateMerger._

  def apply(columnName: ColumnName, expr: Expr): Option[Expr] =
    if(standardSystemColumns(columnName)) {
      Some(AggregateFunctionCall[MT](
        MonomorphicFunction(SoQLFunctions.Max, Map("a" -> expr.typ)),
        Seq(expr),
        false,
        None
      )(FuncallPositionInfo.Synthetic))
    } else {
      None
    }
}
