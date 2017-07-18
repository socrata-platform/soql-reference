package com.socrata.soql.mapping

import com.socrata.soql.ast._
import com.socrata.soql.environment.ColumnName

import scala.util.parsing.input.{Position, NoPosition}

/**
 * Maps column names in the given AST.  Position information is not updated
 * or retained.
 *
 * @param columnNameMap Map from current names to new names.  The map must be defined
 *                      for all column names passed in.
 */
class ColumnNameMapper(columnNameMap: Map[ColumnName, ColumnName]) {

  def mapSelect(ss: Seq[Select]): Seq[Select] = {
    if(ss.nonEmpty) {
      // this only needs to apply to the first query in the chain; subsequent elements
      // take their names from the output of the first query.
      val s = ss.head
      ss.updated(0, Select(
        distinct = s.distinct,
        selection = mapSelection(s.selection),
        from = s.from,
        join = s.join.map(_ map mapJoin),
        where = s.where map mapExpression,
        groupBy = s.groupBy.map(_ map mapExpression),
        having = s.having map mapExpression,
        orderBy = s.orderBy.map(_ map mapOrderBy),
        limit = s.limit,
        offset = s.offset,
        search = s.search))
    } else {
      ss
    }
  }

  def mapJoin(join: Join): Join =  {
    val mappedTableLike = mapSelect(join.tableLike)
    val mappedExpr = mapExpression(join.expr)
    join match {
      case j: InnerJoin =>
        InnerJoin(mappedTableLike, j.alias, mappedExpr)
      case j: LeftOuterJoin =>
        j.copy(tableLike = mappedTableLike, expr = mappedExpr)
      case j: RightOuterJoin =>
        j.copy(tableLike = mappedTableLike, expr = mappedExpr)
      case j: FullOuterJoin =>
        j.copy(tableLike = mappedTableLike, expr = mappedExpr)
    }
  }

  def mapExpression(e: Expression): Expression =  e match {
    case NumberLiteral(v) => NumberLiteral(v)(NoPosition)
    case StringLiteral(v) => StringLiteral(v)(NoPosition)
    case BooleanLiteral(v) => BooleanLiteral(v)(NoPosition)
    case NullLiteral() => NullLiteral()(NoPosition)

    case e: ColumnOrAliasRef =>
      ColumnOrAliasRef(e.qualifier, columnNameMap(e.column))(NoPosition)
    case e: FunctionCall =>
      FunctionCall(e.functionName, e.parameters map mapExpression)(NoPosition, NoPosition)
  }

  def mapOrderBy(o: OrderBy): OrderBy = OrderBy(
    expression = mapExpression(o.expression),
    ascending = o.ascending,
    nullLast = o.nullLast)

  def mapColumnNameAndPosition(s: (ColumnName, Position)): (ColumnName, Position) =
    (columnNameMap(s._1), NoPosition)

  def mapStarSelection(s: StarSelection): StarSelection =
    StarSelection(s.qualifier, s.exceptions map mapColumnNameAndPosition)

  def mapSelectedExpression(s: SelectedExpression): SelectedExpression = {
      // name isn't a column name, but a column alias so no mapping
      SelectedExpression(mapExpression(s.expression),
        s.name.map { case (aliasName, pos) => (aliasName, NoPosition) })
  }

  def mapSelection(s: Selection) = Selection(
    s.allSystemExcept map mapStarSelection,
    s.allUserExcept map mapStarSelection,
    s.expressions map mapSelectedExpression)

}
