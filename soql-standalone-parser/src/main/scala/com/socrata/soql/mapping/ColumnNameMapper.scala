package com.socrata.soql.mapping

import com.socrata.soql.ast._
import com.socrata.soql.environment.ColumnName

import scala.util.parsing.input.{NoPosition, Position}

/**
 * Maps column names in the given AST.  Position information is not updated
 * or retained.
 *
 * @param columnNameMap Map from current names to new names.  The map must be defined
 *                      for all column names passed in.
 */
class ColumnNameMapper(columnNameMap: Map[ColumnName, ColumnName]) {

  def mapSelect(selects: List[Select]): List[Select] = {
    selects match {
      case Nil => Nil
      case h :: tail =>
        val mappedHead = Select(
          distinct = h.distinct,
          selection = mapSelection(h.selection),
          joins = h.joins.map(mapJoin),
          where = h.where map mapExpression,
          groupBys = h.groupBys.map(mapExpression),
          having = h.having map mapExpression,
          orderBys = h.orderBys.map(mapOrderBy),
          limit = h.limit,
          offset = h.offset,
          search = h.search
        )
        mappedHead :: tail
    }
  }

  def mapJoin(join: Join): Join =  {
    val mappedSubSelect = join.from.subSelect.map { ss =>
      ss.copy(selects = mapSelect(ss.selects))
    }
    val mappedFrom = join.from.copy(subSelect = mappedSubSelect)
    val mappedOn = mapExpression(join.on)
    join match {
      case j: InnerJoin =>
        InnerJoin(mappedFrom, mappedOn)
      case j: LeftOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: RightOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: FullOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
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
