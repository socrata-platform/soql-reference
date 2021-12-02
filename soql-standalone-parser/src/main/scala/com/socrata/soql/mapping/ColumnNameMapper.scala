package com.socrata.soql.mapping

import com.socrata.soql.ast._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.{BinaryTree, Compound, PipeQuery, Leaf}

import scala.util.parsing.input.{NoPosition, Position}

/**
 * Maps column names in the given AST.  Position information is not updated
 * or retained.
 * This is used for rewriting rollups in soda fountain and does not work for chained queries (only operate on the first element).
 *
 * @param columnNameMap Map from current names to new names.  The map must be defined
 *                      for all column names passed in.
 */
class ColumnNameMapper(columnNameMap: Map[ColumnName, ColumnName]) {

  def mapSelect(selects: BinaryTree[Select]): BinaryTree[Select] = {
    selects match {
      case PipeQuery(l, r) =>
        // previously when pipe query is in seq form,
        // mapSelect only operates on the first element
        val nl = mapSelect(l)
        PipeQuery(nl, r)
      case Compound(op, l, r) =>
        val nl = mapSelect(l)
        val nr = mapSelect(r)
        Compound(op, nl, nr)
      case Leaf(h) =>
        Leaf(Select(
          distinct = h.distinct,
          selection = mapSelection(h.selection),
          from = h.from,
          joins = h.joins.map(mapJoin),
          where = h.where map mapExpression,
          groupBys = h.groupBys.map(mapExpression),
          having = h.having map mapExpression,
          orderBys = h.orderBys.map(mapOrderBy),
          limit = h.limit,
          offset = h.offset,
          search = h.search
        ))
    }
  }

  def mapJoin(join: Join): Join =  {
    val mappedSubSelect = join.from match {
      case jq: JoinQuery =>
        jq.copy(selects = mapSelect(jq.selects))
      case l =>
        l
    }

    val mappedFrom = mappedSubSelect
    val mappedOn = mapExpression(join.on)
    join match {
      case j: InnerJoin =>
        InnerJoin(mappedFrom, mappedOn, j.lateral)
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
    case Hole(name) => Hole(name)(NoPosition)

    case e: ColumnOrAliasRef =>
      ColumnOrAliasRef(e.qualifier, columnNameMap(e.column))(NoPosition)
    case e: FunctionCall =>
      FunctionCall(e.functionName, e.parameters map mapExpression, e.filter map mapExpression, e.window map mapWindow)(NoPosition, NoPosition)
  }

  def mapOrderBy(o: OrderBy): OrderBy = OrderBy(
    expression = mapExpression(o.expression),
    ascending = o.ascending,
    nullLast = o.nullLast)

  def mapWindow(w: WindowFunctionInfo): WindowFunctionInfo = {
    val WindowFunctionInfo(partitions, orderings, frames) = w
    WindowFunctionInfo(
      partitions.map(mapExpression),
      orderings.map(mapOrderBy),
      frames)
  }

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
