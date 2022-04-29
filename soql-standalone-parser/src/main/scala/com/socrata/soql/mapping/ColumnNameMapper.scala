package com.socrata.soql.mapping

import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery}

import java.util.NoSuchElementException
import scala.util.parsing.input.{NoPosition, Position}

/**
 * Maps column names in the given AST.  Position information is not updated
 * or retained.
 * This is used for rewriting rollups in soda fountain and does not work for chained queries (only operate on the first element).
 *
 * @param rootSchemas Map from current names to new names.  The map must be defined
 *                    for all column names passed in.
 */
class ColumnNameMapper(rootSchemas: Map[String, Map[ColumnName, ColumnName]]) {

  def mapSelects(selects: BinaryTree[Select], generateAliases: Boolean = false): BinaryTree[Select] = {
    selects match {
      case PipeQuery(l, r) =>
        // previously when pipe query is in seq form,
        // mapSelect only operates on the first element
        val nl = mapSelects(l, true)
        PipeQuery(nl, r)
      case Compound(op, l, r) =>
        val nl = mapSelects(l, generateAliases)
        val nr = mapSelects(r, generateAliases)
        Compound(op, nl, nr)
      case Leaf(select) =>
        Leaf(mapSelect(select, generateAliases))
    }
  }

  def mapSelect(select: Select, generateAliases: Boolean): Select = {
    val ss0 = select.from match {
      case None =>
        rootSchemas.get(TableName.PrimaryTable.qualifier) match {
          case Some(x) =>
            Map(TableName.PrimaryTable.qualifier -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case Some(TableName(name, Some(alias))) if name == TableName.This =>
        rootSchemas.get(TableName.PrimaryTable.qualifier) match {
          case Some(x) =>
            Map(alias -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case Some(tn@TableName(_, alias)) =>
        rootSchemas.get(tn.nameWithSodaFountainPrefix) match {
          case Some(x) =>
            Map(alias.getOrElse(TableName.PrimaryTable.qualifier) -> x,
              alias.getOrElse(tn.nameWithSodaFountainPrefix) -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case _ =>
        Map.empty[String, Map[ColumnName, ColumnName]]
    }

    val ss = select.joins.foldLeft(ss0) { (acc, join) =>
      join.from match {
        case JoinTable(tn@TableName(_, _)) =>
          rootSchemas.get(tn.nameWithSodaFountainPrefix) match {
            case Some(schema) =>
              val key = tn.alias.getOrElse(tn.nameWithSodaFountainPrefix)
              acc + (key -> schema)
            case None =>
              acc
          }
        case _ =>
          acc
      }
    }

    val selection = mapSelection(select.selection, ss, generateAliases)
    val aliases = selection.expressions.foldLeft(Map.empty[ColumnName, ColumnName]) { (acc, selectedExpr) =>
      selectedExpr.name match  {
        case Some((columnName, _)) =>
          acc + (columnName -> columnName)
        case None =>
          acc
      }
    }
    val ssWithAliases: Map[String, Map[ColumnName, ColumnName]] =
      if (aliases.nonEmpty) {
        val withAliases = ss.getOrElse(TableName.PrimaryTable.qualifier, Map.empty) ++ aliases
        ss + (TableName.PrimaryTable.qualifier -> withAliases)
      } else {
        ss
      }

    select.copy(
      selection = selection,
      joins = select.joins.map(j => mapJoin(j, ssWithAliases)),
      where = select.where map(mapExpression(_, ssWithAliases)),
      groupBys = select.groupBys.map(mapExpression(_, ssWithAliases)),
      having = select.having.map(mapExpression(_, ssWithAliases)),
      orderBys = select.orderBys.map(mapOrderBy(_, ssWithAliases)),
    )
  }

  def mapJoin(join: Join, schemas: Map[String, Map[ColumnName, ColumnName]]): Join =  {
    val mappedFrom = join.from match {
      case jq: JoinQuery =>
        jq.copy(selects = mapSelects(jq.selects, true))
      case l =>
        l
    }

    val mappedOn = mapExpression(join.on, schemas)
    join match {
      case j: InnerJoin =>
        j.copy(on = mappedOn)
        InnerJoin(from = mappedFrom, mappedOn, j.lateral)
      case j: LeftOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: RightOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: FullOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
    }
  }

  def mapExpression(e: Expression, schemas: Map[String, Map[ColumnName, ColumnName]]): Expression = {
    e match {
      case NumberLiteral(v) => NumberLiteral(v)(NoPosition)
      case StringLiteral(v) => StringLiteral(v)(NoPosition)
      case BooleanLiteral(v) => BooleanLiteral(v)(NoPosition)
      case NullLiteral() => NullLiteral()(NoPosition)
      case Hole.UDF(name) => Hole.UDF(name)(NoPosition)
      case Hole.SavedQuery(name, typ) => Hole.SavedQuery(name, typ)(NoPosition)
      case e: ColumnOrAliasRef =>
        val qualifier = e.qualifier.getOrElse("_")
        schemas.get(qualifier) match {
          case None =>
            e
          case Some(schema) =>
            schema.get(e.column) match {
              case Some(toColumn) =>
                e.copy(column = toColumn)(e.position)
              case None =>
                throw new NoSuchElementException(s"column not found ${qualifier}.${e.column.name}")
            }
        }
      case e: FunctionCall =>
        val mp = e.parameters.map(p => mapExpression(p, schemas))
        val mw = e.window.map(w => mapWindow(w, schemas))
        val mf = e.filter.map(fi => mapExpression(fi, schemas))
        FunctionCall(e.functionName, mp, mf, mw)(NoPosition, NoPosition)
    }
  }

  def mapOrderBy(o: OrderBy, schemas: Map[String, Map[ColumnName, ColumnName]]): OrderBy = {
    OrderBy(expression = mapExpression(o.expression, schemas),
            ascending = o.ascending,
            nullLast = o.nullLast)
  }

  def mapWindow(w: WindowFunctionInfo, schemas: Map[String, Map[ColumnName, ColumnName]]): WindowFunctionInfo = {
    val WindowFunctionInfo(partitions, orderings, frames) = w
    WindowFunctionInfo(partitions.map(mapExpression(_, schemas)),
                       orderings.map(mapOrderBy(_, schemas)),
                       frames)
  }

  def mapColumnNameAndPosition(s: (ColumnName, Position), columnNameMap:Map[ColumnName, ColumnName]): (ColumnName, Position) = {
    (columnNameMap(s._1), NoPosition)
  }

  def mapStarSelection(s: StarSelection, schemas: Map[String, Map[ColumnName, ColumnName]]): StarSelection = {
    val qualifier = s.qualifier.getOrElse(TableName.PrimaryTable.qualifier)
    val mse = s.exceptions.map(x => mapColumnNameAndPosition(x, schemas(qualifier)))
    StarSelection(s.qualifier, mse)
  }

  def mapSelectedExpression(se: SelectedExpression, schemas: Map[String, Map[ColumnName, ColumnName]], generateAliases: Boolean): SelectedExpression = {
    // name isn't a column name, but a column alias so no mapping
    val mse = se.name match {
      case Some((aliasName, pos)) => Some (aliasName, NoPosition)
      case None =>
        se.expression match {
          case columnOrAliasRef: ColumnOrAliasRef if generateAliases =>
            Some(columnOrAliasRef.column, NoPosition)
          case _ =>
            None
        }
    }

    SelectedExpression(expression = mapExpression(se.expression, schemas), mse)
  }

  def mapSelection(s: Selection, schemas: Map[String, Map[ColumnName, ColumnName]], generateAliases: Boolean = false) = {
    val mes = s.expressions.map(e => mapSelectedExpression(e, schemas, generateAliases))
    val mase =  s.allSystemExcept.map(x => mapStarSelection(x, schemas))
    val maue =  s.allUserExcept.map(x => mapStarSelection(x, schemas))
    Selection(mase, maue, mes)
  }
}
