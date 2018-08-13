package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import com.socrata.soql.environment.{ColumnName, TableName, TableSource}


case class Select(
  distinct: Boolean,
  selection: Selection,
  join: List[Join],
  where: Option[Expression],
  groupBy: List[Expression],
  having: Option[Expression],
  orderBy: List[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String]
) {
  def contextualize(fromTableName: String) =
    BasedSelect(distinct, selection, From(TableName(fromTableName), Nil, None), join, where, groupBy, having, orderBy, limit, offset, search)

  def contextualize(from: From) =
    BasedSelect(distinct, selection, from, join, where, groupBy, having, orderBy, limit, offset, search)
}

// can be represented as [Select with From]?
case class BasedSelect(
  distinct: Boolean,
  selection: Selection,
  from: From,
  join: List[Join],
  where: Option[Expression],
  groupBy: List[Expression],
  having: Option[Expression],
  orderBy: List[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String]
) extends TableSource {
  def decontextualized = Select(distinct, selection, join, where, groupBy, having, orderBy, limit, offset, search)
}

case class From(source: TableSource, refinements: List[Select], alias: Option[String])

case object NoContext extends TableSource // context for a 1-row, 1-column virtual table, like "select 1"

case class Selection(allSystemExcept: Option[StarSelection], allUserExcept: Seq[StarSelection], expressions: Seq[SelectedExpression]) {
  override def toString = {
    if(AST.pretty) {
      def star(s: StarSelection, token: String) = {
        val sb = new StringBuilder()
        s.qualifier.foreach { x =>
          sb.append(x.replaceFirst(TableName.SodaFountainTableNamePrefix, TableName.Prefix))
          sb.append(TableName.Field)
        }
        sb.append(token)
        if(s.exceptions.nonEmpty) {
          sb.append(s.exceptions.map(e => e._1).mkString(" (EXCEPT ", ", ", ")"))
        }
        sb.toString
      }
      (allSystemExcept.map(star(_, ":*")) ++ allUserExcept.map(star(_, "*")) ++ expressions.map(_.toString)).mkString(", ")
    } else {
      AST.unpretty(this)
    }
  }

  def isSimple = allSystemExcept.isEmpty && allUserExcept.isEmpty && expressions.isEmpty
}

case class StarSelection(qualifier: Option[String], exceptions: Seq[(ColumnName, Position)]) {
  var starPosition: Position = NoPosition
  def positionedAt(p: Position): this.type = {
    starPosition = p
    this
  }
}

case class SelectedExpression(expression: Expression, name: Option[(ColumnName, Position)]) {
  override def toString =
    if(AST.pretty) {
      name match {
        case Some(name) => expression + " AS " + name._1
        case None => expression.toString
      }
    } else {
      AST.unpretty(this)
    }
}

case class OrderBy(expression: Expression, ascending: Boolean, nullLast: Boolean) {
  override def toString =
    if(AST.pretty) {
      expression + (if(ascending) " ASC" else " DESC") + (if(nullLast) " NULL LAST" else " NULL FIRST")
    } else {
      AST.unpretty(this)
    }
}

object SimpleSelect {
//  def apply(resource: String): Select = {
//    Select(distinct = false,
//           selection = Selection(None, Seq.empty, Seq.empty),
//           from = Some(TableName(resource)),
//           join = None,
//           where = None,
//           groupBy = None,
//           having = None,
//           orderBy = None,
//           limit = None,
//           offset = None,
//           search = None)
//  }


  /**
    * Simple Select is a select created by a join where a sub-query is not used like "JOIN @aaaa-aaaa"
    */
  def isSimple(select: BasedSelect): Boolean = {
     select.from.source match {
      case _: TableName => select.selection.isSimple
      case _ => false
    }
  }

  def isSimple(from: From) = from match {
    case From(bs: BasedSelect, refs, alias) => isSimple(bs) // TODO: do refs need to be Nil for true?
    case _ => false
  }

//  def isSimple(selects: Seq[Select]): Boolean = {
//    selects match {
//      case Seq(s) => isSimple(s)
//      case _ => false
//    }
//  }
}

//case class Select(distinct: Boolean, selection: Selection, from: Option[TableName], join: Option[List[Join]], where: Option[Expression], groupBy: Option[Seq[Expression]], having: Option[Expression], orderBy: Option[Seq[OrderBy]], limit: Option[BigInt], offset: Option[BigInt], search: Option[String]) {
//  override def toString = {
//    if(AST.pretty) {
//      val sb = new StringBuilder("SELECT ")
//      if (distinct) sb.append("DISTINCT ")
//      sb.append(selection)
//      from.foreach(sb.append(" FROM ").append(_))
//      join.toList.flatten.foreach { j =>
//        sb.append(" ")
//        sb.append(j.toString)
//      }
//      where.foreach(sb.append(" WHERE ").append(_))
//      groupBy.foreach { gb => sb.append(gb.mkString(" GROUP BY ", ", ", "")) }
//      having.foreach(sb.append(" HAVING ").append(_))
//      orderBy.foreach { ob => sb.append(ob.mkString(" ORDER BY ", ", ", "")) }
//      limit.foreach(sb.append(" LIMIT ").append(_))
//      offset.foreach(sb.append(" OFFSET ").append(_))
//      search.foreach(s => sb.append(" SEARCH ").append(Expression.escapeString(s)))
//      sb.toString
//    } else {
//      AST.unpretty(this)
//    }
//  }
//}
