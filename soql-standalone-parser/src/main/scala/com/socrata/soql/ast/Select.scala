package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import com.socrata.soql.environment._
import Select.itrToString

// TODO: override toString
// alias must be defined for soql-entered subSelect. not defined for chained soql.
// TODO: could be NonEmptyList
case class SubSelect(selects: List[Select], alias: String)
case class JoinSelect(fromTable: TableName, subSelect: Option[SubSelect]) {
  def aliasOpt: Option[String] =  subSelect.map(_.alias).orElse(fromTable.alias)
  def selects: List[Select] = subSelect.map(_.selects).getOrElse(Nil)

  override def toString: String = {
    val (subSelectStr, aliasStrOpt) = subSelect.map { case SubSelect(h :: tail, alias) =>
      val selectWithFromStr = h.toStringWithFrom(fromTable)
      val selectStr = (selectWithFromStr :: tail.map(_.toString)).mkString("|>")
      (s"($selectStr)", Some(alias))
    }.getOrElse((fromTable.toString, None))

    List(subSelectStr, itrToString("AS", aliasStrOpt)).filter(_.nonEmpty).mkString(" ")
  }
}

object Select {
  type TopLevelSelect = List[Select]

  def itrToString[A](prefix: String, l: Iterable[A], sep: String = "") = {
    if (l.nonEmpty) {
      l.mkString(prefix, sep, "")
    } else {
      ""
    }
  }
}

case class Select(
  distinct: Boolean,
  selection: Selection,
  joins: List[Join],
  where: Option[Expression],
  groupBys: List[Expression],
  having: Option[Expression],
  orderBys: List[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String]) {

  private def preFromToString = {
    val distinctStr = if (distinct) "DISTINCT " else ""
    s"SELECT $distinctStr$selection"
  }

  private def postFromToString = {
    val joinsStr = joins.mkString(" ")
    val whereStr = itrToString("WHERE", where)
    val groupByStr = itrToString("GROUP BY", groupBys)
    val havingStr = itrToString("HAVING", having)
    val obStr = itrToString("ORDER BY", orderBys, ",")
    val limitStr = itrToString("LIMIT", limit)
    val offsetStr = itrToString("OFFSET", offset)
    val searchStr = itrToString("SEARCH", search.map(Expression.escapeString))
    List(joinsStr, whereStr, groupByStr, havingStr, obStr, limitStr, offsetStr, searchStr).filter(_.nonEmpty).mkString(" ")
  }

  def toStringWithFrom(fromTable: Option[TableName] = None): String = {
    if(AST.pretty) {
      List(preFromToString, fromTable.map(_.toString).getOrElse(""), postFromToString).filter(_.nonEmpty).mkString(" ")
    } else {
      AST.unpretty(this)
    }
  }

  def toStringWithFrom(fromTable: TableName): String = toStringWithFrom(Some(fromTable))

  override def toString = toStringWithFrom(None)
}

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
  /**
    * Simple Select is a select created by a join where a sub-query is not used like "JOIN @aaaa-aaaa"
    */
  def isSimple(selects: List[Select]): Boolean = {
    selects.forall(_.selection.isSimple)
  }
}
