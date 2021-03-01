package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import com.socrata.soql.environment._
import Select._
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery}

/**
  * A SubSelect represents (potentially chained) soql that is required to have an alias
  * (because subqueries need aliases)
  */
case class SubSelect(selects: BinaryTree[Select], alias: String)

/**
  * All joins must select from another table. A join may also join on sub-select. A join on a sub-select requires an
  * alias, but the alias is optional if the join is on a table-name only (e.g. "join 4x4").
  *
  *   "join 4x4" => JoinSelect(Left(TableName(4x4, None))) { aliasOpt = None }
  *   "join 4x4 as a" => JoinSelect(Left(TableName(4x4, a))) { aliasOpt = a }
  *   "join (select id from 4x4) as a" =>
  *     JoinSelect(Right(SubSelect(List(_select_id_), a))) { aliasOpt = a }
  *   "join (select c.id from 4x4 as c) as a" =>
  *     JoinSelect(Right(SubSelect(List(_select_id_), a))) { aliasOpt = a }
  */
case class JoinSelect(subSelect: Either[TableName, SubSelect]) {
  // The overall alias for the join select, which is the alias for the subSelect, if defined.
  // Otherwise, it is the alias for the TableName, if defined.
  val alias: Option[String] = {
    subSelect match {
      case Left(TableName(_, alias, _)) => alias
      case Right(SubSelect(_, alias)) => Option(alias)
    }
  }

  def selects: Option[BinaryTree[Select]] = {
    subSelect match {
      case Left(_) => None
      case Right(SubSelect(s, _)) => Some(s)
    }
  }

  override def toString: String = {
    val (subSelectStr, aliasStrOpt) = subSelect match {
      case Right(SubSelect(select, subAlias)) =>
        val selectStr = Select.toString(select)
        (s"($selectStr)", Some(subAlias))
      case Left(tn@TableName(name, alias, _)) =>
        (tn.toString, None)
    }

    List(Some(subSelectStr), itrToString("AS", aliasStrOpt.map(TableName.removeValidPrefix))).flatString
  }
}

object Select {
  def itrToString(prefix: String, l: Iterable[_], sep: String = " "): Option[String] = {
    itrToString(Some(prefix), l, sep)
  }

  def itrToString(prefix: Option[String], l: Iterable[_], sep: String): Option[String] = {
    if (l.nonEmpty) {
      Some(l.mkString(prefix.map(p => s"$p ").getOrElse(""), sep, ""))
    } else {
      None
    }
  }

  def toString(selects: BinaryTree[Select]): String = {
    selects match {
      case PipeQuery(l, r) =>
        val ls = Select.toString(l)
        val rs = Select.toString(r)
        s"$ls |> $rs"
      case Compound(op, l, r) =>
        val ls = Select.toString(l)
        val rs = Select.toString(r)
        s"$ls $op $rs"
      case Leaf(select) =>
        select.toString
    }
  }

  implicit class StringOptionList(l: List[Option[String]]) {
    def flatString = l.flatten.mkString(" ")
  }
}

/**
  * Represents a single select statement, not including the from. Top-level selects have an implicit "from"
  * based on the current view. Joins do require a "from" (which is a member of the JoinSelect class). A List[Select]
  * represents chained soql (e.g. "select a, b |> select a"), and is what is returned from a top-level parse of a
  * soql string (see Parser#selectStatement).
  *
  * the chained soql:
  *   "select id, a |> select a"
  * is equivalent to:
  *   "select a from (select id, a [from current view]) as alias"
  * and is represented as:
  *   List(select_id_a, select_id)
  */
case class Select(
  distinct: Boolean,
  selection: Selection,
  from: Option[TableName],
  joins: Seq[Join],
  where: Option[Expression],
  groupBys: Seq[Expression],
  having: Option[Expression],
  orderBys: Seq[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String]) {

  private def toString(from: Option[TableName]): String = {
    if(AST.pretty) {
      val distinctStr = if (distinct) "DISTINCT " else ""
      val selectStr = Some(s"SELECT $distinctStr$selection")
      val fromStr = this.from.orElse(from).map(t => s"FROM $t")
      val joinsStr = itrToString(None, joins.map(j => s"${j.toString}"), " ")
      val whereStr = itrToString("WHERE", where)
      val groupByStr = itrToString("GROUP BY", groupBys, ", ")
      val havingStr = itrToString("HAVING", having)
      val obStr = itrToString("ORDER BY", orderBys, ", ")
      val searchStr = itrToString("SEARCH", search.map(Expression.escapeString))
      val limitStr = itrToString("LIMIT", limit)
      val offsetStr = itrToString("OFFSET", offset)

      val parts = List(selectStr, fromStr, joinsStr, whereStr, groupByStr, havingStr, obStr, searchStr, limitStr, offsetStr)
      parts.flatString
    } else {
      AST.unpretty(this)
    }
  }

  def toStringWithFrom(fromTable: TableName): String = toString(Some(fromTable))

  def format(from: Option[TableName]): String = {
    val distinctStr = if (distinct) "DISTINCT " else ""
    val selectStr = Some(s"SELECT $distinctStr$selection")
    val fromStr = this.from.orElse(from).map(t => s"\nFROM $t")
    val joinsStr = itrToString(None, joins.map(j => s"\n${j.toString}"), "\n ")
    val whereStr = itrToString("\nWHERE", where)
    val groupByStr = itrToString("\nGROUP BY", groupBys, ", ")
    val havingStr = itrToString("\nHAVING", having)
    val obStr = itrToString("\nORDER BY", orderBys, ", ")
    val searchStr = itrToString("\nSEARCH", search.map(Expression.escapeString))
    val limitStr = itrToString("\nLIMIT", limit)
    val offsetStr = itrToString("\nOFFSET", offset)

    val parts = List(selectStr, fromStr, joinsStr, whereStr, groupByStr, havingStr, obStr, searchStr, limitStr, offsetStr)
    parts.flatString
  }

  override def toString: String = toString(None)
}

// represents the columns being selected. examples:
// "first_name, last_name as last"
// "*"
// "*(except id)"
// ":*"      <-- all columns including system columns (date_created, etc.)
// "sum(count) as s"
case class Selection(allSystemExcept: Option[StarSelection], allUserExcept: Seq[StarSelection], expressions: Seq[SelectedExpression]) {
  override def toString = {
    if(AST.pretty) {
      def star(s: StarSelection, token: String) = {
        val sb = new StringBuilder()
        s.qualifier.foreach { x =>
          sb.append(TableName.withSoqlPrefix(x))
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
  def isSimple(selects: Seq[Select]): Boolean = {
    selects.forall(_.selection.isSimple)
  }
}
