package com.socrata.soql.ast

import scala.util.parsing.input.{Position, NoPosition}
import com.socrata.soql.environment.ColumnName

case class Select(selection: Selection, where: Option[Expression], groupBy: Option[Seq[Expression]], having: Option[Expression], orderBy: Option[Seq[OrderBy]], limit: Option[BigInt], offset: Option[BigInt], search: Option[String]) {
  override def toString = {
    if(AST.pretty) {
      val sb = new StringBuilder("SELECT " + selection)
      where.foreach(sb.append(" WHERE ").append(_))
      groupBy.foreach { gb => sb.append(gb.mkString(" GROUP BY ", ", ", "")) }
      having.foreach(sb.append(" HAVING ").append(_))
      orderBy.foreach { ob => sb.append(ob.mkString(" ORDER BY ", ", ", "")) }
      limit.foreach(sb.append(" LIMIT ").append(_))
      offset.foreach(sb.append(" OFFSET ").append(_))
      search.foreach(sb.append(" SEARCH ").append(_))
      sb.toString
    } else {
      AST.unpretty(this)
    }
  }
}

case class Selection(allSystemExcept: Option[StarSelection], allUserExcept: Option[StarSelection], expressions: Seq[SelectedExpression]) {
  override def toString = {
    if(AST.pretty) {
      def star(s: StarSelection, token: String) = {
        val sb = new StringBuilder(token)
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
}

case class StarSelection(exceptions: Seq[(ColumnName, Position)]) {
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

