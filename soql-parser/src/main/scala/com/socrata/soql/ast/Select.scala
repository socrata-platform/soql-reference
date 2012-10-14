package com.socrata.soql.ast

import scala.util.parsing.input.{Position, NoPosition}

case class Select(selection: Selection, where: Option[Expression], groupBy: Option[GroupBy], orderBy: Option[Seq[OrderBy]], limit: Option[BigInt], offset: Option[BigInt]) {
  override def toString = {
    if(AST.pretty) {
      val sb = new StringBuilder("SELECT " + selection)
      where.foreach(sb.append(" ").append(_))
      groupBy.foreach(sb.append(" ").append(_))
      orderBy.foreach { ob => sb.append(ob.mkString(" ORDER BY ", ", ", "")) }
      limit.foreach(sb.append(" LIMIT ").append(_))
      offset.foreach(sb.append(" OFFSET ").append(_))
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
          sb.append(s.exceptions.mkString(" (EXCEPT ", ", ", ")"))
        }
        sb.toString
      }
      (allSystemExcept.map(star(_, ":*")) ++ allUserExcept.map(star(_, "*")) ++ expressions.map(_.toString)).mkString(", ")
    } else {
      AST.unpretty(this)
    }
  }

  def unpositioned = Selection(allSystemExcept.map(_.unpositioned), allUserExcept.map(_.unpositioned), expressions.map(_.unpositioned))
}

case class StarSelection(exceptions: Seq[Identifier], starPosition: Position) {
  def unpositioned = StarSelection(exceptions.map(_.unpositioned), NoPosition)
}

case class SelectedExpression(expression: Expression, name: Option[Identifier]) {
  override def toString =
    if(AST.pretty) {
      name match {
        case Some(name) => expression + " AS " + name
        case None => expression.toString
      }
    } else {
      AST.unpretty(this)
    }

  def unpositioned = SelectedExpression(expression.unpositioned, name.map(_.unpositioned))
}

case class GroupBy(expressions: Seq[Expression], having: Option[Expression]) {
  override def toString =
    if(AST.pretty) {
      "GROUP BY " + expressions.mkString(", ") + having.map { h => " HAVING " + h }.getOrElse("")
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

