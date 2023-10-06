package com.socrata.soql.analyzer2

import com.socrata.soql.util.LazyToString

trait Stringifier[MT <: MetaTypes] extends StatementUniverse[MT] {
  def statement(stmt: Statement): LazyToString
  def from(from: From): LazyToString
  def expr(expr: Expr): LazyToString
}

object Stringifier {
  def simple[MT <: MetaTypes]: Stringifier[MT] =
    new Stringifier[MT] {
      override def statement(stmt: Statement) = LazyToString(stmt)
      override def from(from: From) = LazyToString(from)
      override def expr(expr: Expr) = LazyToString(expr)
    }

  def pretty[MT <: MetaTypes](
    implicit cvDoc: HasDoc[MT#ColumnValue],
    dtnDoc: HasDoc[MT#DatabaseTableNameImpl],
    dcnDoc: HasDoc[MT#DatabaseColumnNameImpl]
  ): Stringifier[MT] =
    new Stringifier[MT] {
      override def statement(stmt: Statement) = LazyToString(stmt.debugStr)
      override def from(from: From) = LazyToString(from.debugStr)
      override def expr(expr: Expr) = LazyToString(expr.debugStr)
    }
}
