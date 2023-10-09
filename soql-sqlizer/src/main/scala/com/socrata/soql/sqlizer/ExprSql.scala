package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

sealed abstract class ExprSql[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT] {
  def compressed: ExprSql.Compressed[MT]
  def expr: Expr
  def typ: CT = expr.typ
  def sqls: Seq[Doc]
  def databaseExprCount: Int
  def isExpanded: Boolean
  def withExpr(newExpr: Expr): ExprSql
}

abstract class ExprSqlFactory[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT] {
  def apply(sql: Doc, expr: Expr): ExprSql.Compressed[MT] =
    new ExprSql.Compressed(sql, expr)

  def apply(sqls: Seq[Doc], expr: Expr): ExprSql =
    if(sqls.lengthCompare(1) == 0) {
      new ExprSql.Compressed(sqls.head, expr)
    } else {
      new ExprSql.Expanded(this, sqls, expr)
    }

  def compress(expr: Option[Expr], rawSqls: Seq[Doc]): Doc
}

object ExprSql {
  class Compressed[MT <: MetaTypes with MetaTypesExt] private[sqlizer] (rawSql: Doc[SqlizeAnnotation[MT]], val expr: Expr[MT]) extends ExprSql[MT] {
    def compressed = this
    def sql = rawSql.annotate(SqlizeAnnotation.Expression(expr))
    def sqls = Seq(sql)
    def databaseExprCount = 1
    def isExpanded = false
    def withExpr(newExpr: Expr) = new Compressed(rawSql, newExpr)
  }

  class Expanded[MT <: MetaTypes with MetaTypesExt] private[sqlizer] (factory: ExprSqlFactory[MT], rawSqls: Seq[Doc[SqlizeAnnotation[MT]]], val expr: Expr[MT]) extends ExprSql[MT] {
    assert(rawSqls.lengthCompare(1) != 0)

    def sqls = rawSqls.map(_.annotate(SqlizeAnnotation.Expression(expr)))

    def compressed = {
      val compressedSql = factory.compress(Some(expr), rawSqls)

      new Compressed(
        compressedSql,
        expr
      )
    }

    def databaseExprCount = rawSqls.length

    def isExpanded = true

    def withExpr(newExpr: Expr) = new Expanded(factory, rawSqls, newExpr)
  }
}
