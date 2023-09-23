package com.socrata.soql.analyzer2
package rewrite

trait RewritePassHelpers[MT <: MetaTypes] extends StatementUniverse[MT] {
  def isLiteralTrue(e: Expr): Boolean
  def isOrderable(e: CT): Boolean
  def and: MonomorphicFunction
}
