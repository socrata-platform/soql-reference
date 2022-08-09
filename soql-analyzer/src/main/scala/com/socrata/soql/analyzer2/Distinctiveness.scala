package com.socrata.soql.analyzer2

sealed trait Distinctiveness[+CT, +CV] {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Distinctiveness[CT, CV]
  private[analyzer2] def doRelabel(state: RelabelState): Distinctiveness[CT, CV]
  def debugStr(sb: StringBuilder): StringBuilder
}
object Distinctiveness {
  case object Indistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
    private[analyzer2] def doRelabel(state: RelabelState) = this
    def debugStr(sb: StringBuilder): StringBuilder = sb
  }

  case object FullyDistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
    private[analyzer2] def doRelabel(state: RelabelState) = this
    def debugStr(sb: StringBuilder): StringBuilder = sb.append("DISTINCT ")
  }

  case class On[+CT, +CV](exprs: Seq[Expr[CT, CV]]) extends Distinctiveness[CT, CV] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
      On(exprs.map(_.doRewriteDatabaseNames(state)))
    private[analyzer2] def doRelabel(state: RelabelState) =
      On(exprs.map(_.doRelabel(state)))
    def debugStr(sb: StringBuilder): StringBuilder = {
      sb.append("DISTINCT ON (")
      var didOne = false
      for(expr <- exprs) {
        if(didOne) sb.append(", ")
        else didOne = true
        expr.debugStr(sb)
      }
      sb.append(") ")
    }
  }
}
