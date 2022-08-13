package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.typechecker.HasDoc

sealed trait Distinctiveness[+CT, +CV] {
  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Distinctiveness[CT, CV]
  private[analyzer2] def doRelabel(state: RelabelState): Distinctiveness[CT, CV]
  def debugDoc(implicit ev: HasDoc[CV]): Option[Doc[Annotation[Nothing, CT]]]
}
object Distinctiveness {
  case object Indistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
    private[analyzer2] def doRelabel(state: RelabelState) = this
    def debugDoc(implicit ev: HasDoc[Nothing]) = None
  }

  case object FullyDistinct extends Distinctiveness[Nothing, Nothing] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this
    private[analyzer2] def doRelabel(state: RelabelState) = this
    def debugDoc(implicit ev: HasDoc[Nothing]) = Some(d"DISTINCT")
  }

  case class On[+CT, +CV](exprs: Seq[Expr[CT, CV]]) extends Distinctiveness[CT, CV] {
    private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
      On(exprs.map(_.doRewriteDatabaseNames(state)))
    private[analyzer2] def doRelabel(state: RelabelState) =
      On(exprs.map(_.doRelabel(state)))
    def debugDoc(implicit ev: HasDoc[CV]) =
      Some(exprs.map(_.debugDoc).encloseNesting(d"DISTINCT ON (", d",", d")"))
  }
}
