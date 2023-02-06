package com.socrata.soql.analyzer2.expression

import scala.language.higherKinds
import com.socrata.soql.analyzer2._

trait LiteralImpl[MT <: MetaTypes] { this: Literal[MT] =>
  type Self[MT <: MetaTypes] <: Literal[MT]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    Map.empty

  private[analyzer2] def findIsomorphism(state: IsomorphismState, that: Expr[MT]): Boolean = {
    this == that
  }

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] = Some(this).filter(predicate)
}
