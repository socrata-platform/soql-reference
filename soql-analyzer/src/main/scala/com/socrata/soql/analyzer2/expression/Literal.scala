package com.socrata.soql.analyzer2.expression

import scala.language.higherKinds
import com.socrata.soql.analyzer2._

trait LiteralImpl[+CT, +CV] { this: Literal[CT, CV] =>
  type Self[+CT, +CV] <: Literal[CT, CV]

  def isAggregated = false
  def isWindowed = false

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    Map.empty

  private[analyzer2] def findIsomorphism[CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: Expr[CT2, CV2]): Boolean = {
    this == that
  }

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] = Some(this).filter(predicate)
}
