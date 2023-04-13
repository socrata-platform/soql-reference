package com.socrata.soql.analyzer2.expression

import scala.language.higherKinds
import scala.util.parsing.input.Position

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

trait FuncallLikeImpl[MT <: MetaTypes] { this: FuncallLike[MT] =>
  type Self[MT <: MetaTypes] <: FuncallLike[MT]

  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position
}
