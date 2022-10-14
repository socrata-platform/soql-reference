package com.socrata.soql.analyzer2.expression

import scala.util.parsing.input.Position

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

trait FuncallLikeImpl[+CT, +CV] { this: FuncallLike[CT, CV] =>
  type Self[+CT, +CV] <: FuncallLike[CT, CV]

  val function: MonomorphicFunction[CT]
  val functionNamePosition: Position
}
