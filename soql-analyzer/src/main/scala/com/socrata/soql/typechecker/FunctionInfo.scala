package com.socrata.soql.typechecker

import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.environment.FunctionName

trait FunctionInfo[Type] {
  /** Return a function that can convert values of type `from` to values of
    * type `to`, if one exists and is allowed to do such a conversion implicitly.
    * @note This does not necessarily mean that the result's parameter type is equal to `from`
    *       or that its result type is equal to `to`, though they will accept or return types
    *       compatible with those types (in the sense of `canBePassedToWithoutConversion`) */
  def implicitConversions(from: Type, to: Type): Option[MonomorphicFunction[Type]]

  def functionsWithArity(name: FunctionName, n: Int): Set[Function[Type]]
}
