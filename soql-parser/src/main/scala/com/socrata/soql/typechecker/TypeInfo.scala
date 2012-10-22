package com.socrata.soql.typechecker

import com.socrata.collection.OrderedSet
import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.names.{TypeName, FunctionName}

trait FunctionTypeInfo[Type] {
  /** The set of all types a function can be declared to accept.  That is,
    * every real type except null.  It should be ordered by most-preferred
    * to least-preferred for null-disambiguation purposes. */
  def typeParameterUniverse: OrderedSet[Type]

  /** Return a function that can convert values of type `from` to values of
    * type `to`, if one exists and is allowed to do such a conversion implicitly.
    * @note This does not necessarily mean that the result's parameter type is equal to `from`
    *       or that its result type is equal to `to`, though they will accept or return types
    *       compatible with those types (in the sense of `canBePassedToWithoutConversion`) */
  def implicitConversions(from: Type, to: Type): Option[MonomorphicFunction[Type]]

  /** Test to see if `actual` is compatible with `expected` in that a function which expects
    * the latter type can accept the former.
    *
    * @note `actual` and `expected` are allowed to be the same. */
  def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean
}

trait TypeInfo[Type] extends FunctionTypeInfo[Type] {
  def booleanLiteralType(b: Boolean): Type
  def stringLiteralType(s: String): Type
  def numberLiteralType(n: BigDecimal): Type
  def nullLiteralType: Type

  def functionsWithArity(name: FunctionName, n: Int): Set[Function[Type]]

  def typeFor(name: TypeName): Option[Type]

  def typeNameFor(typ: Type): TypeName

  def getCastFunction(from: Type, to: Type): Option[MonomorphicFunction[Type]]
}
