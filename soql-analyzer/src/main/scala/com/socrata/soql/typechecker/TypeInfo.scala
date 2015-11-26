package com.socrata.soql.typechecker

import com.socrata.soql.environment.TypeName
import com.socrata.soql.collection.OrderedSet

trait TypeInfo[Type] {
  def booleanLiteralType(b: Boolean): Type
  def stringLiteralType(s: String): Type
  def numberLiteralType(n: BigDecimal): Type
  def nullLiteralType: Type

  def typeFor(name: TypeName): Option[Type]

  def typeNameFor(typ: Type): TypeName

  def canonicalize(typ: Type): Type

  def isOrdered(typ: Type): Boolean

  /** The set of all types a function can be declared to accept.  That is,
    * every real type except null.  It should be ordered by most-preferred
    * to least-preferred for null-disambiguation purposes. */
  def typeParameterUniverse: OrderedSet[Type]

  /** Test to see if `actual` is compatible with `expected` in that a function which expects
    * the latter type can accept the former.
    *
    * @note `actual` and `expected` are allowed to be the same. */
  def canBePassedToWithoutConversion(actual: Type, expected: Type): Boolean
}
