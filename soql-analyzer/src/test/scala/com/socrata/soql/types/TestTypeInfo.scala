package com.socrata.soql.types

import com.socrata.soql.environment.TypeName
import com.socrata.soql.typechecker.TypeInfo

object TestTypeInfo extends TypeInfo[TestType] {
  def booleanLiteralType(b: Boolean) = TestBoolean

  def stringLiteralType(s: String) = TestTextLiteral(s)

  def numberLiteralType(n: BigDecimal) = TestNumberLiteral(n)

  def nullLiteralType = TestNull

  def typeFor(name: TypeName) =
    TestType.typesByName.get(name)

  def typeNameFor(typ: TestType): TypeName = typ.name

  def canonicalize(typ: TestType) = typ.canonical

  def typeParameterUniverse = TestTypeConversions.typeParameterUniverse

  def canBePassedToWithoutConversion(actual: TestType, expected: TestType) = TestTypeConversions.canBePassedToWithoutConversion(actual, expected)
}
