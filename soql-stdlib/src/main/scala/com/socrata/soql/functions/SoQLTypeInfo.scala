package com.socrata.soql.functions

import com.socrata.soql.environment.TypeName
import com.socrata.soql.types._
import com.socrata.soql.typechecker.TypeInfo

object SoQLTypeInfo extends TypeInfo[SoQLAnalysisType] {
  def booleanLiteralType(b: Boolean) = SoQLBoolean

  def stringLiteralType(s: String) = SoQLTextLiteral(s)

  def numberLiteralType(n: BigDecimal) = SoQLNumberLiteral(n)

  def nullLiteralType = SoQLNull

  def typeFor(name: TypeName) =
    SoQLType.typesByName.get(name)

  def typeNameFor(typ: SoQLAnalysisType): TypeName = typ.name

  def canonicalize(typ: SoQLAnalysisType) = typ.canonical

  def typeParameterUniverse = SoQLTypeConversions.typeParameterUniverse

  def canBePassedToWithoutConversion(actual: SoQLAnalysisType, expected: SoQLAnalysisType) =
    SoQLTypeConversions.canBePassedToWithoutConversion(actual, expected)

  def isOrdered(typ: SoQLAnalysisType): Boolean = SoQLTypeClasses.Ordered(typ.canonical)
}
