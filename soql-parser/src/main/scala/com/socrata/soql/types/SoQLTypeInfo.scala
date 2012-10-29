package com.socrata.soql.types

import com.socrata.soql.names._
import com.socrata.soql.functions._
import com.socrata.soql.typechecker._

object SoQLTypeInfo extends TypeInfo[SoQLType] {
  def booleanLiteralType(b: Boolean) = SoQLBoolean

  def stringLiteralType(s: String) = SoQLTextLiteral(s)

  def numberLiteralType(n: BigDecimal) = SoQLNumberLiteral(n)

  def nullLiteralType = SoQLNull

  def isAggregate(function: MonomorphicFunction[SoQLType]) = false

  def functionsWithArity(name: FunctionName, n: Int) =
    SoQLFunctions.functionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            Set.empty
        }
      case None =>
        Set.empty
    }

  def typeFor(name: TypeName) =
    SoQLType.typesByName.get(name)

  def typeNameFor(typ: SoQLType): TypeName = typ.name

  def canonicalize(typ: SoQLType) = typ.canonical

  def typeParameterUniverse = SoQLTypeConversions.typeParameterUniverse

  def implicitConversions(from: SoQLType, to: SoQLType) = SoQLTypeConversions.implicitConversions(from, to)

  def canBePassedToWithoutConversion(actual: SoQLType, expected: SoQLType) = SoQLTypeConversions.canBePassedToWithoutConversion(actual, expected)
}
