package com.socrata.soql.analysis

import types._
import com.socrata.soql.names._
import com.socrata.soql.DatasetContext
import util.parsing.input.Position
import com.socrata.collection.OrderedMap

class EndToEnd(val aliases: OrderedMap[ColumnName, typed.TypedFF[SoQLType]], val columns: Map[ColumnName, SoQLType])(implicit ctx: DatasetContext) extends Typechecker[SoQLType] with SoQLTypeConversions {
  def booleanLiteralType(b: Boolean) = SoQLBoolean

  def stringLiteralType(s: String) = SoQLTextLiteral(s)

  def numberLiteralType(n: BigDecimal) = SoQLNumberLiteral(n)

  def nullLiteralType = SoQLNull

  def isAggregate(function: MonomorphicFunction[SoQLType]) = false

  def functionsWithArity(name: FunctionName, n: Int, position: Position) =
    SoQLFunctions.functionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            throw new NoSuchFunction(name, position)
        }
      case None =>
        throw new NoSuchFunction(name, position)
    }

  def typeFor(name: TypeName, position: Position) =
    SoQLType.typesByName.get(name) match {
      case Some(typ) => typ
      case None => throw new UnknownType(position)
    }

  def getCastFunction(from: SoQLType, to: SoQLType, position: Position) = {
    throw new IncompatibleType(position)
  }
}
