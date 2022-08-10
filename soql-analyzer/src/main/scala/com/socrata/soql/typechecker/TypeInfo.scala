package com.socrata.soql.typechecker

import com.socrata.soql.ast.{Hole, Literal}
import com.socrata.soql.environment.TypeName
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

trait TypeInfoCommon[Type, Value] {
  /** The set of all types a function can be declared to accept.  That is,
    * every real type except null.  It should be ordered by most-preferred
    * to least-preferred for null-disambiguation purposes. */
  def typeParameterUniverse: OrderedSet[Type]

  def typeOf(value: Value): Type
  def typeNameFor(typ: Type): TypeName

  def typeFor(name: TypeName): Option[Type]

  def isOrdered(typ: Type): Boolean
  def isBoolean(typ: Type): Boolean
  def isGroupable(typ: Type): Boolean

  def boolType: Type
}

trait TypeInfo[Type, Value] extends TypeInfoCommon[Type, Value] {
  def booleanLiteralExpr(b: Boolean, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def stringLiteralExpr(s: String, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def numberLiteralExpr(n: BigDecimal, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def nullLiteralExpr(pos: Position): Seq[CoreExpr[Nothing, Type]]

  def literalExprFor(value: Value, pos: Position): Option[CoreExpr[Nothing, Type]]
}

trait TypeInfo2[Type, Value] extends TypeInfoCommon[Type, Value] {
  val hasType: HasType[Value, Type]
  def typeParameterUniverse: OrderedSet[Type]
  def potentialExprs(l: Literal): Seq[analyzer2.Expr[Type, Value]]
  def literalBoolean(b: Boolean, position: Position): analyzer2.Expr[Type, Value]
}
