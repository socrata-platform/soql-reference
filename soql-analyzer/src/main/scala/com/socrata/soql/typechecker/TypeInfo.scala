package com.socrata.soql.typechecker

import com.socrata.soql.ast
import com.socrata.soql.ast.{Hole, Literal}
import com.socrata.soql.environment.{TypeName, Provenance}
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

trait TypeInfoCommon[Type, Value] {
  /** The set of all types a function can be declared to accept.  That is,
    * every real type except null.  It should be ordered by most-preferred
    * to least-preferred for null-disambiguation purposes. */
  def typeParameterUniverse: OrderedSet[Type]
  lazy val typeParameterUniverseIndices = typeParameterUniverse.iterator.zipWithIndex.toMap

  def typeOf(value: Value): Type
  def typeNameFor(typ: Type): TypeName

  def typeFor(name: TypeName): Option[Type]

  def isOrdered(typ: Type): Boolean
  def isBoolean(typ: Type): Boolean
  def isGroupable(typ: Type): Boolean
}

trait TypeInfo[Type, Value] extends TypeInfoCommon[Type, Value] {
  def booleanLiteralExpr(b: Boolean, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def stringLiteralExpr(s: String, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def numberLiteralExpr(n: BigDecimal, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def nullLiteralExpr(pos: Position): Seq[CoreExpr[Nothing, Type]]

  def literalExprFor(value: Value, pos: Position): Option[CoreExpr[Nothing, Type]]
}

trait TypeInfo2[MT <: analyzer2.MetaTypes] extends analyzer2.StatementUniverse[MT] with TypeInfoCommon[MT#ColumnType, MT#ColumnValue] {
  def potentialExprs(l: ast.Literal, currentSource: Option[ScopedResourceName], currentPrimaryTable: Option[Provenance]): Seq[Expr]
  def literalBoolean(b: Boolean, currentSource: Source): Expr

  def boolType: CT

  def updateProvenance(value: CV)(f: Provenance => Provenance): CV

  def hasType: analyzer2.HasType[CV, CT]
}
