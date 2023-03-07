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
}

trait TypeInfo[Type, Value] extends TypeInfoCommon[Type, Value] {
  def booleanLiteralExpr(b: Boolean, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def stringLiteralExpr(s: String, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def numberLiteralExpr(n: BigDecimal, pos: Position): Seq[CoreExpr[Nothing, Type]]
  def nullLiteralExpr(pos: Position): Seq[CoreExpr[Nothing, Type]]

  def literalExprFor(value: Value, pos: Position): Option[CoreExpr[Nothing, Type]]
}

trait TypeInfo2[Type, Value] extends TypeInfoCommon[Type, Value] {
  def metaProject[MT <: analyzer2.MetaTypes](
    implicit typeEv: Type =:= MT#ColumnType,
    typeEvRev: MT#ColumnType =:= Type,
    valueEv: Value =:= MT#ColumnValue,
    valueEvRev: MT#ColumnValue =:= Value
  ): TypeInfoMetaProjection[MT]
}

abstract class TypeInfoMetaProjection[MT <: analyzer2.MetaTypes] extends analyzer2.MetaTypeHelper[MT] with TypeInfoCommon[MT#ColumnType, MT#ColumnValue] {
  val unproject: TypeInfo2[CT, CV]

  val hasType: analyzer2.HasType[CV, CT]

  def potentialExprs(l: Literal, currentPrimaryTable: Option[analyzer2.CanonicalName]): Seq[analyzer2.Expr[MT]]
  def literalBoolean(b: Boolean, position: Position): analyzer2.Expr[MT]

  def boolType: CT

  final override def typeParameterUniverse: OrderedSet[CT] = unproject.typeParameterUniverse
  final override def typeOf(value: CV): CT = unproject.typeOf(value)
  final override def typeNameFor(typ: CT): TypeName = unproject.typeNameFor(typ)
  final override def typeFor(name: TypeName): Option[CT] = unproject.typeFor(name)
  final override def isOrdered(typ: CT): Boolean = unproject.isOrdered(typ)
  final override def isBoolean(typ: CT): Boolean = unproject.isBoolean(typ)
  final override def isGroupable(typ: CT): Boolean = unproject.isGroupable(typ)
}
