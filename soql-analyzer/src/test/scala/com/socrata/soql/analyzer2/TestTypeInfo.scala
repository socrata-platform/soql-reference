package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.{TypeName, Provenance}
import com.socrata.soql.typechecker.TypeInfo2

object TestTypeInfo {
  val typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestBoolean,
    TestUnorderable
  )
}

class TestTypeInfo[MT <: MetaTypes with ({ type ColumnType = TestType; type ColumnValue = TestValue })] extends TypeInfo2[MT] {
  implicit object hasType extends HasType[CV, CT] {
    def typeOf(v: CV) = v.typ
  }

  def boolType = TestBoolean

  def literalBoolean(b: Boolean, source: Option[ScopedResourceName], position: Position): Expr =
    LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(source, position))

  def potentialExprs(l: ast.Literal, source: Option[ScopedResourceName], currentPrimaryTable: Option[Provenance]): Seq[Expr] =
    l match {
      case ast.StringLiteral(s) =>
        val asInt =
          try {
            Some(LiteralValue[MT](TestNumber(s.toInt))(new AtomicPositionInfo(source, l.position)))
          } catch {
            case _ : NumberFormatException => None
          }
        Seq(LiteralValue[MT](TestText(s))(new AtomicPositionInfo(source, l.position))) ++ asInt
      case ast.NumberLiteral(n) => Seq(LiteralValue[MT](TestNumber(n.toInt))(new AtomicPositionInfo(source, l.position)))
      case ast.BooleanLiteral(b) => Seq(LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(source, l.position)))
      case ast.NullLiteral() => typeParameterUniverse.iterator.map(NullLiteral[MT](_)(new AtomicPositionInfo(source, l.position))).toVector
    }

  def updateProvenance(v: CV)(f: Provenance => Provenance) = v

  def typeParameterUniverse = TestTypeInfo.typeParameterUniverse
  def isBoolean(typ: TestType): Boolean = typ == TestBoolean
  def isGroupable(typ: TestType): Boolean = true
  def isOrdered(typ: TestType): Boolean = typ.isOrdered
  def typeFor(name: TypeName): Option[TestType] = TestType.typesByName.get(name)
  def typeNameFor(typ: TestType): TypeName = typ.name
  def typeOf(value: TestValue): TestType = value.typ
}
