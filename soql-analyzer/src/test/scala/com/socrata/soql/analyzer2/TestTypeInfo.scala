package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.typechecker.{TypeInfo2, HasType, TypeInfoMetaProjection}

object TestTypeInfo extends TypeInfo2[TestType, TestValue] {
  def metaProject[MT <: MetaTypes](
    implicit typeEv: TestType =:= MT#ColumnType,
    typeEvRev: MT#ColumnType =:= TestType,
    valueEv: TestValue =:= MT#ColumnValue,
    valueEvRev: MT#ColumnValue =:= TestValue
  ): TypeInfoMetaProjection[MT] =
    new TypeInfoMetaProjection[MT] {
      val unproject = TestTypeInfo.asInstanceOf[TypeInfo2[CT, CV]]

      implicit object hasType extends HasType[CV, CT] {
        def typeOf(v: CV) = v.typ
      }

      def boolType = TestBoolean

      def literalBoolean(b: Boolean, position: Position): Expr[MT] =
        LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(position))

      def potentialExprs(l: ast.Literal): Seq[Expr[MT]] =
        l match {
          case ast.StringLiteral(s) =>
            val asInt =
              try {
                Some(LiteralValue[MT](TestNumber(s.toInt))(new AtomicPositionInfo(l.position)))
              } catch {
                case _ : NumberFormatException => None
              }
            Seq(LiteralValue[MT](TestText(s))(new AtomicPositionInfo(l.position))) ++ asInt
          case ast.NumberLiteral(n) => Seq(LiteralValue[MT](TestNumber(n.toInt))(new AtomicPositionInfo(l.position)))
          case ast.BooleanLiteral(b) => Seq(LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(l.position)))
          case ast.NullLiteral() => typeParameterUniverse.iterator.map(NullLiteral[MT](_)(new AtomicPositionInfo(l.position))).toVector
        }
    }


  // Members declared in com.socrata.soql.typechecker.TypeInfoCommon

  def typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestBoolean,
    TestUnorderable
  )
  def isBoolean(typ: TestType): Boolean = typ == TestBoolean
  def isGroupable(typ: TestType): Boolean = true
  def isOrdered(typ: TestType): Boolean = typ.isOrdered
  def typeFor(name: TypeName): Option[TestType] = TestType.typesByName.get(name)
  def typeNameFor(typ: TestType): TypeName = typ.name
  def typeOf(value: TestValue): TestType = value.typ
}
