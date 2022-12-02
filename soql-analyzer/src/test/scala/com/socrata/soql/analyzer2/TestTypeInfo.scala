package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.typechecker.{TypeInfo2, HasType}

object TestTypeInfo extends TypeInfo2[TestType, TestValue] {
    implicit object hasType extends HasType[TestValue, TestType] {
      def typeOf(v: TestValue) = v.typ
    }

    def literalBoolean(b: Boolean, position: Position): Expr[TestType, TestValue] =
      LiteralValue(TestBoolean(b))(position)

    def potentialExprs(l: ast.Literal): Seq[Expr[TestType, TestValue]] =
      l match {
        case ast.StringLiteral(s) =>
          val asInt =
            try {
              Some(LiteralValue(TestNumber(s.toInt))(l.position))
            } catch {
              case _ : NumberFormatException => None
            }
          Seq(LiteralValue(TestText(s))(l.position)) ++ asInt
        case ast.NumberLiteral(n) => Seq(LiteralValue(TestNumber(n.toInt))(l.position))
        case ast.BooleanLiteral(b) => Seq(LiteralValue(TestBoolean(b))(l.position))
        case ast.NullLiteral() => typeParameterUniverse.iterator.map(NullLiteral(_)(l.position)).toVector
      }

    def typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
      TestText,
      TestNumber,
      TestBoolean
    )

    // Members declared in com.socrata.soql.typechecker.TypeInfoCommon
    def boolType: TestType = TestBoolean
    def isBoolean(typ: TestType): Boolean = typ == TestBoolean
    def isGroupable(typ: TestType): Boolean = true
    def isOrdered(typ: TestType): Boolean = true
    def typeFor(name: TypeName): Option[TestType] = TestType.typesByName.get(name)
    def typeNameFor(typ: TestType): TypeName = typ.name
    def typeOf(value: TestValue): TestType = value.typ
}
