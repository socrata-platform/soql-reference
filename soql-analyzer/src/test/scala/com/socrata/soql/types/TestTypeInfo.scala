package com.socrata.soql.types

import com.socrata.soql.ast.{Hole, Literal}
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.typechecker.TypeInfo
import com.socrata.soql.typed

import scala.util.parsing.input.Position

object TestTypeInfo extends TypeInfo[TestType, SoQLValue] {
  val typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestDouble,
    TestMoney,
    TestBoolean,
    TestFixedTimestamp,
    TestFloatingTimestamp,
    TestLocation,
    TestObject,
    TestArray
  )

  def boolType = TestBoolean.t
  def booleanLiteralExpr(b: Boolean, pos: Position) = Seq(typed.BooleanLiteral(b, TestBoolean.t)(pos))

  def stringLiteralExpr(s: String, pos: Position) = Seq(typed.StringLiteral(s, TestText.t)(pos))

  def numberLiteralExpr(n: BigDecimal, pos: Position) = {
    val baseNum = typed.NumberLiteral(n, TestNumber.t)(pos)
    Seq(
      baseNum,
      typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(baseNum), None, None)(pos, pos),
      typed.FunctionCall(TestFunctions.NumberToDouble.monomorphic.get, Seq(baseNum), None, None)(pos, pos)
    )
  }

  def nullLiteralExpr(pos: Position) = typeParameterUniverse.toSeq.map(typed.NullLiteral(_)(pos))

  def typeFor(name: TypeName) =
    TestType.typesByName.get(name)

  def typeNameFor(typ: TestType): TypeName = typ.name

  def isOrdered(typ: TestType) = typ.isOrdered
  def isGroupable(typ: TestType) = typ != TestArray
  def isBoolean(typ: TestType) = typ == TestBoolean

  def typeOf(value: SoQLValue) =
    value.typ match { // hmph, annoying that this code uses SoQLValue instead of a TestValue
      case SoQLText => TestText
      case SoQLNumber => TestNumber
      case SoQLBoolean => TestBoolean
      case SoQLFixedTimestamp => TestFixedTimestamp
      case SoQLFloatingTimestamp => TestFloatingTimestamp
      case _ => throw new Exception("Need to add a case to TestTypeInfo#typeOf") // ick but whatever, if you run into this just add your case
    }
  def literalExprFor(value: SoQLValue, pos: Position) =
    value match {
      case SoQLText(s) => Some(typed.StringLiteral(s, TestText.t)(pos))
      case _ => None
    }
}
