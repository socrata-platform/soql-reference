package com.socrata.soql.types

import com.ibm.icu.util.CaseInsensitiveString

import com.socrata.soql.environment.TypeName

sealed abstract class TestType(val name: TypeName, val isOrdered: Boolean) {
  def this(name: String, isOrdered: Boolean) = this(TypeName(name), isOrdered)
  override final def toString = name.toString

  def real = true

  def isPassableTo(that: TestType): Boolean = (this == that)
  def canonical: TestType = this
}

object TestType {
  // FIXME: Figure out a way to DRY this
  val typesByName = Seq(
    TestText, TestBoolean, TestNumber, TestMoney, TestDouble, TestFixedTimestamp, TestFloatingTimestamp,
    TestObject, TestArray, TestBlob, TestLocation,
    TestPoint, TestMultiPoint, TestLine, TestMultiLine, TestPolygon, TestMultiPolygon
  ).foldLeft(Map.empty[TypeName, TestType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }
}

case object TestText extends TestType("text", isOrdered = true)
case object TestBoolean extends TestType("boolean", isOrdered = true)
case object TestNumber extends TestType("number", isOrdered = true)
case object TestMoney extends TestType("money", isOrdered = true)
case object TestDouble extends TestType("double", isOrdered = true)
case object TestFixedTimestamp extends TestType("fixed_timestamp", isOrdered = true)
case object TestFloatingTimestamp extends TestType("floating_timestamp", isOrdered = true)
case object TestObject extends TestType("object", isOrdered = false)
case object TestArray extends TestType("array", isOrdered = false)
case object TestJson extends TestType("json", isOrdered = false)
case object TestBlob extends TestType("blob", isOrdered = false)
case object TestLocation extends TestType("location", isOrdered = false)
case object TestPoint extends TestType("point", isOrdered = false)
case object TestMultiPoint extends TestType("multi_point", isOrdered = false)
case object TestLine extends TestType("line", isOrdered = false)
case object TestMultiLine extends TestType("multi_line", isOrdered = false)
case object TestPolygon extends TestType("polygon", isOrdered = false)
case object TestMultiPolygon extends TestType("multi_polygon", isOrdered = false)

case object TestNull extends TestType("null", isOrdered = false) {
  override def isPassableTo(that: TestType) = true
}

sealed abstract class FakeTestType(name: String) extends TestType(name, isOrdered = false) {
  override def real = false
  def realType: TestType
  override def canonical = realType
}

case class TestTextLiteral(text: CaseInsensitiveString) extends FakeTestType("*text") {
  override def isPassableTo(that: TestType) = super.isPassableTo(that) || that == TestText
  def realType = TestText
}
object TestTextLiteral {
  def apply(s: String): TestTextLiteral = apply(new CaseInsensitiveString(s))
}

case class TestNumberLiteral(number: BigDecimal) extends FakeTestType("*number") {
  override def isPassableTo(that: TestType) = super.isPassableTo(that) || that == TestNumber
  def realType = TestNumber
}
