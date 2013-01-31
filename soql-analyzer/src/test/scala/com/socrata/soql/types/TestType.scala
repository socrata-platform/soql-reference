package com.socrata.soql.types

import com.ibm.icu.util.CaseInsensitiveString

import com.socrata.soql.environment.TypeName

sealed abstract class TestType(val name: TypeName) {
  def this(name: String) = this(TypeName(name))
  override final def toString = name.toString

  def real = true

  def isPassableTo(that: TestType): Boolean = (this == that)
  def canonical: TestType = this
}

object TestType {
  // FIXME: Figure out a way to DRY this
  val typesByName = Seq(
    TestText, TestBoolean, TestNumber, TestMoney, TestDouble, TestFixedTimestamp, TestFloatingTimestamp, TestObject, TestArray, TestLocation
  ).foldLeft(Map.empty[TypeName, TestType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }
}

case object TestText extends TestType("text")
case object TestBoolean extends TestType("boolean")
case object TestNumber extends TestType("number")
case object TestMoney extends TestType("money")
case object TestDouble extends TestType("double")
case object TestFixedTimestamp extends TestType("fixed_timestamp")
case object TestFloatingTimestamp extends TestType("floating_timestamp")
case object TestObject extends TestType("object")
case object TestArray extends TestType("array")
case object TestLocation extends TestType("location")
case object TestJson extends TestType("json")

case object TestNull extends TestType("null") {
  override def isPassableTo(that: TestType) = true
}

sealed abstract class FakeTestType(name: String) extends TestType(name) {
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
