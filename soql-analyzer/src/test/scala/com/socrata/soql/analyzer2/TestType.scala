package com.socrata.soql.analyzer2

import com.socrata.soql.environment.TypeName

sealed abstract class TestType(val name: TypeName, val isOrdered: Boolean) {
  def this(name: String, isOrdered: Boolean) = this(TypeName(name), isOrdered)
  override final def toString = name.toString

  def isPassableTo(that: TestType): Boolean = this == that
}

object TestType {
  // FIXME: Figure out a way to DRY this
  val typesByName = Seq(
    TestText, TestBoolean, TestNumber
  ).foldLeft(Map.empty[TypeName, TestType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }
}

sealed trait TestValue {
  def typ: TestType
}

case class TestText(value: String) extends TestValue {
  def typ = TestText
}
object TestText extends TestType("text", isOrdered = true)

case class TestBoolean(value: Boolean) extends TestValue {
  def typ = TestBoolean
}
object TestBoolean extends TestType("boolean", isOrdered = true) {
  val canonicalTrue = new TestBoolean(true)
  val canonicalFalse = new TestBoolean(false)
  def apply(b: Boolean) = if(b) canonicalTrue else canonicalFalse
}

case class TestNumber(num: Int) extends TestValue {
  def typ = TestNumber
}
object TestNumber extends TestType("number", isOrdered = true)

case object TestNull extends TestType("null", isOrdered = false) with TestValue {
  override def isPassableTo(that: TestType) = true
  def typ = this
}
