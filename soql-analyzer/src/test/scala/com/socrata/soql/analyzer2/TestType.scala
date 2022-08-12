package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.prettyprint.prelude._

import com.socrata.soql.environment.TypeName
import com.socrata.soql.typechecker.HasDoc

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

  implicit object jEncode extends JsonEncode[TestType] {
    def encode(t: TestType) = JString(t.name.name)
  }
}

sealed trait TestValue {
  def typ: TestType
  def doc: Doc[Nothing]
}
object TestValue {
  implicit object hasDoc extends HasDoc[TestValue] {
    def docOf(v: TestValue) = v.doc
  }
}

case class TestText(value: String) extends TestValue {
  def typ = TestText
  def doc = Doc(JString(value).toString)
}
object TestText extends TestType("text", isOrdered = true)

case class TestBoolean(value: Boolean) extends TestValue {
  def typ = TestBoolean
  def doc = if(value) d"true" else d"false"
}
object TestBoolean extends TestType("boolean", isOrdered = true) {
  val canonicalTrue = new TestBoolean(true)
  val canonicalFalse = new TestBoolean(false)
  def apply(b: Boolean) = if(b) canonicalTrue else canonicalFalse
}

case class TestNumber(num: Int) extends TestValue {
  def typ = TestNumber
  def doc = Doc(num)
}
object TestNumber extends TestType("number", isOrdered = true)

case object TestNull extends TestType("null", isOrdered = false) with TestValue {
  override def isPassableTo(that: TestType) = true
  def typ = this
  def doc = d"NULL"
}
