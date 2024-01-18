package com.socrata.soql.sqlizer

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.AutomaticJsonEncodeBuilder

import com.socrata.prettyprint.prelude._
import com.socrata.soql.environment.{TypeName, Provenance}
import com.socrata.soql.analyzer2.HasDoc
import com.socrata.soql.types.SoQLID

sealed abstract class TestType(val name: TypeName, val isOrdered: Boolean) {
  def this(name: String, isOrdered: Boolean) = this(TypeName(name), isOrdered)
  override final def toString = name.toString

  def isPassableTo(that: TestType): Boolean = this == that
}

object TestType {
  // FIXME: Figure out a way to DRY this
  val typesByName = Seq(
    TestID, TestText, TestBoolean, TestNumber, TestCompound
  ).foldLeft(Map.empty[TypeName, TestType]) { (acc, typ) =>
    acc + (typ.name -> typ)
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

case class TestID(value: Long) extends TestValue {
  var provenance: Option[Provenance] = None

  def typ = TestID
  def doc = Doc(SoQLID.FormattedButUnobfuscatedStringRep(SoQLID(value)))
}
object TestID extends TestType("id", isOrdered = true)

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

case class TestNumber(num: Double) extends TestValue {
  def typ = TestNumber
  def doc = Doc(num.toString)
}
object TestNumber extends TestType("number", isOrdered = true)

case class TestCompound(a: Option[String], b: Option[Double]) extends TestValue {
  def typ = TestCompound
  def doc = Doc(JString(TestCompound.jEncode.encode(this).toString).toString)
}
object TestCompound extends TestType("compound", isOrdered = true) {
  private val jEncode = AutomaticJsonEncodeBuilder[TestCompound]
}
