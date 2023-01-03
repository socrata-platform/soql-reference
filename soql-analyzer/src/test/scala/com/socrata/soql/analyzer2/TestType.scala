package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
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

  implicit object serialize extends Readable[TestType] with Writable[TestType] {
    def writeTo(buffer: WriteBuffer, typ: TestType): Unit = {
      buffer.write(typ.name)
    }

    def readFrom(buffer: ReadBuffer): TestType = {
      val name = buffer.read[TypeName]()
      typesByName.get(name) match {
        case Some(t) => t
        case None => fail("Unknown type " + JString(name.name))
      }
    }
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

  implicit object serialize extends Readable[TestValue] with Writable[TestValue] {
    def writeTo(buffer: WriteBuffer, v: TestValue): Unit = {
      v match {
        case TestText(s) =>
          buffer.write(0)
          buffer.write(s)
        case TestBoolean(b) =>
          buffer.write(1)
          buffer.write(b)
        case TestNumber(n) =>
          buffer.write(2)
          buffer.write(n)
        case TestUnorderable(s) =>
          buffer.write(3)
          buffer.write(s)
        case TestNull =>
          buffer.write(4)
      }
    }

    def readFrom(buffer: ReadBuffer): TestValue = {
      buffer.read[Int]() match {
        case 0 => TestText(buffer.read[String]())
        case 1 => TestBoolean(buffer.read[Boolean]())
        case 2 => TestNumber(buffer.read[Int]())
        case 3 => TestUnorderable(buffer.read[String]())
        case 4 => TestNull
        case other => fail("Unknown value tag " + other)
      }
    }
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

case class TestUnorderable(value: String) extends TestValue {
  def typ = TestUnorderable
  def doc = Doc(JString(value).toString)
}
object TestUnorderable extends TestType("unorderable", isOrdered = false)

case object TestNull extends TestType("null", isOrdered = false) with TestValue {
  override def isPassableTo(that: TestType) = true
  def typ = this
  def doc = d"NULL"
}
