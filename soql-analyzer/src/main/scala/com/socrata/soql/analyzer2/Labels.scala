package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.{JValue, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2.serialization.{ReadBuffer, WriteBuffer, Readable, Writable}

class LabelProvider extends Cloneable {
  private var tables = 0
  private var columns = 0

  def tableLabel(): AutoTableLabel = {
    tables += 1
    new AutoTableLabel(tables)
  }
  def columnLabel(): AutoColumnLabel = {
    columns += 1
    new AutoColumnLabel(columns)
  }

  override def clone(): LabelProvider =
    super.clone().asInstanceOf[LabelProvider]
}

object LabelProvider {
  implicit object serialize extends Writable[LabelProvider] with Readable[LabelProvider] {
    def writeTo(buffer: WriteBuffer, lp: LabelProvider): Unit = {
      buffer.write(lp.tables)
      buffer.write(lp.columns)
    }

    def readFrom(buffer: ReadBuffer): LabelProvider = {
      val result = new LabelProvider
      result.tables = buffer.read[Int]()
      result.columns = buffer.read[Int]()
      result
    }
  }

  private[analyzer2] def subscript(n: Int): String = {
    val nStr = n.toString
    val sb = new StringBuilder(nStr.length)
    for(c <- nStr) {
      sb.append(
        c match {
          case '-' => '\u208b'
          case '0' => '\u2080'
          case '1' => '\u2081'
          case '2' => '\u2082'
          case '3' => '\u2083'
          case '4' => '\u2084'
          case '5' => '\u2085'
          case '6' => '\u2086'
          case '7' => '\u2087'
          case '8' => '\u2088'
          case '9' => '\u2089'
          case _ => throw new Exception("unreachable")
        }
      )
    }
    sb.toString()
  }
}

sealed abstract class TableLabel {
  def debugDoc: Doc[Nothing] = Doc(toString)
}
object TableLabel {
  implicit object jCodec extends JsonEncode[TableLabel] with JsonDecode[TableLabel] {
    def encode(v: TableLabel) = v match {
      case a: AutoTableLabel => AutoTableLabel.jCodec.encode(a)
      case d: DatabaseTableName => DatabaseTableName.jCodec.encode(d)
    }

    def decode(v: JValue) = v match {
      case n: JNumber => AutoTableLabel.jCodec.decodeNum(n)
      case s: JString => DatabaseTableName.jCodec.decodeStr(s)
      case other => Left(DecodeError.join(Seq(
                                            DecodeError.InvalidType(expected = JNumber, got = other.jsonType),
                                            DecodeError.InvalidType(expected = JString, got = other.jsonType))))
    }
  }

  implicit object serialize extends Writable[TableLabel] with Readable[TableLabel] {
    def writeTo(buffer: WriteBuffer, label: TableLabel): Unit = {
      label match {
        case a: AutoTableLabel =>
          buffer.write(0)
          AutoTableLabel.serialize.writeTo(buffer, a)
        case d: DatabaseTableName =>
          buffer.write(1)
          DatabaseTableName.serialize.writeTo(buffer, d)
      }
    }

    def readFrom(buffer: ReadBuffer): TableLabel = {
      buffer.read[Int]() match {
        case 0 =>
          AutoTableLabel.serialize.readFrom(buffer)
        case 1 =>
          DatabaseTableName.serialize.readFrom(buffer)
        case other =>
          fail("Unknown table label type " + other)
      }
    }
  }
}

final class AutoTableLabel private[analyzer2] (private val name: Int) extends TableLabel {
  override def toString = s"t${LabelProvider.subscript(name)}"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case atl: AutoTableLabel => this.name == atl.name
      case _ => false
    }
}
object AutoTableLabel {
  def unapply(atl: AutoTableLabel): Some[Int] = Some(atl.name)

  def forTest(name: Int) = new AutoTableLabel(name)

  implicit object jCodec extends JsonEncode[AutoTableLabel] with JsonDecode[AutoTableLabel] {
    def encode(v: AutoTableLabel) = JNumber(v.name)
    def decode(x: JValue) = x match {
      case n: JNumber => decodeNum(n)
      case other => Left(DecodeError.InvalidType(expected = JNumber, got = other.jsonType))
    }
    private[analyzer2] def decodeNum(n: JNumber) =
      try {
        Right(new AutoTableLabel(n.toJBigDecimal.intValueExact))
      } catch {
        case _ : ArithmeticException =>
          Left(DecodeError.InvalidValue(n))
      }
  }

  implicit object serialize extends Writable[AutoTableLabel] with Readable[AutoTableLabel] {
    def writeTo(buffer: WriteBuffer, a: AutoTableLabel) =
      buffer.write(a.name)

    def readFrom(buffer: ReadBuffer) =
      new AutoTableLabel(buffer.read[Int]())
  }
}

final case class DatabaseTableName(name: String) extends TableLabel {
  override def toString = name
}
object DatabaseTableName {
  implicit object jCodec extends JsonEncode[DatabaseTableName] with JsonDecode[DatabaseTableName] {
    def encode(v: DatabaseTableName) = JString(v.name)
    def decode(x: JValue) = x match {
      case s: JString => decodeStr(s)
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
    private[analyzer2] def decodeStr(s: JString) =
      Right(DatabaseTableName(s.string))
  }

  implicit object serialize extends Writable[DatabaseTableName] with Readable[DatabaseTableName] {
    def writeTo(buffer: WriteBuffer, d: DatabaseTableName) =
      buffer.write(d.name)

    def readFrom(buffer: ReadBuffer) =
      DatabaseTableName(buffer.read[String]())
  }
}

sealed abstract class ColumnLabel {
  def debugDoc: Doc[Nothing] = Doc(toString)
}
object ColumnLabel {
  implicit object jCodec extends JsonEncode[ColumnLabel] with JsonDecode[ColumnLabel] {
    def encode(v: ColumnLabel) = v match {
      case a: AutoColumnLabel => AutoColumnLabel.jCodec.encode(a)
      case d: DatabaseColumnName => DatabaseColumnName.jCodec.encode(d)
    }

    def decode(v: JValue) = v match {
      case n: JNumber => AutoColumnLabel.jCodec.decodeNum(n)
      case s: JString => DatabaseColumnName.jCodec.decodeStr(s)
      case other => Left(DecodeError.join(Seq(
                                            DecodeError.InvalidType(expected = JNumber, got = other.jsonType),
                                            DecodeError.InvalidType(expected = JString, got = other.jsonType))))
    }
  }

  implicit object serialize extends Writable[ColumnLabel] with Readable[ColumnLabel] {
    def writeTo(buffer: WriteBuffer, label: ColumnLabel): Unit = {
      label match {
        case a: AutoColumnLabel =>
          buffer.write(0)
          AutoColumnLabel.serialize.writeTo(buffer, a)
        case d: DatabaseColumnName =>
          buffer.write(1)
          DatabaseColumnName.serialize.writeTo(buffer, d)
      }
    }

    def readFrom(buffer: ReadBuffer): ColumnLabel = {
      buffer.read[Int]() match {
        case 0 =>
          AutoColumnLabel.serialize.readFrom(buffer)
        case 1 =>
          DatabaseColumnName.serialize.readFrom(buffer)
        case other =>
          fail("Unknown column label type " + other)
      }
    }
  }
}

final class AutoColumnLabel private[analyzer2] (private val name: Int) extends ColumnLabel {
  override def toString = s"c${LabelProvider.subscript(name)}"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case acl: AutoColumnLabel => this.name == acl.name
      case _ => false
    }
}
object AutoColumnLabel {
  def unapply(atl: AutoColumnLabel): Some[Int] = Some(atl.name)

  def forTest(name: Int) = new AutoColumnLabel(name)

  implicit object jCodec extends JsonEncode[AutoColumnLabel] with JsonDecode[AutoColumnLabel] {
    def encode(v: AutoColumnLabel) = JNumber(v.name)
    def decode(x: JValue) = x match {
      case n: JNumber => decodeNum(n)
      case other => Left(DecodeError.InvalidType(expected = JNumber, got = other.jsonType))
    }
    private[analyzer2] def decodeNum(n: JNumber) =
      try {
        Right(new AutoColumnLabel(n.toJBigDecimal.intValueExact))
      } catch {
        case _ : ArithmeticException =>
          Left(DecodeError.InvalidValue(n))
      }
  }

  implicit object serialize extends Writable[AutoColumnLabel] with Readable[AutoColumnLabel] {
    def writeTo(buffer: WriteBuffer, a: AutoColumnLabel) =
      buffer.write(a.name)

    def readFrom(buffer: ReadBuffer) =
      new AutoColumnLabel(buffer.read[Int]())
  }
}

final case class DatabaseColumnName(name: String) extends ColumnLabel {
  override def toString = name
}
object DatabaseColumnName {
  implicit object jCodec extends JsonEncode[DatabaseColumnName] with JsonDecode[DatabaseColumnName] {
    def encode(v: DatabaseColumnName) = JString(v.name)
    def decode(x: JValue) = x match {
      case s: JString => decodeStr(s)
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
    private[analyzer2] def decodeStr(s: JString) =
      Right(DatabaseColumnName(s.string))
  }

  implicit object serialize extends Writable[DatabaseColumnName] with Readable[DatabaseColumnName] {
    def writeTo(buffer: WriteBuffer, d: DatabaseColumnName) =
      buffer.write(d.name)

    def readFrom(buffer: ReadBuffer) =
      DatabaseColumnName(buffer.read[String]())
  }
}
