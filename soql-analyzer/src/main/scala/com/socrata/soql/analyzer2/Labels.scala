package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.{JValue, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}

import com.socrata.prettyprint.prelude._

class LabelProvider {
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
}

object LabelProvider {
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
}
