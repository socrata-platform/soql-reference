package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.{JValue, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError, FieldEncode, FieldDecode}
import com.rojoma.json.v3.util.{SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, TagToValue, WrapperJsonEncode, WrapperJsonDecode, WrapperFieldEncode}

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable, Version}

class LabelProvider extends Cloneable {
  private var tables = 0
  private var columns = 0
  private var ctes = 0

  def tableLabel(): AutoTableLabel = {
    tables += 1
    new AutoTableLabel(tables)
  }
  def columnLabel(): AutoColumnLabel = {
    columns += 1
    new AutoColumnLabel(columns)
  }
  def cteLabel(): AutoCTELabel = {
    ctes += 1
    new AutoCTELabel(ctes)
  }

  override def clone(): LabelProvider =
    super.clone().asInstanceOf[LabelProvider]
}

object LabelProvider {
  implicit object serialize extends Writable[LabelProvider] with Readable[LabelProvider] {
    def writeTo(buffer: WriteBuffer, lp: LabelProvider): Unit = {
      buffer.write(lp.tables)
      buffer.write(lp.columns)
      buffer.write(lp.ctes)
    }

    def readFrom(buffer: ReadBuffer): LabelProvider = {
      val result = new LabelProvider

      buffer.version match {
        case Version.V6 =>
          result.tables = buffer.read[Int]()
          result.columns = buffer.read[Int]()
          result.ctes = 0
        case Version.V7 =>
          result.tables = buffer.read[Int]()
          result.columns = buffer.read[Int]()
          result.ctes = buffer.read[Int]()
      }

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

final class AutoTableLabel private[analyzer2] (val name: Int) {
  override def toString = s"t${LabelProvider.subscript(name)}"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case atl: AutoTableLabel => this.name == atl.name
      case _ => false
    }

  def debugDoc: Doc[Nothing] = Doc(toString)
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

  implicit object fCodec extends FieldEncode[AutoTableLabel] with FieldDecode[AutoTableLabel] {
    def encode(v: AutoTableLabel) = v.name.toString
    def decode(s: String) =
      try {
        val n = BigInt(s)
        if(n.isValidInt) {
          Right(new AutoTableLabel(n.intValue))
        } else {
          Left(DecodeError.InvalidField(s))
        }
      } catch {
        case _ : NumberFormatException => Left(DecodeError.InvalidField(s))
      }
  }

  implicit object serialize extends Writable[AutoTableLabel] with Readable[AutoTableLabel] {
    def writeTo(buffer: WriteBuffer, a: AutoTableLabel) =
      buffer.write(a.name)

    def readFrom(buffer: ReadBuffer) =
      new AutoTableLabel(buffer.read[Int]())
  }

  implicit object ordering extends Ordering[AutoTableLabel] {
    def compare(a: AutoTableLabel, b: AutoTableLabel) =
      a.name.compare(b.name)
  }
}

sealed abstract class ColumnLabel[+T] {
  def debugDoc(implicit ev: HasDoc[T]): Doc[Nothing]
}

final class AutoColumnLabel private[analyzer2] (val name: Int) extends ColumnLabel[Nothing] {
  override def toString = s"c${LabelProvider.subscript(name)}"

  def debugDoc(implicit ev: HasDoc[Nothing]): Doc[Nothing] = Doc(toString)

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

  implicit object ordering extends Ordering[AutoColumnLabel] {
    def compare(a: AutoColumnLabel, b: AutoColumnLabel) =
      a.name.compare(b.name)
  }
}

final case class DatabaseColumnName[T](name: T) extends ColumnLabel[T] {
  def debugDoc(implicit ev: HasDoc[T]) = ev.docOf(name)
}

object DatabaseColumnName {
  implicit def jEncode[T: JsonEncode] = WrapperJsonEncode[DatabaseColumnName[T]](_.name)
  implicit def jDecode[T: JsonDecode] = WrapperJsonDecode[DatabaseColumnName[T]](DatabaseColumnName[T](_))

  implicit def fEncode[T: FieldEncode] = WrapperFieldEncode[DatabaseColumnName[T]]({ dcn => FieldEncode[T].encode(dcn.name) })
  implicit def fDecode[T: FieldDecode] = new FieldDecode[DatabaseColumnName[T]] {
    override def decode(s: String) = FieldDecode[T].decode(s).map(DatabaseColumnName(_))
  }

  implicit def serialize[T : Writable] = new Writable[DatabaseColumnName[T]] {
    def writeTo(buffer: WriteBuffer, d: DatabaseColumnName[T]) =
      buffer.write(d.name)
  }

  implicit def deserialize[T : Readable] = new Readable[DatabaseColumnName[T]] {
    def readFrom(buffer: ReadBuffer) =
      DatabaseColumnName(buffer.read[T]())
  }

  implicit def ordering[T](implicit ordering: Ordering[T]): Ordering[DatabaseColumnName[T]] =
    new Ordering[DatabaseColumnName[T]] {
      def compare(a: DatabaseColumnName[T], b: DatabaseColumnName[T]) =
        ordering.compare(a.name, b.name)
    }
}

object ColumnLabel {
  implicit def jEncode[T : JsonEncode] = SimpleHierarchyEncodeBuilder[ColumnLabel[T]](TagToValue)
    .branch[AutoColumnLabel]("auto")
    .branch[DatabaseColumnName[T]]("dcn")
    .build

  implicit def jDecode[T : JsonDecode] = SimpleHierarchyDecodeBuilder[ColumnLabel[T]](TagToValue)
    .branch[AutoColumnLabel]("auto")
    .branch[DatabaseColumnName[T]]("dcn")
    .build


  implicit def serialize[T : Writable] = new Writable[ColumnLabel[T]] {
    def writeTo(buffer: WriteBuffer, label: ColumnLabel[T]): Unit = {
      label match {
        case a: AutoColumnLabel =>
          buffer.write(0)
          buffer.write(a)
        case d: DatabaseColumnName[T] =>
          buffer.write(1)
          buffer.write(d)
      }
    }
  }

  implicit def deserialize[T : Readable] = new Readable[ColumnLabel[T]] {
    def readFrom(buffer: ReadBuffer): ColumnLabel[T] = {
      buffer.read[Int]() match {
        case 0 =>
          buffer.read[AutoColumnLabel]()
        case 1 =>
          buffer.read[DatabaseColumnName[T]]()
        case other =>
          fail("Unknown column label type " + other)
      }
    }
  }

  implicit def ordering[T](implicit ordering: Ordering[T]): Ordering[ColumnLabel[T]] =
    new Ordering[ColumnLabel[T]] {
      val dcnOrdering = implicitly[Ordering[DatabaseColumnName[T]]]
      def compare(a: ColumnLabel[T], b: ColumnLabel[T]) =
        (a, b) match {
          case (a: DatabaseColumnName[T], b: DatabaseColumnName[T]) => dcnOrdering.compare(a, b)
          case (a: DatabaseColumnName[T], b: AutoColumnLabel) => -1
          case (a: AutoColumnLabel, b: DatabaseColumnName[T]) => 1
          case (a: AutoColumnLabel, b: AutoColumnLabel) => AutoColumnLabel.ordering.compare(a, b)
        }
    }
}

final class AutoCTELabel private[analyzer2] (val name: Int) {
  override def toString = s"cte${LabelProvider.subscript(name)}"

  override def hashCode = name.hashCode
  override def equals(that: Any) =
    that match {
      case acl: AutoCTELabel => this.name == acl.name
      case _ => false
    }

  def debugDoc: Doc[Nothing] = Doc(toString)
}
object AutoCTELabel {
  def unapply(acl: AutoCTELabel): Some[Int] = Some(acl.name)

  def forTest(name: Int) = new AutoCTELabel(name)

  implicit object jCodec extends JsonEncode[AutoCTELabel] with JsonDecode[AutoCTELabel] {
    def encode(v: AutoCTELabel) = JNumber(v.name)
    def decode(x: JValue) = x match {
      case n: JNumber => decodeNum(n)
      case other => Left(DecodeError.InvalidType(expected = JNumber, got = other.jsonType))
    }
    private[analyzer2] def decodeNum(n: JNumber) =
      try {
        Right(new AutoCTELabel(n.toJBigDecimal.intValueExact))
      } catch {
        case _ : ArithmeticException =>
          Left(DecodeError.InvalidValue(n))
      }
  }

  implicit object fCodec extends FieldEncode[AutoCTELabel] with FieldDecode[AutoCTELabel] {
    def encode(v: AutoCTELabel) = v.name.toString
    def decode(s: String) =
      try {
        val n = BigInt(s)
        if(n.isValidInt) {
          Right(new AutoCTELabel(n.intValue))
        } else {
          Left(DecodeError.InvalidField(s))
        }
      } catch {
        case _ : NumberFormatException => Left(DecodeError.InvalidField(s))
      }
  }

  implicit object serialize extends Writable[AutoCTELabel] with Readable[AutoCTELabel] {
    def writeTo(buffer: WriteBuffer, a: AutoCTELabel) =
      buffer.write(a.name)

    def readFrom(buffer: ReadBuffer) =
      new AutoCTELabel(buffer.read[Int]())
  }

  implicit object ordering extends Ordering[AutoCTELabel] {
    def compare(a: AutoCTELabel, b: AutoCTELabel) =
      a.name.compare(b.name)
  }
}
