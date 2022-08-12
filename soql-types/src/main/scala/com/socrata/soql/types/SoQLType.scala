package com.socrata.soql.types

import java.net.{URI, URISyntaxException}

import com.socrata.prettyprint.prelude._
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.ibm.icu.util.CaseInsensitiveString
import com.rojoma.json.v3.ast.{JArray, JObject, JValue, JString}
import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey, JsonUtil}
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.obfuscation.{CryptProvider, Obfuscator}
import com.vividsolutions.jts.geom.{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime, Period}
import org.joda.time.format.ISODateTimeFormat

import scala.xml.SAXParseException

sealed abstract class SoQLType(n: String) {
  val name = TypeName(n)

  override final def toString = name.toString
  def isPassableTo(that: SoQLType) = this == that

  def t: SoQLType = this // Various things are invariant in SoQLType for good reasons; this avoids sprinkling ": SoQLType" all over the place
}

object SoQLType {
  // FIXME: Figure out a way to DRY this.  It's pretty easy in Scala 2.10, but
  // I still want to retain pre-2.10 compat.
  val typesByName = Seq(
    SoQLID, SoQLVersion, SoQLText, SoQLBoolean, SoQLNumber, SoQLMoney, SoQLDouble, SoQLFixedTimestamp, SoQLFloatingTimestamp,
    SoQLDate, SoQLTime, SoQLInterval, SoQLObject, SoQLArray, SoQLJson, SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine,
    SoQLPolygon, SoQLMultiPolygon, SoQLBlob,
    SoQLPhone, SoQLLocation, SoQLUrl, SoQLDocument, SoQLPhoto
  ).foldLeft(Map.empty[TypeName, SoQLType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }

  // The ordering here is used to define disambiguation rules; types that appear earlier will be preferred
  // to types that appear later.
  val typePreferences = Seq[SoQLType](
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLInterval,
    SoQLPoint,
    SoQLMultiPoint,
    SoQLLine,
    SoQLMultiLine,
    SoQLPolygon,
    SoQLMultiPolygon,
    SoQLPhone,
    SoQLLocation,
    SoQLUrl,
    SoQLObject,
    SoQLArray,
    SoQLID,
    SoQLVersion,
    SoQLJson,
    SoQLDocument,
    SoQLPhoto,
    SoQLBlob
  )

  assert(typePreferences.toSet == typesByName.values.toSet, "Mismatch in typesByName and typePreferences: " + (typePreferences.toSet -- typesByName.values.toSet) + " " + (typesByName.values.toSet -- typePreferences.toSet))

  object Serialization {
    def serialize(out: CodedOutputStream, t: SoQLType) =
      out.writeStringNoTag(t.name.name)
    def deserialize(in: CodedInputStream): SoQLType = {
      val name = TypeName(in.readString())
      SoQLType.typesByName(name) // post-analysis, the fake types are gone
    }
  }

  implicit object jEncode extends JsonEncode[SoQLType] {
    def encode(t: SoQLType) = JString(t.name.name)
  }

  implicit object jDecode extends JsonDecode[SoQLType] {
    def decode(x: JValue) = x match {
      case JString(name) => typesByName.get(TypeName(name)).toRight(DecodeError.InvalidValue(got = x))
      case other => Left(DecodeError.InvalidType(expected = JString, got = other.jsonType))
    }
  }
}

sealed trait SoQLValue {
  def typ: SoQLType
  def doc(cryptProvider: CryptProvider): Doc[Nothing]
}

case class SoQLID(value: Long) extends SoQLValue {
  def typ = SoQLID

  def doc(cryptProvider: CryptProvider) =
    Doc(new SoQLID.StringRep(cryptProvider)(this))
}
case object SoQLID extends SoQLType("row_identifier") {
  private def prefix = "row-"

  class StringRep(cryptProvider: CryptProvider) {
    private class IdObfuscator extends Obfuscator(prefix, cryptProvider) {
      def obfuscate(soqlId: SoQLID): String =
        encrypt(soqlId.value)
      def deobfuscate(obfuscatedId: String): Option[SoQLID] =
        decrypt(obfuscatedId).map(new SoQLID(_))
    }
    private val obfuscator = new IdObfuscator

    def unapply(text: String): Option[SoQLID] =
      obfuscator.deobfuscate(text)

    def unapply(text: CaseInsensitiveString): Option[SoQLID] =
      unapply(text.getString)

    def apply(soqlId: SoQLID) =
      obfuscator.obfuscate(soqlId)
  }

  /**
   * This exists for OBE compatibility reason.
   * @param unused
   */
  class ClearNumberRep(unused: CryptProvider) extends StringRep(unused) {
    override def unapply(text: String): Option[SoQLID] = {
      try {
        Some(new SoQLID(text.toLong))
      } catch {
        case ex: NumberFormatException =>
          None
      }
    }

    override def unapply(text: CaseInsensitiveString): Option[SoQLID] = unapply(text.getString)

    override def apply(soqlId: SoQLID) = soqlId.value.toString
  }

  def isPossibleId(s: String): Boolean = Obfuscator.isPossibleObfuscatedValue(s, prefix = prefix)
  def isPossibleId(s: CaseInsensitiveString): Boolean = isPossibleId(s.getString)
}

case class SoQLVersion(value: Long) extends SoQLValue {
  def typ = SoQLVersion

  def doc(cryptProvider: CryptProvider) =
    Doc(new SoQLVersion.StringRep(cryptProvider)(this))
}
case object SoQLVersion extends SoQLType("row_version") {
  private def prefix = "rv-"
  class StringRep(cryptProvider: CryptProvider) {
    private class VersionObfuscator extends Obfuscator(prefix, cryptProvider) {
      def obfuscate(version: SoQLVersion): String =
        encrypt(version.value)
      def deobfuscate(obfuscatedVersion: String): Option[SoQLVersion] =
        decrypt(obfuscatedVersion).map(new SoQLVersion(_))
    }
    private val obfuscator = new VersionObfuscator

    def unapply(text: String): Option[SoQLVersion] =
      obfuscator.deobfuscate(text)

    def unapply(text: CaseInsensitiveString): Option[SoQLVersion] =
      unapply(text.getString)

    def apply(soqlVersion: SoQLVersion) =
      obfuscator.obfuscate(soqlVersion)
  }
  def isPossibleVersion(s: String): Boolean = Obfuscator.isPossibleObfuscatedValue(s, prefix = prefix)
  def isPossibleVersion(s: CaseInsensitiveString): Boolean = isPossibleVersion(s.getString)
}

case class SoQLText(value: String) extends SoQLValue {
  def typ = SoQLText

  def doc(cryptProvider: CryptProvider) =
    Doc(JString(value).toString)
}
case object SoQLText extends SoQLType("text")

case class SoQLBoolean(value: Boolean) extends SoQLValue {
  def typ = SoQLBoolean

  def doc(cryptProvider: CryptProvider) =
    Doc(if(value) "true" else "false")
}
case object SoQLBoolean extends SoQLType("boolean") {
  val canonicalTrue = new SoQLBoolean(true)
  val canonicalFalse = new SoQLBoolean(false)
  def apply(b: Boolean) = if(b) canonicalTrue else canonicalFalse
  def canonicalValue(b: Boolean) = apply(b)
}

case class SoQLNumber(value: java.math.BigDecimal) extends SoQLValue {
  def typ = SoQLNumber

  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)

  override def equals(that: Any) =
    that match {
      case SoQLNumber(thatValue) => this.value.compareTo(thatValue) == 0
      case _ => false
    }

  override def hashCode = value.stripTrailingZeros.hashCode
}
case object SoQLNumber extends SoQLType("number")

case class SoQLMoney(value: java.math.BigDecimal) extends SoQLValue {
  def typ = SoQLMoney
  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)
}
case object SoQLMoney extends SoQLType("money")

case class SoQLDouble(value: Double) extends SoQLValue {
  def typ = SoQLDouble
  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)
}
case object SoQLDouble extends SoQLType("double")

case class SoQLFixedTimestamp(value: DateTime) extends SoQLValue {
  def typ = SoQLFixedTimestamp
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLFixedTimestamp.StringRep(value)).toString)
}
case object SoQLFixedTimestamp extends SoQLType("fixed_timestamp") {
  object StringRep {
    private val fixedParser = ISODateTimeFormat.dateTimeParser.withZoneUTC
    private val fixedRenderer = ISODateTimeFormat.dateTime.withZoneUTC

    def unapply(text: String): Option[DateTime] =
      try { Some(fixedParser.parseDateTime(text)) }
      catch { case _: IllegalArgumentException => None }

    def unapply(text: CaseInsensitiveString): Option[DateTime] = unapply(text.getString)

    def apply(dateTime: DateTime) =
      fixedRenderer.print(dateTime)

    def printTo(appendable: Appendable, dateTime: DateTime) =
      fixedRenderer.printTo(appendable, dateTime)
  }
}

case class SoQLFloatingTimestamp(value: LocalDateTime) extends SoQLValue {
  def typ = SoQLFloatingTimestamp
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLFloatingTimestamp.StringRep(value)).toString)
}
case object SoQLFloatingTimestamp extends SoQLType("floating_timestamp") {
  object StringRep {
    def unapply(text: String): Option[LocalDateTime] =
      try { Some(ISODateTimeFormat.localDateOptionalTimeParser.parseLocalDateTime(text)) }
      catch { case _: IllegalArgumentException => None }

    def unapply(text: CaseInsensitiveString): Option[LocalDateTime] = unapply(text.getString)

    def apply(dateTime: LocalDateTime) =
      ISODateTimeFormat.dateTime.print(dateTime)

    def printTo(appendable: Appendable, dateTime: LocalDateTime) =
      ISODateTimeFormat.dateTime.printTo(appendable, dateTime)
  }
}

case class SoQLDate(value: LocalDate) extends SoQLValue {
  def typ = SoQLDate
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLDate.StringRep(value)).toString)
}
case object SoQLDate extends SoQLType("date") {
  object StringRep {
    def unapply(text: String): Option[LocalDate] =
      try { Some(ISODateTimeFormat.localDateParser.parseLocalDate(text)) }
      catch { case _: IllegalArgumentException => None }

    def unapply(text: CaseInsensitiveString): Option[LocalDate] = unapply(text.getString)

    def apply(date: LocalDate) =
      ISODateTimeFormat.date.print(date)

    def printTo(appendable: Appendable, date: LocalDate) =
      ISODateTimeFormat.dateTime.printTo(appendable, date)
  }
}

case class SoQLTime(value: LocalTime) extends SoQLValue {
  def typ = SoQLTime
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLTime.StringRep(value)).toString)
}
case object SoQLTime extends SoQLType("time") {
  object StringRep {
    def unapply(text: String): Option[LocalTime] =
      try { Some(ISODateTimeFormat.localTimeParser.parseLocalTime(text)) }
      catch { case _: IllegalArgumentException => None }

    def unapply(text: CaseInsensitiveString): Option[LocalTime] = unapply(text.getString)

    def apply(time: LocalTime) =
      ISODateTimeFormat.time.print(time)

    def printTo(appendable: Appendable, time: LocalTime) =
      ISODateTimeFormat.dateTime.printTo(appendable, time)
  }
}

case class SoQLInterval(value: Period) extends SoQLValue {
  def typ = SoQLInterval
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLInterval.StringRep(value)).toString)
}

case object SoQLInterval extends SoQLType("interval") {
  object StringRep {
    def unapply(text: String): Option[Period] =
      try {
        Some(Period.parse(text)) // Accept ISO period format like P1Y2DT3H P=period, Y=year, D=day, T=time, H=hour
      }
      catch { case _: IllegalArgumentException => None }

    def unapply(text: CaseInsensitiveString): Option[Period] = unapply(text.getString)

    def apply(value: Period) = {
      value.toString
    }

    def printTo(appendable: Appendable, value: Period) = {
      appendable.append(apply(value))
    }
  }
}

case class SoQLObject(value: JObject) extends SoQLValue {
  def typ = SoQLObject
  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)
}
case object SoQLObject extends SoQLType("object")

case class SoQLArray(value: JArray) extends SoQLValue {
  def typ = SoQLArray
  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)
}
case object SoQLArray extends SoQLType("array")

case class SoQLJson(value: JValue) extends SoQLValue {
  def typ = SoQLJson
  def doc(cryptProvider: CryptProvider) =
    Doc(value.toString)
}
case object SoQLJson extends SoQLType("json")

case class SoQLPoint(value: Point) extends SoQLValue {
  def typ = SoQLPoint
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLPoint.WktRep(value)).toString)
}
case object SoQLPoint extends SoQLType("point") with SoQLGeometryLike[Point] {
  override protected val Treified = classOf[Point]
}

case class SoQLMultiLine(value: MultiLineString) extends SoQLValue {
  def typ = SoQLMultiLine
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLMultiLine.WktRep(value)).toString)
}
case object SoQLMultiLine extends SoQLType("multiline") with SoQLGeometryLike[MultiLineString] {
  override protected val Treified = classOf[MultiLineString]
}

case class SoQLMultiPolygon(value: MultiPolygon) extends SoQLValue {
  def typ = SoQLMultiPolygon
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLMultiPolygon.WktRep(value)).toString)
}
case object SoQLMultiPolygon extends SoQLType("multipolygon") with SoQLGeometryLike[MultiPolygon] {
  override protected val Treified = classOf[MultiPolygon]
}

case class SoQLPolygon(value: Polygon) extends SoQLValue {
  def typ = SoQLPolygon
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLPolygon.WktRep(value)).toString)
}
case object SoQLPolygon extends SoQLType("polygon") with SoQLGeometryLike[Polygon] {
  override protected val Treified = classOf[Polygon]
}

case class SoQLMultiPoint(value: MultiPoint) extends SoQLValue {
  def typ = SoQLMultiPoint
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLMultiPoint.WktRep(value)).toString)
}
case object SoQLMultiPoint extends SoQLType("multipoint") with SoQLGeometryLike[MultiPoint] {
  override protected val Treified = classOf[MultiPoint]
}

case class SoQLLine(value: LineString) extends SoQLValue {
  def typ = SoQLLine
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLLine.WktRep(value)).toString)
}
case object SoQLLine extends SoQLType("line") with SoQLGeometryLike[LineString] {
  override protected val Treified = classOf[LineString]
}

case class SoQLBlob(value: String) extends SoQLValue {
  def typ = SoQLBlob
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(value).toString)
}
case object SoQLBlob extends SoQLType("blob")

case class SoQLPhoto(value: String) extends SoQLValue {
  def typ = SoQLPhoto
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(value).toString)
}
case object SoQLPhoto extends SoQLType("photo")

case class SoQLLocation(latitude: Option[java.math.BigDecimal],
                        longitude: Option[java.math.BigDecimal],
                        @JsonKey("human_address") address: Option[String]
) extends SoQLValue {
  def typ = SoQLLocation
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLLocation.jCodec.encode(this).toString).toString)
}

case object SoQLLocation extends SoQLType("location") {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLLocation]

  def isPossibleLocation(s: String): Boolean = {
    try {
      JsonUtil.parseJson[SoQLLocation](s).isRight
    } catch {
      case ex: JsonReaderException => false
    }
  }

  def isPossibleLocation(s: CaseInsensitiveString): Boolean = isPossibleLocation(s.getString)
}

// SoQLNull should not be a type, but it's too deeply ingrained to change now.
case object SoQLNull extends SoQLType("null") with SoQLValue {
  override def isPassableTo(that: SoQLType) = true
  def typ = this
  def doc(cryptProvider: CryptProvider) = d"NULL"
}

case class SoQLPhone(@JsonKey("phone_number") phoneNumber: Option[String],
                     @JsonKey("phone_type") phoneType: Option[String]) extends SoQLValue {
  def typ = SoQLPhone
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLPhone.jCodec.encode(this).toString).toString)
}

case object SoQLPhone extends SoQLType("phone") {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLPhone]

  // Phone number can take almost anything.  But it does not take :, {, } to avoid confusion with phone type and json
  val phoneRx = "((?i)Home|Cell|Work|Fax|Other)?(: ?)?([^:{}]+)?".r

  def isPossible(s: String): Boolean = {
    s match {
      case phoneRx(pt, sep, pn) =>
        Option(pt).nonEmpty || Option(pn).nonEmpty
      case _ =>
        try {
          JsonUtil.parseJson[SoQLPhone](s).isRight
        } catch {
          case ex: JsonReaderException => false
        }
    }
  }

  def isPossible(s: CaseInsensitiveString): Boolean = isPossible(s.getString)
}

case class SoQLUrl(@JsonKey("url") url: Option[String],
                   @JsonKey("description") description: Option[String]) extends SoQLValue {
  def typ = SoQLUrl
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLUrl.jCodec.encode(this).toString).toString)
}

case object SoQLUrl extends SoQLType("url") {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLUrl]

  def isPossible(s: String): Boolean = parseUrl(s).isDefined

  def isPossible(s: CaseInsensitiveString): Boolean = isPossible(s.getString)

  /**
    * Tag and attribute must be lower case.
    */
  def parseUrl(value: String): Option[SoQLUrl] = {
    parseAsJson(value).orElse(parseAsUri(value)).orElse(parseAsHtmlTag(value))
  }

  private def parseAsJson(value: String): Option[SoQLUrl] = {
    try {
      JsonUtil.parseJson[SoQLUrl](value).toOption
    } catch {
      case ex: JsonReaderException => None
    }
  }

  private def parseAsUri(value: String): Option[SoQLUrl] = {
    try {
      val uri = new URI(value)
      Some(SoQLUrl(Some(value), None))
    } catch {
      case e: URISyntaxException => None
    }
  }

  private def parseAsHtmlTag(value: String): Option[SoQLUrl] = {
    try {
      xml.XML.loadString(value) match {
        case a @ <a>{_*}</a> =>
          Some(SoQLUrl(a.attribute("href").map(_.text),
            if (a.text.nonEmpty) Some(a.text) else None))
        case _ => None
      }
    } catch {
      case e: SAXParseException => None
    }
  }
}

case class SoQLDocument(@JsonKey("file_id") fileId: String,
                        @JsonKey("content_type") contentType: Option[String],
                        @JsonKey("filename") filename: Option[String]) extends SoQLValue {
  def typ = SoQLDocument
  def doc(cryptProvider: CryptProvider) =
    Doc(JString(SoQLDocument.jCodec.encode(this).toString).toString)
}

case object SoQLDocument extends SoQLType("document") {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLDocument]

  def isPossible(s: String): Boolean =  parseAsJson(s).isDefined

  def isPossible(s: CaseInsensitiveString): Boolean = isPossible(s.getString)

  private def parseAsJson(value: String): Option[SoQLDocument] = {
    try {
      JsonUtil.parseJson[SoQLDocument](value).toOption
    } catch {
      case ex: JsonReaderException => None
    }
  }
}
