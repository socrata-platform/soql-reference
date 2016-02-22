package com.socrata.soql.types


import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.ibm.icu.util.CaseInsensitiveString
import com.rojoma.json.v3.ast.{JValue, JArray, JObject}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, JsonKey}
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.obfuscation.{Obfuscator, CryptProvider}
import com.vividsolutions.jts.geom.{MultiLineString, MultiPolygon, Point, Polygon, MultiPoint, LineString}
import org.joda.time.{LocalTime, LocalDate, LocalDateTime, DateTime}
import org.joda.time.format.ISODateTimeFormat

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
    SoQLDate, SoQLTime, SoQLObject, SoQLArray, SoQLJson, SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine,
    SoQLPolygon, SoQLMultiPolygon, SoQLBlob, SoQLLocation
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
    SoQLPoint,
    SoQLMultiPoint,
    SoQLLine,
    SoQLMultiLine,
    SoQLPolygon,
    SoQLMultiPolygon,
    SoQLLocation,
    SoQLObject,
    SoQLArray,
    SoQLID,
    SoQLVersion,
    SoQLJson,
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
}

sealed trait SoQLValue {
  def typ: SoQLType
}

case class SoQLID(value: Long) extends SoQLValue {
  def typ = SoQLID
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
}
case object SoQLText extends SoQLType("text")

case class SoQLBoolean(value: Boolean) extends SoQLValue {
  def typ = SoQLBoolean
}
case object SoQLBoolean extends SoQLType("boolean") {
  val canonicalTrue = SoQLBoolean(true)
  val canonicalFalse = SoQLBoolean(false)
  def canonicalValue(b: Boolean) = if(b) canonicalTrue else canonicalFalse
}

case class SoQLNumber(value: java.math.BigDecimal) extends SoQLValue {
  def typ = SoQLNumber
}
case object SoQLNumber extends SoQLType("number")

case class SoQLMoney(value: java.math.BigDecimal) extends SoQLValue {
  def typ = SoQLMoney
}
case object SoQLMoney extends SoQLType("money")

case class SoQLDouble(value: Double) extends SoQLValue {
  def typ = SoQLDouble
}
case object SoQLDouble extends SoQLType("double")

case class SoQLFixedTimestamp(value: DateTime) extends SoQLValue {
  def typ = SoQLFixedTimestamp
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

case class SoQLObject(value: JObject) extends SoQLValue {
  def typ = SoQLObject
}
case object SoQLObject extends SoQLType("object")

case class SoQLArray(value: JArray) extends SoQLValue {
  def typ = SoQLArray
}
case object SoQLArray extends SoQLType("array")

case class SoQLJson(value: JValue) extends SoQLValue {
  def typ = SoQLJson
}
case object SoQLJson extends SoQLType("json")

case class SoQLPoint(value: Point) extends SoQLValue {
  def typ = SoQLPoint
}
case object SoQLPoint extends {
  protected val Treified = classOf[Point]
} with SoQLType("point") with SoQLGeometryLike[Point]

case class SoQLMultiLine(value: MultiLineString) extends SoQLValue {
  def typ = SoQLMultiLine
}
case object SoQLMultiLine extends {
  protected val Treified = classOf[MultiLineString]
} with SoQLType("multiline") with SoQLGeometryLike[MultiLineString]

case class SoQLMultiPolygon(value: MultiPolygon) extends SoQLValue {
  def typ = SoQLMultiPolygon
}
case object SoQLMultiPolygon extends {
  protected val Treified = classOf[MultiPolygon]
} with SoQLType("multipolygon") with SoQLGeometryLike[MultiPolygon]

case class SoQLPolygon(value: Polygon) extends SoQLValue {
  def typ = SoQLPolygon
}
case object SoQLPolygon extends {
  protected val Treified = classOf[Polygon]
} with SoQLType("polygon") with SoQLGeometryLike[Polygon]

case class SoQLMultiPoint(value: MultiPoint) extends SoQLValue {
  def typ = SoQLMultiPoint
}
case object SoQLMultiPoint extends {
  protected val Treified = classOf[MultiPoint]
} with SoQLType("multipoint") with SoQLGeometryLike[MultiPoint]

case class SoQLLine(value: LineString) extends SoQLValue {
  def typ = SoQLLine
}
case object SoQLLine extends {
  protected val Treified = classOf[LineString]
} with SoQLType("line") with SoQLGeometryLike[LineString]

case class SoQLBlob(value: String) extends SoQLValue {
  def typ = SoQLBlob
}
case object SoQLBlob extends SoQLType("blob")

case class SoQLLocation(latitude: Option[java.math.BigDecimal],
                        longitude: Option[java.math.BigDecimal],
                        @JsonKey("human_address") address: Option[String]
) extends SoQLValue {
  def typ = SoQLLocation
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
}
