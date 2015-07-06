package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC

object SoQLPackEncoder {
  type Encoder = PartialFunction[SoQLValue, Any]

  val encoderByType: Map[SoQLType, Encoder] = Map(
    SoQLPoint        -> geomEncoder,
    SoQLMultiLine    -> geomEncoder,
    SoQLMultiPolygon -> geomEncoder,
    SoQLPolygon      -> geomEncoder,
    SoQLLine         -> geomEncoder,
    SoQLMultiPoint   -> geomEncoder,
    SoQLText         -> { case SoQLText(str) => str },
    SoQLBoolean      -> { case SoQLBoolean(bool) => bool },
    SoQLID           -> { case SoQLID(long) => long },
    SoQLVersion      -> { case SoQLVersion(long) => long },
    SoQLNumber       -> { case SoQLNumber(bd) => encodeBigDecimal(bd) },
    SoQLMoney        -> { case SoQLMoney(bd) => encodeBigDecimal(bd) },
    SoQLDouble       -> { case SoQLDouble(dbl) => dbl },
    SoQLFixedTimestamp -> { case SoQLFixedTimestamp(dt) => encodeDateTime(dt) },
    SoQLFloatingTimestamp -> { case SoQLFloatingTimestamp(ldt) => encodeDateTime(ldt.toDateTime(UTC)) },
    SoQLDate         -> { case SoQLDate(date) => encodeDateTime(date.toDateTimeAtStartOfDay(UTC)) },
    SoQLTime         -> { case SoQLTime(time) => time.getMillisOfDay }
  )

  lazy val geomEncoder: Encoder = {
    case SoQLPoint(pt)       => SoQLPoint.WkbRep(pt)
    case SoQLMultiLine(ml)   => SoQLMultiLine.WkbRep(ml)
    case SoQLMultiPolygon(p) => SoQLMultiPolygon.WkbRep(p)
    case SoQLPolygon(p)      => SoQLPolygon.WkbRep(p)
    case SoQLLine(l)         => SoQLLine.WkbRep(l)
    case SoQLMultiPoint(mp)  => SoQLMultiPoint.WkbRep(mp)
  }

  def encodeBigDecimal(bd: java.math.BigDecimal): Any =
    Array(bd.scale, bd.unscaledValue.toByteArray)

  import org.joda.time.chrono._

  val FifteenMinMillis = 15 * 60 * 1000

  val Chronologies = Array(ISOChronology.getInstance,
                           CopticChronology.getInstance,
                           EthiopicChronology.getInstance,
                           GregorianChronology.getInstance,
                           JulianChronology.getInstance,
                           IslamicChronology.getInstance,
                           BuddhistChronology.getInstance,
                           GJChronology.getInstance)
  val ChronoClasses = Chronologies.map(_.getClass)
  val IsoClass = ChronoClasses(0)

  // Encodes a DateTime with timezone and chronology.  The TimeZone info is not fully captured, rather
  // it is recorded as hh:mm offset in 15-minute granularity (-95 to 95).  It practically holds as much info
  // as ISO8601 date time string, which records +/-hh:mm.
  def encodeDateTime(dt: DateTime): Any = {
    val tz15min = dt.getZone.getOffset(0) / FifteenMinMillis
    (dt.getChronology.getClass, tz15min) match {
      case (IsoClass, 0) =>         // UTC timezone, standard chronology.  Encode as aray of len 1
        Array[Any](dt.getMillis)
      case (IsoClass, tz) =>        // Non-UTC timezone, standard chrono - encode as [millis, tz]
        Array[Any](dt.getMillis, tz)
      case (chronoClass, tz) =>               // Nonstandard everything - encode as [millis, tz, chronos]
        Array[Any](dt.getMillis, tz, ChronoClasses.indexOf(chronoClass))
    }
  }
}