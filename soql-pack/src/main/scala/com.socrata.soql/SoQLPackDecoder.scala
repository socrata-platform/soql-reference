package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry
import java.math.BigDecimal
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime, LocalDate, LocalTime}
import org.velvia.MsgPackUtils._
import scala.util.Try

/**
 * The decoders decode from elements of the Seq[Any] decoded by msgpack4s from
 * a binary array corresponding to a row of SoQLPack.
 */
object SoQLPackDecoder {
  type Decoder = Any => Option[SoQLValue]
  val decoderByType: Map[SoQLType, Decoder] = Map(
    SoQLPoint        -> (decodeGeom(SoQLPoint, _: Any).map(x => SoQLPoint(x))),
    SoQLMultiLine    -> (decodeGeom(SoQLMultiLine, _: Any).map(x => SoQLMultiLine(x))),
    SoQLMultiPolygon -> (decodeGeom(SoQLMultiPolygon, _: Any).map(x => SoQLMultiPolygon(x))),
    SoQLPolygon      -> (decodeGeom(SoQLPolygon, _: Any).map(x => SoQLPolygon(x))),
    SoQLLine         -> (decodeGeom(SoQLLine, _: Any).map(x => SoQLLine(x))),
    SoQLMultiPoint   -> (decodeGeom(SoQLMultiPoint, _: Any).map(x => SoQLMultiPoint(x))),
    SoQLText         -> decodeBinaryString _,
    SoQLNull         -> (x => Some(SoQLNull)),
    SoQLBoolean      -> decodeBoolean _,
    SoQLID           -> (x => decodeLong(x).map(SoQLID(_))),
    SoQLVersion      -> (x => decodeLong(x).map(SoQLVersion(_))),
    SoQLNumber       -> (x => decodeBigDecimal(x).map(SoQLNumber(_))),
    SoQLMoney        -> (x => decodeBigDecimal(x).map(SoQLMoney(_))),
    SoQLDouble       -> (x => Try(x.asInstanceOf[Double]).toOption.map(SoQLDouble(_))),
    SoQLFixedTimestamp -> (x => decodeDateTime(x).map(SoQLFixedTimestamp(_))),
    SoQLFloatingTimestamp -> (x => decodeDateTime(x).map(t => SoQLFloatingTimestamp(new LocalDateTime(t)))),
    SoQLDate         -> (x => decodeDateTime(x).map(t => SoQLDate(new LocalDate(t)))),
    SoQLTime         -> (x => decodeLong(x).map(t => SoQLTime(LocalTime.fromMillisOfDay(t.toInt))))
  )

  def decodeLong(item: Any): Option[Long] = Try(getLong(item)).toOption

  def decodeGeom[T <: Geometry](typ: SoQLGeometryLike[T], item: Any): Option[T] =
    typ.WkbRep.unapply(item.asInstanceOf[Array[Byte]])

  // msgpack4s sometimes does not decode binary as string, when a flag is set
  def decodeBinaryString(item: Any): Option[SoQLValue] =
    Try(SoQLText(new String(item.asInstanceOf[Array[Byte]], "UTF-8"))).toOption

  def decodeBoolean(item: Any): Option[SoQLValue] =
    Some(SoQLBoolean(item.asInstanceOf[Boolean]))

  def decodeBigDecimal(item: Any): Option[BigDecimal] = item match {
    case Seq(scale: Byte, bytes: Array[Byte]) =>
      Some(new BigDecimal(new java.math.BigInteger(bytes), scale.toInt))
    case Seq(scale: Short, bytes: Array[Byte]) =>
      Some(new BigDecimal(new java.math.BigInteger(bytes), scale.toInt))
    case Seq(scale: Int, bytes: Array[Byte]) =>
      Some(new BigDecimal(new java.math.BigInteger(bytes), scale))
    case other: Any => None
  }

  def decodeDateTime(item: Any): Option[DateTime] = item match {
    case Seq(millis: Any) =>
      Some(new DateTime(getLong(millis), DateTimeZone.UTC))
    case Seq(millis: Any, tz: Byte) =>
      Some(new DateTime(getLong(millis), DateTimeZone.forOffsetMillis(tz * SoQLPackEncoder.FifteenMinMillis)))
    case Seq(millis: Any, tz: Byte, chrono: Byte) =>
      val zone = DateTimeZone.forOffsetMillis(tz * SoQLPackEncoder.FifteenMinMillis)
      val chronos = SoQLPackEncoder.Chronologies(chrono).withZone(zone)
      Some(new DateTime(getLong(millis), chronos))
    case other: Any => None
  }
}