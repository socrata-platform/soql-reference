package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry
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
    SoQLVersion      -> (x => decodeLong(x).map(SoQLVersion(_)))
  )

  def decodeLong(item: Any): Option[Long] = Try(getLong(item)).toOption

  def decodeGeom[T <: Geometry](typ: SoQLGeometryLike[T], item: Any): Option[T] =
    typ.WkbRep.unapply(item.asInstanceOf[Array[Byte]])

  // msgpack4s sometimes does not decode binary as string, when a flag is set
  def decodeBinaryString(item: Any): Option[SoQLValue] =
    Try(SoQLText(new String(item.asInstanceOf[Array[Byte]], "UTF-8"))).toOption

  def decodeBoolean(item: Any): Option[SoQLValue] =
    Some(SoQLBoolean(item.asInstanceOf[Boolean]))
}