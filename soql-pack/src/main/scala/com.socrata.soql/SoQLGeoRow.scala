package com.socrata.soql

import com.rojoma.json.v3.ast._
import com.vividsolutions.jts.geom.Geometry
import types._

class SoQLGeoRow(underlying: Array[SoQLValue], parent: SoQLPackIterator)
    extends IndexedSeq[SoQLValue] {
  def geometry: Option[Geometry] = underlying(parent.geomIndex) match {
    case SoQLNull => None
    case SoQLMultiPolygon(mp) => Some(mp)
    case SoQLPolygon(p) => Some(p)
    case SoQLPoint(pt) => Some(pt)
    case SoQLMultiPoint(mp) => Some(mp)
    case SoQLLine(l) => Some(l)
    case SoQLMultiLine(ml) => Some(ml)
    case x: SoQLValue => throw new RuntimeException("Should not be seeing non-geom SoQL type " + x)
  }

  def properties: Map[String, JValue] = {
    val zipped = parent.colNames.zip(underlying).filterNot(_._1 == parent.geomName)

    val jValues = zipped.map { case (k, v) =>
      val jVal = v match {
        case SoQLText(str)   => JString(str)
        case SoQLNumber(bd)  => JNumber(bd)
        case SoQLDouble(dbl) => JNumber(dbl)
        case SoQLBoolean(b)  => JBoolean(b)
        case SoQLFixedTimestamp(dt) => JString(SoQLFixedTimestamp.StringRep(dt))
        case SoQLFloatingTimestamp(dt) => JString(SoQLFloatingTimestamp.StringRep(dt))
        case SoQLTime(dt) => JString(SoQLTime.StringRep(dt))
        case SoQLDate(dt) => JString(SoQLDate.StringRep(dt))
        case other: SoQLValue => JString(other.toString)
      }

      k -> jVal
    }

    jValues.toMap
  }
  def apply(idx: Int): SoQLValue = underlying(idx)
  def length: Int = underlying.length
}
