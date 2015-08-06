package com.socrata.soql

import com.rojoma.json.v3.ast._
import com.vividsolutions.jts.geom.Geometry
import types._

class SoQLGeoRow(underlying: Array[SoQLValue], parent: SoQLPackIterator)
    extends IndexedSeq[SoQLValue] {
  def geometry: Geometry = underlying(parent.geomIndex) match {
    case SoQLMultiPolygon(mp) => mp
    case SoQLPolygon(p) => p
    case SoQLPoint(pt) => pt
    case SoQLMultiPoint(mp) => mp
    case SoQLLine(l) => l
    case SoQLMultiLine(ml) => ml
    case x: SoQLValue => throw new RuntimeException("Should not be seeing non-geom SoQL type" + x)
  }

  def properties: Map[String, JValue] = {
    val jValues = underlying.map {
      case SoQLText(str)   => JString(str)
      case SoQLNumber(bd)  => JNumber(bd)
      case SoQLMoney(bd)   => JNumber(bd)
      case SoQLDouble(dbl) => JNumber(dbl)
      case SoQLBoolean(b)  => JBoolean(b)
      case SoQLFixedTimestamp(dt) => JString(SoQLFixedTimestamp.StringRep(dt))
      case SoQLFloatingTimestamp(dt) => JString(SoQLFloatingTimestamp.StringRep(dt))
      case SoQLTime(dt) => JString(SoQLTime.StringRep(dt))
      case SoQLDate(dt) => JString(SoQLDate.StringRep(dt))
      case other: SoQLValue => JString(other.toString)
    }

    parent.colNames.zip(jValues).filterNot(_._1 == parent.geomName).toMap
  }
  def apply(idx: Int): SoQLValue = underlying.apply(idx)
  def length: Int = underlying.length
}
