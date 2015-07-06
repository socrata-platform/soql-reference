package com.socrata.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Geometry

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
    SoQLDouble       -> { case SoQLDouble(dbl) => dbl }
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
}