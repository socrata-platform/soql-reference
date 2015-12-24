package com.socrata.soql.functions

import com.socrata.soql.types._

object SoQLTypeClasses {
  val Ordered = Set[SoQLType](
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLID,
    SoQLVersion
  )

  val Equatable = Ordered ++ Set[SoQLType](
    SoQLBlob,
    SoQLLocation
  )

  val NumLike = Set[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  val RealNumLike = Set[SoQLType](SoQLNumber, SoQLDouble)
  val GeospatialLike = Set[SoQLType](SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine, SoQLPolygon, SoQLMultiPolygon)
}
