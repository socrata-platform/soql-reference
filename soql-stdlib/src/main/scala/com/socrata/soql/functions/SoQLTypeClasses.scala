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
    SoQLVersion,
    SoQLUrl
  )

  val SingleGeospatialLike = Set[SoQLType](SoQLPoint, SoQLLine, SoQLPolygon)

  val MultiGeospatialLike = Set[SoQLType](SoQLMultiPoint, SoQLMultiLine, SoQLMultiPolygon)

  val GeospatialLike = MultiGeospatialLike ++ SingleGeospatialLike

  val Equatable = Ordered ++ GeospatialLike ++ Set[SoQLType](
    SoQLBlob,
    SoQLPhone,
    SoQLLocation,
    SoQLUrl
  )

  val NumLike = Set[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  val RealNumLike = Set[SoQLType](SoQLNumber, SoQLDouble)
}
