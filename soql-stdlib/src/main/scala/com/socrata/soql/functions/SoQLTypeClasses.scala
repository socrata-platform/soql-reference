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

  val GeospatialLike = Set[SoQLType](SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine, SoQLPolygon, SoQLMultiPolygon)

  val Equatable = Ordered ++ GeospatialLike ++ Set[SoQLType](
    SoQLBlob,
    SoQLPhone,
    SoQLPhoto,
    SoQLLocation,
    SoQLUrl,
    SoQLJson
  )

  val NumLike = Set[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  val RealNumLike = Set[SoQLType](SoQLNumber, SoQLDouble)
  val TimestampLike = Set[SoQLType](SoQLFixedTimestamp, SoQLFloatingTimestamp)
}
