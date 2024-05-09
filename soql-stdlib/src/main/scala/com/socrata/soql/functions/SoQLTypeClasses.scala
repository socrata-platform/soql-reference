package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.collection.CovariantSet

object SoQLTypeClasses {
  val Ordered = CovariantSet[SoQLType](
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

  val PointLike = CovariantSet[SoQLType](SoQLPoint, SoQLMultiPoint)
  val LineLike = CovariantSet[SoQLType](SoQLLine, SoQLMultiLine)
  val PolygonLike = CovariantSet[SoQLType](SoQLPolygon, SoQLMultiPolygon)

  val GeospatialLike = PointLike ++ LineLike ++ PolygonLike

  val Equatable = Ordered ++ GeospatialLike ++ Set[SoQLType](
    SoQLBlob,
    SoQLPhone,
    SoQLPhoto,
    SoQLLocation,
    SoQLUrl,
    SoQLJson
  )

  val NumLike = CovariantSet[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  val RealNumLike = CovariantSet[SoQLType](SoQLNumber, SoQLDouble)
  val TimestampLike = CovariantSet[SoQLType](SoQLFixedTimestamp, SoQLFloatingTimestamp)
}
