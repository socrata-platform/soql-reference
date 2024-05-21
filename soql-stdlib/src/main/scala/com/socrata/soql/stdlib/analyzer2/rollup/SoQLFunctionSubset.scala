package com.socrata.soql.stdlib.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.SimpleFunctionSubset
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.types.{SoQLType, SoQLValue}

class SoQLFunctionSubset[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends SimpleFunctionSubset[MT]
{
  private val monomorphicMap = locally {
    import SoQLFunctions._
    Seq(
      // out of a YMD timestamp truncation, we can further truncate
      (FloatingTimeStampTruncYm, FloatingTimeStampTruncYmd),
      (FloatingTimeStampTruncY, FloatingTimeStampTruncYmd),
      // .. or extract one of the date-related fields
      (FloatingTimestampDateField, FloatingTimeStampTruncYmd),
      (FloatingTimestampYearField, FloatingTimeStampTruncYmd),
      (FloatingTimeStampExtractY, FloatingTimeStampTruncYmd),
      (FloatingTimestampMonthField, FloatingTimeStampTruncYmd),
      (FloatingTimeStampExtractM, FloatingTimeStampTruncYmd),
      (FloatingTimestampDayField, FloatingTimeStampTruncYmd),
      (FloatingTimeStampExtractD, FloatingTimeStampTruncYmd),
      (FloatingTimestampDayOfWeekField, FloatingTimeStampTruncYmd),
      (FloatingTimeStampExtractDow, FloatingTimeStampTruncYmd),
      (FloatingTimestampWeekOfYearField, FloatingTimeStampTruncYmd),
      (FloatingTimeStampExtractWoy, FloatingTimeStampTruncYmd),
      (FloatingTimestampIsoYearField, FloatingTimeStampTruncYmd),
      (FloatingTimestampExtractIsoY, FloatingTimeStampTruncYmd),
      // out of a YM timestamp truncation, we can further truncate
      (FloatingTimeStampTruncY, FloatingTimeStampTruncYm),
      // .. or extract one of the fields
      (FloatingTimestampYearField, FloatingTimeStampTruncYm),
      (FloatingTimeStampExtractY, FloatingTimeStampTruncYmd),
      (FloatingTimestampMonthField, FloatingTimeStampTruncYm),
      (FloatingTimeStampExtractM, FloatingTimeStampTruncYmd),
      // Out of a Y timestamp truncation, we can extract the year field
      (FloatingTimestampYearField, FloatingTimeStampTruncY),
      (FloatingTimeStampExtractY, FloatingTimeStampTruncYmd),
      // Out of a YM date truncation, we can further truncate
      (DateTruncY, DateTruncYm),
      // ... or extract one of the fields
      (DateYearField, DateTruncYm),
      (DateMonthField, DateTruncYm),
      // Out of a Y date truncation, we ca extract the year field
      (DateYearField, DateTruncY),
      // datez_trunc functions return _fixed_ timestamps...
      (FixedTimeStampZTruncY, FixedTimeStampZTruncYmd),
      (FixedTimeStampZTruncYm, FixedTimeStampZTruncYmd),
      (FixedTimeStampZTruncY, FixedTimeStampZTruncYm)
    ).map { case (a, b) =>
        (a.monomorphic.get.function.identity, b.monomorphic.get.function.identity) -> a.monomorphic.get
    }.toMap
  }

  protected def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction): Option[MonomorphicFunction] =
    monomorphicMap.get((a.function.identity, b.function.identity))


  private val MFFixedTimeStampTruncYmdAtTimeZone = SoQLFunctions.FixedTimeStampTruncYmdAtTimeZone.monomorphic.get
  private val MFFixedTimeStampTruncYmAtTimeZone = SoQLFunctions.FixedTimeStampTruncYmAtTimeZone.monomorphic.get
  private val MFFixedTimeStampTruncYAtTimeZone = SoQLFunctions.FixedTimeStampTruncYAtTimeZone.monomorphic.get

  private val MFFloatingTimeStampTruncYmd = SoQLFunctions.FloatingTimeStampTruncYmd.monomorphic.get
  private val MFFloatingTimeStampTruncYm = SoQLFunctions.FloatingTimeStampTruncYm.monomorphic.get
  private val MFFloatingTimeStampTruncY = SoQLFunctions.FloatingTimeStampTruncY.monomorphic.get

  override def apply(a: Expr, b: Expr, under: IsomorphismState.View[MT]): Option[Expr => Expr] = {
    (a, b) match {
      // We can rewrite date_trunc_y(some_timestamp, some_timezone) in terms of
      // a rollup column defined as date_trunc_ymd(some_timestamp, some_timezone)
      // as date_trunc_y(that_column)
      case (afc@FunctionCall(MFFixedTimeStampTruncYAtTimeZone, Seq(aTS, aTZ)), FunctionCall(MFFixedTimeStampTruncYmdAtTimeZone, Seq(bTS, bTZ))) if aTS.isIsomorphic(bTS, under) && aTZ.isIsomorphic(bTZ, under) =>
        Some { expr => FunctionCall[MT](MFFloatingTimeStampTruncY, Seq(expr))(afc.position) }
      // ditto for date_trunc_y and date_trunc_ym
      case (afc@FunctionCall(MFFixedTimeStampTruncYAtTimeZone, Seq(aTS, aTZ)), FunctionCall(MFFixedTimeStampTruncYmAtTimeZone, Seq(bTS, bTZ))) if aTS.isIsomorphic(bTS, under) && aTZ.isIsomorphic(bTZ, under) =>
        Some { expr => FunctionCall[MT](MFFloatingTimeStampTruncY, Seq(expr))(afc.position) }
      // ditto for date_trunc_ym and date_trunc_ymd
      case (afc@FunctionCall(MFFixedTimeStampTruncYmAtTimeZone, Seq(aTS, aTZ)), FunctionCall(MFFixedTimeStampTruncYmdAtTimeZone, Seq(bTS, bTZ))) if aTS.isIsomorphic(bTS, under) && aTZ.isIsomorphic(bTZ, under) =>
        Some { expr => FunctionCall[MT](MFFloatingTimeStampTruncYm, Seq(expr))(afc.position) }
      case _ =>
        super.apply(a, b, under)
    }
  }
}
