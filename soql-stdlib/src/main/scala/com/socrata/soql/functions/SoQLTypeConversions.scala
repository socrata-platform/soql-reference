package com.socrata.soql.functions

import java.util.regex.Pattern

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLNumberLiteral
import org.joda.time.format.ISODateTimeFormat

object SoQLTypeConversions {
  val typeParameterUniverse: OrderedSet[SoQLAnalysisType] = OrderedSet(
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLLocation,
    SoQLObject,
    SoQLArray
  )

  private val fixedParser = ISODateTimeFormat.dateTimeParser.withZoneUTC
  def isFixedTimestampLiteral(text: String) =
    try { fixedParser.parseDateTime(text); true }
    catch { case _: IllegalArgumentException => false }

  def isDateLiteral(text: String) =
    try { ISODateTimeFormat.dateElementParser.parseLocalDate(text); true }
    catch { case _: IllegalArgumentException => false }

  def isTimeLiteral(text: String) =
    try { ISODateTimeFormat.timeElementParser.parseLocalTime(text); true }
    catch { case _: IllegalArgumentException => false }

  def isFloatingTimestampLiteral(text: String) =
    try { ISODateTimeFormat.localDateOptionalTimeParser.parseLocalDateTime(text); true }
    catch { case _: IllegalArgumentException => false }

  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType): Option[MonomorphicFunction[SoQLType]] = {
    (from,to) match {
      case (SoQLTextLiteral(text), SoQLFixedTimestamp) if isFixedTimestampLiteral(text.getString)  =>
        Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
      case (SoQLTextLiteral(text), SoQLFloatingTimestamp) if isFloatingTimestampLiteral(text.getString) =>
        Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
      case (SoQLTextLiteral(text), SoQLDate) if isDateLiteral(text.getString) =>
        Some(SoQLFunctions.TextToDate.monomorphic.getOrElse(sys.error("text to date conversion not monomorphic?")))
      case (SoQLTextLiteral(text), SoQLTime) if isTimeLiteral(text.getString) =>
        Some(SoQLFunctions.TextToTime.monomorphic.getOrElse(sys.error("text to time conversion not monomorphic?")))
      case (SoQLNumberLiteral(num), SoQLMoney) =>
        Some(SoQLFunctions.NumberToMoney.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case (SoQLNumberLiteral(num), SoQLDouble) =>
        Some(SoQLFunctions.NumberToDouble.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLAnalysisType, expected: SoQLAnalysisType) =
    actual.isPassableTo(expected)
}
