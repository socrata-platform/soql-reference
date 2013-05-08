package com.socrata.soql.functions

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLNumberLiteral

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
    SoQLArray,
    SoQLID,
    SoQLVersion
  )

  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType): Option[MonomorphicFunction[SoQLType]] = {
    (from,to) match {
      case (SoQLTextLiteral(SoQLFixedTimestamp.StringRep(_)), SoQLFixedTimestamp) =>
        Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
      case (SoQLTextLiteral(SoQLFloatingTimestamp.StringRep(_)), SoQLFloatingTimestamp) =>
        Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
      case (SoQLTextLiteral(SoQLDate.StringRep(_)), SoQLDate) =>
        Some(SoQLFunctions.TextToDate.monomorphic.getOrElse(sys.error("text to date conversion not monomorphic?")))
      case (SoQLTextLiteral(SoQLTime.StringRep(_)), SoQLTime) =>
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
