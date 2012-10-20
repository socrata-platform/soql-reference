package com.socrata.soql.analysis.types

import com.socrata.soql.analysis.MonomorphicFunction
import com.socrata.collection.OrderedSet
import java.util.regex.Pattern

trait SoQLTypeConversions {
  def typeParameterUniverse = SoQLTypeConversions.typeParameterUniverse

  val FixedTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?Z$""")
  def isFixedTimestampLiteral(text: String) =
    FixedTimestampRegex.matcher(text).matches()

  val FloatingTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?$""")
  def isFloatingTimestampLiteral(text: String) =
    FloatingTimestampRegex.matcher(text).matches()

  def implicitConversions(from: SoQLType, to: SoQLType): Option[MonomorphicFunction[SoQLType]] = {
    (from,to) match {
      case (SoQLTextLiteral(text), SoQLFixedTimestamp) if isFixedTimestampLiteral(text.getString)  =>
        Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
      case (SoQLTextLiteral(text), SoQLFloatingTimestamp) if isFloatingTimestampLiteral(text.getString) =>
        Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
      case (SoQLNumberLiteral(num), SoQLMoney) =>
        Some(SoQLFunctions.NumberToMoney.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case (SoQLNumberLiteral(num), SoQLDouble) =>
        Some(SoQLFunctions.NumberToDouble.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLType, expected: SoQLType) =
    actual.isPassableTo(expected)
}

object SoQLTypeConversions {
  val typeParameterUniverse: OrderedSet[SoQLType] = OrderedSet(
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLLocation,
    SoQLObject,
    SoQLArray
  )
}
