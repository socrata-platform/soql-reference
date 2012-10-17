package com.socrata.soql.analysis.types

import com.socrata.soql.analysis.MonomorphicFunction
import com.socrata.collection.OrderedSet
import java.util.regex.Pattern

trait SoQLTypeConversions {
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

  val FixedTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?Z$""")
  def isFixedTimestampLiteral(text: String) =
    FixedTimestampRegex.matcher(text).matches()

  val FloatingTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?$""")
  def isFloatingTimestampLiteral(text: String) =
    FloatingTimestampRegex.matcher(text).matches()

  def implicitConversions(from: SoQLType, to: SoQLType): Option[MonomorphicFunction[SoQLType]] = {
    from match {
      case SoQLTextLiteral(text) if isFixedTimestampLiteral(text.getString) && to == SoQLFixedTimestamp =>
        Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
      case SoQLTextLiteral(text) if isFloatingTimestampLiteral(text.getString) && to == SoQLFloatingTimestamp =>
        Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLType, expected: SoQLType) =
    actual.isPassableTo(expected)
}
