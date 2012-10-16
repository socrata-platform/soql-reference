package com.socrata.soql.analysis.types

import com.socrata.soql.analysis.MonomorphicFunction

trait SoQLTypeConversions {
  val typeParameterUniverse: Seq[SoQLType] = Vector(
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

  val numberLiterals = Set[SoQLType](SoQLNumberLiteral)
  val textLiterals = Set[SoQLType](SoQLTextLiteral, SoQLTextFixedTimestampLiteral, SoQLTextFloatingTimestampLiteral)

  def implicitConversions(from: SoQLType, to: SoQLType): Option[MonomorphicFunction[SoQLType]] = {
    from match {
      case SoQLTextFixedTimestampLiteral =>
        to match {
          case SoQLFixedTimestamp => Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
          case _ => None
        }
      case SoQLTextFloatingTimestampLiteral =>
        to match {
          case SoQLFloatingTimestamp => Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
          case _ => None
        }
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLType, expected: SoQLType) =
    actual == expected || actual == SoQLNull || (numberLiterals.contains(actual) && expected == SoQLNumber) || (textLiterals.contains(actual) && expected == SoQLText)
}
