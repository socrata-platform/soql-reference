package com.socrata.soql.types

import java.util.regex.Pattern

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.functions.MonomorphicFunction

object TestTypeConversions {
  val typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestDouble,
    TestMoney,
    TestBoolean,
    TestFixedTimestamp,
    TestFloatingTimestamp,
    TestLocation,
    TestObject,
    TestArray
  )

  val FixedTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?Z$""")
  def isFixedTimestampLiteral(text: String) =
    FixedTimestampRegex.matcher(text).matches()

  val FloatingTimestampRegex = Pattern.compile("""(?i)^[0-9]{4,}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:,[0-9]{1,3})?$""")
  def isFloatingTimestampLiteral(text: String) =
    FloatingTimestampRegex.matcher(text).matches()

  def implicitConversions(from: TestType, to: TestType): Option[MonomorphicFunction[TestType]] = {
    (from,to) match {
      case (TestTextLiteral(text), TestFixedTimestamp) if isFixedTimestampLiteral(text.getString)  =>
        Some(TestFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
      case (TestTextLiteral(text), TestFloatingTimestamp) if isFloatingTimestampLiteral(text.getString) =>
        Some(TestFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
      case (TestNumberLiteral(num), TestMoney) =>
        Some(TestFunctions.NumberToMoney.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case (TestNumberLiteral(num), TestDouble) =>
        Some(TestFunctions.NumberToDouble.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: TestType, expected: TestType) =
    actual.isPassableTo(expected)
}
