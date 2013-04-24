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
  def implicitConversions(from: TestType, to: TestType): Option[MonomorphicFunction[TestType]] = {
    (from,to) match {
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
