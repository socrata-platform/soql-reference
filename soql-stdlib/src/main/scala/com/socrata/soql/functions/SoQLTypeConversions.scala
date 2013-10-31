package com.socrata.soql.functions

import scala.collection.JavaConverters._

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLNumberLiteral
import com.socrata.soql.parsing.Lexer
import com.socrata.soql.tokens.{EOF, NumberLiteral}

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

  private def getMonomorphically(f: Function[SoQLType]): MonomorphicFunction[SoQLType] =
    f.monomorphic.getOrElse(sys.error(f.name + " not monomorphic?"))

  private val textToFixedTimestampFunc =
    Some(SoQLFunctions.TextToFixedTimestamp.monomorphic.getOrElse(sys.error("text to fixed_timestamp conversion not monomorphic?")))
  private val textToFloatingTimestampFunc =
    Some(SoQLFunctions.TextToFloatingTimestamp.monomorphic.getOrElse(sys.error("text to floating_timestamp conversion not monomorphic?")))
  private val textToDateFunc =
    Some(SoQLFunctions.TextToDate.monomorphic.getOrElse(sys.error("text to date conversion not monomorphic?")))
  private val textToTimeFunc =
    Some(SoQLFunctions.TextToTime.monomorphic.getOrElse(sys.error("text to time conversion not monomorphic?")))
  private val numberToMoneyFunc =
    Some(SoQLFunctions.NumberToMoney.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
  private val numberToDoubleFunc =
    Some(SoQLFunctions.NumberToDouble.monomorphic.getOrElse(sys.error("text to money conversion not monomorphic?")))
  private val textToRowIdFunc =
    Some(SoQLFunctions.TextToRowIdentifier.monomorphic.getOrElse(sys.error("text to row_identifier conversion not monomorphic?")))
  private val textToRowVersionFunc =
    Some(SoQLFunctions.TextToRowVersion.monomorphic.getOrElse(sys.error("text to row_version conversion not monomorphic?")))
  private val textToNumberFunc =
    Some(getMonomorphically(SoQLFunctions.TextToNumber))
  private val textToMoneyFunc =
    Some(getMonomorphically(SoQLFunctions.TextToMoney))

  private def isNumberLiteral(s: String) = try {
    val lexer = new Lexer(s)
    lexer.yylex() match {
      case NumberLiteral(_) => lexer.yylex() == EOF()
      case _ => false
    }
  } catch {
    case e: Exception =>
      false
  }

  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType): Option[MonomorphicFunction[SoQLType]] = {
    (from,to) match {
      case (SoQLTextLiteral(SoQLFixedTimestamp.StringRep(_)), SoQLFixedTimestamp) =>
        textToFixedTimestampFunc
      case (SoQLTextLiteral(SoQLFloatingTimestamp.StringRep(_)), SoQLFloatingTimestamp) =>
        textToFloatingTimestampFunc
      case (SoQLTextLiteral(SoQLDate.StringRep(_)), SoQLDate) =>
        textToDateFunc
      case (SoQLTextLiteral(SoQLTime.StringRep(_)), SoQLTime) =>
        textToTimeFunc
      case (SoQLNumberLiteral(num), SoQLMoney) =>
        numberToMoneyFunc
      case (SoQLNumberLiteral(num), SoQLDouble) =>
        numberToDoubleFunc
      case (SoQLTextLiteral(s), SoQLID) if SoQLID.isPossibleId(s) =>
        textToRowIdFunc
      case (SoQLTextLiteral(s), SoQLVersion) if SoQLVersion.isPossibleVersion(s) =>
        textToRowVersionFunc
      case (SoQLTextLiteral(s), SoQLNumber) if isNumberLiteral(s.toString) =>
        textToNumberFunc
      case (SoQLTextLiteral(s), SoQLMoney) if isNumberLiteral(s.toString) =>
        textToMoneyFunc
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLAnalysisType, expected: SoQLAnalysisType) =
    actual.isPassableTo(expected)
}
