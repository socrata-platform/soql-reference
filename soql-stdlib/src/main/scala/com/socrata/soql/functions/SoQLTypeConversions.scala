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
    SoQLPoint,
    SoQLMultiPoint,
    SoQLLine,
    SoQLMultiLine,
    SoQLPolygon,
    SoQLMultiPolygon,
    SoQLLocation,
    SoQLObject,
    SoQLArray,
    SoQLID,
    SoQLVersion,
    SoQLBlob
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
  private val textToPointFunc =
    Some(SoQLFunctions.TextToPoint.monomorphic.getOrElse(sys.error("text to point conversion not monomorphic?")))
  private val textToMultiPointFunc =
    Some(SoQLFunctions.TextToMultiPoint.monomorphic.getOrElse(sys.error("text to multi point conversion not monomorphic?")))
  private val textToLineFunc =
    Some(SoQLFunctions.TextToLine.monomorphic.getOrElse(sys.error("text to line conversion not monomorphic?")))
  private val textToMultiLineFunc =
    Some(SoQLFunctions.TextToMultiLine.monomorphic.getOrElse(sys.error("text to multi line conversion not monomorphic?")))
  private val textToPolygonFunc =
    Some(SoQLFunctions.TextToPolygon.monomorphic.getOrElse((sys.error(("text to polygon conversion not monomorphic?")))))
  private val textToMultiPolygonFunc =
    Some(SoQLFunctions.TextToMultiPolygon.monomorphic.getOrElse(sys.error("text to multi polygon conversion not monomorphic?")))
  private val textToBlobFunc =
    Some(SoQLFunctions.TextToBlob.monomorphic.getOrElse(sys.error("text to blob conversion not monomorphic?")))
  private val textToLocationFunc =
    Some(SoQLFunctions.TextToLocation.monomorphic.getOrElse(sys.error("text to location conversion not monomorphic?")))
  private val textToLocationLatitudeFunc =
    Some(SoQLFunctions.TextToLocationLatitude.monomorphic.getOrElse(sys.error("text to location latitude conversion not monomorphic?")))
  private val textToLocationLongitudeFunc =
    Some(SoQLFunctions.TextToLocationLongitude.monomorphic.getOrElse(sys.error("text to location longitude conversion not monomorphic?")))
  private val textToLocationAddressFunc =
    Some(SoQLFunctions.TextToLocationAddress.monomorphic.getOrElse(sys.error("text to location address conversion not monomorphic?")))

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
      case (SoQLTextLiteral(s), SoQLPoint) if SoQLPoint.WktRep.unapply(s.toString).isDefined =>
        textToPointFunc
      case (SoQLTextLiteral(s), SoQLMultiPoint) if SoQLMultiPoint.WktRep.unapply(s.toString).isDefined =>
        textToMultiPointFunc
      case (SoQLTextLiteral(s), SoQLLine) if SoQLLine.WktRep.unapply((s.toString)).isDefined =>
        textToLineFunc
      case (SoQLTextLiteral(s), SoQLMultiLine) if SoQLMultiLine.WktRep.unapply(s.toString).isDefined =>
        textToMultiLineFunc
      case (SoQLTextLiteral(s), SoQLPolygon) if SoQLPolygon.WktRep.unapply(s.toString).isDefined =>
        textToPolygonFunc
      case (SoQLTextLiteral(s), SoQLMultiPolygon) if SoQLMultiPolygon.WktRep.unapply(s.toString).isDefined =>
        textToMultiPolygonFunc
      case (SoQLTextLiteral(s), SoQLBlob) =>
        textToBlobFunc
      case (SoQLTextLiteral(s), SoQLLocation) if SoQLLocation.isPossibleLocation(s) =>
        textToLocationFunc
      case (SoQLTextLiteral(s), SoQLLocationLatitude) if SoQLLocationLatitude.isPossible(s) =>
        textToLocationLatitudeFunc
      case (SoQLTextLiteral(s), SoQLLocationLongitude) if SoQLLocationLongitude.isPossible(s) =>
        textToLocationLongitudeFunc
      case (SoQLTextLiteral(s), SoQLLocationAddress) if SoQLLocationAddress.isPossible(s) =>
        textToLocationAddressFunc
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLAnalysisType, expected: SoQLAnalysisType) =
    actual.isPassableTo(expected)
}
