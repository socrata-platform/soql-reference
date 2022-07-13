package com.socrata.soql.functions

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.typed
import com.socrata.soql.types._
import com.socrata.soql.typechecker.TypeInfo

import scala.util.parsing.input.Position

object SoQLTypeInfo extends TypeInfo[SoQLType, SoQLValue] {
  val typeParameterUniverse = OrderedSet(SoQLType.typePreferences : _*)

  def booleanLiteralExpr(b: Boolean, pos: Position) = Seq(typed.BooleanLiteral(b, SoQLBoolean.t)(pos))

  private def getMonomorphically(f: Function[SoQLType]): MonomorphicFunction[SoQLType] =
    f.monomorphic.getOrElse(sys.error(f.identity + " not monomorphic?"))

  private val textToFixedTimestampFunc = getMonomorphically(SoQLFunctions.TextToFixedTimestamp)
  private val textToFloatingTimestampFunc = getMonomorphically(SoQLFunctions.TextToFloatingTimestamp)
  private val textToDateFunc = getMonomorphically(SoQLFunctions.TextToDate)
  private val textToTimeFunc = getMonomorphically(SoQLFunctions.TextToTime)
  private val textToIntervalFunc = getMonomorphically(SoQLFunctions.TextToInterval)
  private val numberToMoneyFunc = getMonomorphically(SoQLFunctions.NumberToMoney)
  private val numberToDoubleFunc = getMonomorphically(SoQLFunctions.NumberToDouble)
  private val textToRowIdFunc = getMonomorphically(SoQLFunctions.TextToRowIdentifier)
  private val textToRowVersionFunc = getMonomorphically(SoQLFunctions.TextToRowVersion)
  private val textToNumberFunc = getMonomorphically(SoQLFunctions.TextToNumber)
  private val textToMoneyFunc = getMonomorphically(SoQLFunctions.TextToMoney)
  private val textToPointFunc = getMonomorphically(SoQLFunctions.TextToPoint)
  private val textToMultiPointFunc = getMonomorphically(SoQLFunctions.TextToMultiPoint)
  private val textToLineFunc = getMonomorphically(SoQLFunctions.TextToLine)
  private val textToMultiLineFunc = getMonomorphically(SoQLFunctions.TextToMultiLine)
  private val textToPolygonFunc = getMonomorphically(SoQLFunctions.TextToPolygon)
  private val textToMultiPolygonFunc = getMonomorphically(SoQLFunctions.TextToMultiPolygon)
  private val textToBlobFunc = getMonomorphically(SoQLFunctions.TextToBlob)
  private val textToPhotoFunc = getMonomorphically(SoQLFunctions.TextToPhoto)
  private val textToPhoneFunc = getMonomorphically(SoQLFunctions.TextToPhone)
  private val textToUrlFunc = getMonomorphically(SoQLFunctions.TextToUrl)
  private val textToLocationFunc = getMonomorphically(SoQLFunctions.TextToLocation)
  private val textToBooleanFunc = getMonomorphically(SoQLFunctions.TextToBool)

  private val numberRx = "([+-]?[0-9]+)(\\.[0-9]*)?(e[+-]?[0-9]+)?".r
  private def isNumberLiteral(s: String) = try {
    s match {
      case numberRx(_, _, _) =>
        true
      case _ =>
        false
    }
  } catch {
    case e: Exception =>
      false
  }

  private val booleanRx = "(?i)(true|false|1|0)".r
  private def isBooleanLiteral(s: String) = try {
    s match {
      case booleanRx(_) =>
        true
      case _ =>
        false
    }
  } catch {
    case e: Exception =>
      false
  }

  val stringConversions = Seq[(String => Boolean, Seq[MonomorphicFunction[SoQLType]])](
    (SoQLFixedTimestamp.StringRep.unapply(_).isDefined, Seq(textToFixedTimestampFunc)),
    (SoQLFloatingTimestamp.StringRep.unapply(_).isDefined, Seq(textToFloatingTimestampFunc)),
    (SoQLDate.StringRep.unapply(_).isDefined, Seq(textToDateFunc)),
    (SoQLTime.StringRep.unapply(_).isDefined, Seq(textToTimeFunc)),
    (SoQLInterval.StringRep.unapply(_).isDefined, Seq(textToIntervalFunc)),
    (SoQLID.isPossibleId, Seq(textToRowIdFunc)),
    (SoQLVersion.isPossibleVersion, Seq(textToRowVersionFunc)),
    (isNumberLiteral, Seq(textToNumberFunc, textToMoneyFunc)),
    (SoQLPoint.WktRep.unapply(_).isDefined, Seq(textToPointFunc)),
    (SoQLMultiPoint.WktRep.unapply(_).isDefined, Seq(textToMultiPointFunc)),
    (SoQLLine.WktRep.unapply(_).isDefined, Seq(textToLineFunc)),
    (SoQLMultiLine.WktRep.unapply(_).isDefined, Seq(textToMultiLineFunc)),
    (SoQLPolygon.WktRep.unapply(_).isDefined, Seq(textToPolygonFunc)),
    (SoQLMultiPolygon.WktRep.unapply(_).isDefined, Seq(textToMultiPolygonFunc)),
    (SoQLPhone.isPossible, Seq(textToPhoneFunc)),
    (SoQLUrl.isPossible, Seq(textToUrlFunc)),
    (SoQLLocation.isPossibleLocation, Seq(textToLocationFunc)),
    (isBooleanLiteral, Seq(textToBooleanFunc))
  )

  def stringLiteralExpr(s: String, pos: Position) = {
    val baseString = typed.StringLiteral(s, SoQLText.t)(pos)
    val results = Seq.newBuilder[typed.CoreExpr[Nothing, SoQLType]]
    results += baseString
    results ++= (for {
      (test, funcs) <- stringConversions
      if test(s)
      func <- funcs
    } yield typed.FunctionCall(func, Seq(baseString), None, None)(pos, pos))
    results += typed.FunctionCall(textToBlobFunc, Seq(baseString), None, None)(pos, pos)
    results += typed.FunctionCall(textToPhotoFunc, Seq(baseString), None, None)(pos, pos)
    results.result()
  }

  def numberLiteralExpr(n: BigDecimal, pos: Position) = {
    val baseNumber = typed.NumberLiteral(n, SoQLNumber.t)(pos)
    Seq(
      baseNumber,
      typed.FunctionCall(numberToMoneyFunc, Seq(baseNumber), None, None)(pos, pos),
      typed.FunctionCall(numberToDoubleFunc, Seq(baseNumber), None, None)(pos, pos)
    )
  }

  def nullLiteralExpr(pos: Position) = typeParameterUniverse.toSeq.map(typed.NullLiteral(_)(pos))

  def typeFor(name: TypeName) =
    SoQLType.typesByName.get(name)

  def typeNameFor(typ: SoQLType): TypeName = typ.name

  def isOrdered(typ: SoQLType): Boolean = SoQLTypeClasses.Ordered(typ)
  def isBoolean(typ: SoQLType): Boolean = typ == SoQLBoolean
  def isGroupable(typ: SoQLType): Boolean = SoQLTypeClasses.Equatable(typ)

  def typeOf(value: SoQLValue) = value.typ

  def literalExprFor(value: SoQLValue, pos: Position) =
    value match {
      case SoQLText(s) =>
        Some(typed.StringLiteral(s, SoQLText.t)(pos))
      case SoQLNumber(n) =>
        Some(typed.NumberLiteral(n, SoQLNumber.t)(pos))
      case SoQLBoolean(b) =>
        Some(typed.BooleanLiteral(b, SoQLBoolean.t)(pos))
      case SoQLFixedTimestamp(ts) =>
        Some(
          typed.FunctionCall(
            textToFixedTimestampFunc,
            Seq(typed.StringLiteral(SoQLFixedTimestamp.StringRep(ts), SoQLText.t)(pos)),
            None, None
          )(pos, pos)
        )
      case SoQLFloatingTimestamp(ts) =>
        Some(
          typed.FunctionCall(
            textToFloatingTimestampFunc,
            Seq(typed.StringLiteral(SoQLFloatingTimestamp.StringRep(ts), SoQLText.t)(pos)),
            None, None
          )(pos, pos)
        )
      case _ =>
        None
    }
}
