package com.socrata.soql.functions

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.TypeName
import com.socrata.soql.parsing.Lexer
import com.socrata.soql.tokens.{EOF, NumberLiteral}
import com.socrata.soql.typed
import com.socrata.soql.types._
import com.socrata.soql.typechecker.TypeInfo

import scala.util.parsing.input.Position

object SoQLTypeInfo extends TypeInfo[SoQLType] {
  val typeParameterUniverse = OrderedSet(SoQLType.typePreferences : _*)

  def booleanLiteralExpr(b: Boolean, pos: Position) = Seq(typed.BooleanLiteral(b, SoQLBoolean)(pos))

  private def getMonomorphically(f: Function[SoQLType]): MonomorphicFunction[SoQLType] =
    f.monomorphic.getOrElse(sys.error(f.identity + " not monomorphic?"))

  private val textToFixedTimestampFunc = getMonomorphically(SoQLFunctions.TextToFixedTimestamp)
  private val textToFloatingTimestampFunc = getMonomorphically(SoQLFunctions.TextToFloatingTimestamp)
  private val textToDateFunc = getMonomorphically(SoQLFunctions.TextToDate)
  private val textToTimeFunc = getMonomorphically(SoQLFunctions.TextToTime)
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
  private val textToLocationFunc = getMonomorphically(SoQLFunctions.TextToLocation)

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

  val stringConversions = Seq[(String => Boolean, Seq[MonomorphicFunction[SoQLType]])](
    (SoQLFixedTimestamp.StringRep.unapply(_).isDefined, Seq(textToFixedTimestampFunc)),
    (SoQLFloatingTimestamp.StringRep.unapply(_).isDefined, Seq(textToFloatingTimestampFunc)),
    (SoQLDate.StringRep.unapply(_).isDefined, Seq(textToDateFunc)),
    (SoQLTime.StringRep.unapply(_).isDefined, Seq(textToTimeFunc)),
    (SoQLID.isPossibleId, Seq(textToRowIdFunc)),
    (SoQLVersion.isPossibleVersion, Seq(textToRowVersionFunc)),
    (isNumberLiteral, Seq(textToNumberFunc, textToMoneyFunc)),
    (SoQLPoint.WktRep.unapply(_).isDefined, Seq(textToPointFunc)),
    (SoQLMultiPoint.WktRep.unapply(_).isDefined, Seq(textToMultiPointFunc)),
    (SoQLLine.WktRep.unapply(_).isDefined, Seq(textToLineFunc)),
    (SoQLMultiLine.WktRep.unapply(_).isDefined, Seq(textToMultiLineFunc)),
    (SoQLPolygon.WktRep.unapply(_).isDefined, Seq(textToPolygonFunc)),
    (SoQLMultiPolygon.WktRep.unapply(_).isDefined, Seq(textToMultiPolygonFunc)),
    (SoQLLocation.isPossibleLocation, Seq(textToLocationFunc))
  )

  def stringLiteralExpr(s: String, pos: Position) = {
    val baseString = typed.StringLiteral(s, SoQLText)(pos)
    val results = Seq.newBuilder[typed.CoreExpr[Nothing, SoQLType]]
    results += baseString
    results ++= (for {
      (test, funcs) <- stringConversions
      if test(s)
      func <- funcs
    } yield typed.FunctionCall(func, Seq(baseString))(pos, pos))
    results += typed.FunctionCall(textToBlobFunc, Seq(baseString))(pos, pos)
    results.result()
  }

  def numberLiteralExpr(n: BigDecimal, pos: Position) = {
    val baseNumber = typed.NumberLiteral(n, SoQLNumber)(pos)
    Seq(
      baseNumber,
      typed.FunctionCall(numberToMoneyFunc, Seq(baseNumber))(pos, pos),
      typed.FunctionCall(numberToDoubleFunc, Seq(baseNumber))(pos, pos)
    )
  }

  def nullLiteralExpr(pos: Position) = typeParameterUniverse.toSeq.map(typed.NullLiteral(_)(pos))

  def typeFor(name: TypeName) =
    SoQLType.typesByName.get(name)

  def typeNameFor(typ: SoQLType): TypeName = typ.name

  def isOrdered(typ: SoQLType): Boolean = SoQLTypeClasses.Ordered(typ)
  def isBoolean(typ: SoQLType): Boolean = typ == SoQLBoolean
  def isGroupable(typ: SoQLType): Boolean = SoQLTypeClasses.Equatable(typ)
}
