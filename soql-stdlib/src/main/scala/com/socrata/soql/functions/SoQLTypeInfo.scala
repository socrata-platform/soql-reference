package com.socrata.soql.functions

import org.joda.time.{DateTime, LocalDateTime, LocalDate, LocalTime, Period}
import com.vividsolutions.jts.geom.{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.{TypeName, Provenance}
import com.socrata.soql.typed
import com.socrata.soql.types._
import com.socrata.soql.typechecker.{TypeInfo, TypeInfo2, TypeInfoMetaProjection}
import com.socrata.soql.ast
import com.socrata.soql.analyzer2
import com.socrata.soql.functions

import scala.util.parsing.input.{Position, NoPosition}

object SoQLTypeInfo extends TypeInfo[SoQLType, SoQLValue] with TypeInfo2[SoQLType, SoQLValue] {
  val typeParameterUniverse = OrderedSet(SoQLType.typePreferences : _*)

  def metaProject[MT <: analyzer2.MetaTypes](
    implicit typeEv: SoQLType =:= analyzer2.types.ColumnType[MT],
    typeEvRev: analyzer2.types.ColumnType[MT] =:= SoQLType,
    valueEv: SoQLValue =:= analyzer2.types.ColumnValue[MT],
    valueEvRev: analyzer2.types.ColumnValue[MT] =:= SoQLValue
  ): TypeInfoMetaProjection[MT] =
    new TypeInfoMetaProjection[MT] with analyzer2.StatementUniverse[MT] {
      val unproject = SoQLTypeInfo.asInstanceOf[TypeInfo2[CT, CV]]

      private implicit def monomorphicFunctionConvert(f: functions.MonomorphicFunction[SoQLType]): MonomorphicFunction =
        f.asInstanceOf[MonomorphicFunction]

      implicit object hasType extends analyzer2.HasType[CV, CT] {
        def typeOf(cv: CV): CT = cv.typ
      }

      def potentialExprs(l: ast.Literal, primaryTable: Option[Provenance]) =
        l match {
          case ast.NullLiteral() => typeParameterUniverse.iterator.map(analyzer2.NullLiteral(_)(new analyzer2.AtomicPositionInfo(l.position))).toVector
          case ast.BooleanLiteral(b) => Seq(analyzer2.LiteralValue[MT](SoQLBoolean(b))(new analyzer2.AtomicPositionInfo(l.position)))
          case ast.NumberLiteral(n) =>
            val baseNumber = analyzer2.LiteralValue[MT](SoQLNumber(n.bigDecimal))(new analyzer2.AtomicPositionInfo(l.position))
            Seq(
              baseNumber,
              analyzer2.FunctionCall[MT](numberToMoneyFunc, Seq(baseNumber))(new analyzer2.FuncallPositionInfo(l.position, NoPosition, NoPosition)),
              analyzer2.FunctionCall[MT](numberToDoubleFunc, Seq(baseNumber))(new analyzer2.FuncallPositionInfo(l.position, NoPosition, NoPosition))
            )
          case ast.StringLiteral(s) =>
            val baseString = analyzer2.LiteralValue[MT](SoQLText(s))(new analyzer2.AtomicPositionInfo(l.position))
            val results = Seq.newBuilder[analyzer2.Expr[MT]]
            results += baseString
            for {
              conversion <- stringConversions
              v <- conversion.test(s)
              expr <- conversion.exprs
            } {
              results += expr(v, l.position, primaryTable).asInstanceOf[analyzer2.Expr[MT]] // SAFETY: CT and CV are the same, and that's all this cares about
            }
            results += analyzer2.FunctionCall[MT](textToBlobFunc, Seq(baseString))(new analyzer2.FuncallPositionInfo(l.position, NoPosition, NoPosition))
            results += analyzer2.FunctionCall[MT](textToPhotoFunc, Seq(baseString))(new analyzer2.FuncallPositionInfo(l.position, NoPosition, NoPosition))
            results.result()
        }

      def boolType = SoQLBoolean.t

      def literalBoolean(b: Boolean, pos: Position) =
        analyzer2.LiteralValue[MT](SoQLBoolean(b))(new analyzer2.AtomicPositionInfo(pos))

      def updateProvenance(v: CV)(f: Provenance => Provenance): CV = {
        valueEvRev(v) match {
          case id: SoQLID =>
            val newId = id.copy()
            newId.provenance = id.provenance.map(f)
            newId
          case version: SoQLVersion =>
            val newVersion = version.copy()
            newVersion.provenance = version.provenance.map(f)
            newVersion
          case _ =>
            v
        }
      }
    }

  def booleanLiteralExpr(b: Boolean, pos: Position) = Seq(typed.BooleanLiteral(b, SoQLBoolean.t)(pos))

  private final class FakeMT extends analyzer2.MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Nothing
    type DatabaseTableNameImpl = Nothing
  }

  implicit object hasType extends analyzer2.HasType[FakeMT#ColumnValue, FakeMT#ColumnType] {
    def typeOf(cv: SoQLValue) = cv.typ
  }

  private def getMonomorphically(f: Function[SoQLType]): MonomorphicFunction[SoQLType] =
    f.monomorphic.getOrElse(sys.error(f.identity + " not monomorphic?"))

  private def funcExpr(f: MonomorphicFunction[SoQLType]) = { (t: SoQLValue, pos: Position) =>
    analyzer2.FunctionCall[FakeMT](f, Seq(analyzer2.LiteralValue[FakeMT](t)(new analyzer2.AtomicPositionInfo(pos))))(new analyzer2.FuncallPositionInfo(pos, NoPosition, NoPosition))
  }

  private def textToFixedTimestampExpr(dt: DateTime, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLFixedTimestamp(dt))(new analyzer2.AtomicPositionInfo(pos))
  private def textToFloatingTimestampExpr(ldt: LocalDateTime, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLFloatingTimestamp(ldt))(new analyzer2.AtomicPositionInfo(pos))
  private def textToDateExpr(d: LocalDate, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLDate(d))(new analyzer2.AtomicPositionInfo(pos))
  private def textToTimeExpr(t: LocalTime, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLTime(t))(new analyzer2.AtomicPositionInfo(pos))
  private def textToIntervalExpr(p: Period, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLInterval(p))(new analyzer2.AtomicPositionInfo(pos))
  private def textToNumberExpr(s: SoQLText, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLNumber(new java.math.BigDecimal(s.value)))(new analyzer2.AtomicPositionInfo(pos))
  private def textToMoneyExpr(s: SoQLText, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLMoney(new java.math.BigDecimal(s.value)))(new analyzer2.AtomicPositionInfo(pos))
  private def textToPhoneExpr(phone: SoQLPhone, pos: Position) =
    analyzer2.LiteralValue[FakeMT](phone)(new analyzer2.AtomicPositionInfo(pos))
  private def textToUrlExpr(url: SoQLUrl, pos: Position) =
    analyzer2.LiteralValue[FakeMT](url)(new analyzer2.AtomicPositionInfo(pos))
  private def textToPointExpr(p: Point, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLPoint(p))(new analyzer2.AtomicPositionInfo(pos))
  private def textToMultiPointExpr(mp: MultiPoint, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLMultiPoint(mp))(new analyzer2.AtomicPositionInfo(pos))
  private def textToLineExpr(l: LineString, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLLine(l))(new analyzer2.AtomicPositionInfo(pos))
  private def textToMultiLineExpr(ml: MultiLineString, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLMultiLine(ml))(new analyzer2.AtomicPositionInfo(pos))
  private def textToPolygonExpr(p: Polygon, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLPolygon(p))(new analyzer2.AtomicPositionInfo(pos))
  private def textToMultiPolygonExpr(mp: MultiPolygon, pos: Position) =
    analyzer2.LiteralValue[FakeMT](SoQLMultiPolygon(mp))(new analyzer2.AtomicPositionInfo(pos))
  private def textToRowIdExpr(rid: SoQLID, pos: Position, primaryTable: Option[Provenance]) = {
    val newRID = rid.copy()
    newRID.provenance = primaryTable
    analyzer2.LiteralValue[FakeMT](newRID)(new analyzer2.AtomicPositionInfo(pos))
  }
  private def textToRowVersionExpr(rv: SoQLVersion, pos: Position, primaryTable: Option[Provenance]) = {
    val newRV = rv.copy()
    newRV.provenance = primaryTable
    analyzer2.LiteralValue[FakeMT](newRV)(new analyzer2.AtomicPositionInfo(pos))
  }

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
  private def textToBooleanExpr(s: SoQLText, pos: Position) =
    s.value.toLowerCase match {
      case "true"|"1" => analyzer2.LiteralValue[FakeMT](SoQLBoolean(true))(new analyzer2.AtomicPositionInfo(pos))
      case "false"|"0" => analyzer2.LiteralValue[FakeMT](SoQLBoolean(false))(new analyzer2.AtomicPositionInfo(pos))
    }
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

  // This is a bit icky now; it can be simplified when/if the old type tree format goes away
  sealed private abstract class Conversions {
    type TestResult
    def test(s: String): Option[TestResult]
    val functions: Seq[MonomorphicFunction[SoQLType]]
    val exprs: Seq[(TestResult, Position, Option[Provenance]) => analyzer2.Expr[FakeMT]]
  }
  private object Conversions {
    def apply[T](tst: String => Option[T], fs: Seq[MonomorphicFunction[SoQLType]], es: Seq[(T, Position) => analyzer2.Expr[FakeMT]]) = new Conversions {
      type TestResult = T
      def test(s: String) = tst(s)
      val functions = fs
      val exprs = es.map { ef => (t: T, pos: Position, cn: Option[Provenance]) => ef(t, pos) }
    }
    def simple(tst: String => Boolean, fs: Seq[MonomorphicFunction[SoQLType]], es: Seq[(SoQLText, Position) => analyzer2.Expr[FakeMT]]) = new Conversions {
      type TestResult = String
      def test(s: String) = if(tst(s)) Some(s) else None
      val functions = fs
      val exprs = es.map { ef => (s: String, pos: Position, cn: Option[Provenance]) => ef(SoQLText(s), pos) }
    }
    def provenanced[T](tst: String => Option[T], fs: Seq[MonomorphicFunction[SoQLType]], es: Seq[(T, Position, Option[Provenance]) => analyzer2.Expr[FakeMT]]) = new Conversions {
      type TestResult = T
      def test(s: String) = tst(s)
      val functions = fs
      val exprs = es.map { ef => (t: T, pos: Position, cn: Option[Provenance]) => ef(t, pos, cn) }
    }
  }

  private val stringConversions = Seq[Conversions](
    Conversions(SoQLFixedTimestamp.StringRep.unapply(_), Seq(textToFixedTimestampFunc), Seq(textToFixedTimestampExpr _)),
    Conversions(SoQLFloatingTimestamp.StringRep.unapply(_), Seq(textToFloatingTimestampFunc), Seq(textToFloatingTimestampExpr _)),
    Conversions(SoQLDate.StringRep.unapply(_), Seq(textToDateFunc), Seq(textToDateExpr _)),
    Conversions(SoQLTime.StringRep.unapply(_), Seq(textToTimeFunc), Seq(textToTimeExpr _)),
    Conversions(SoQLInterval.StringRep.unapply(_), Seq(textToIntervalFunc), Seq(textToIntervalExpr _)),
    Conversions.provenanced(SoQLID.FormattedButUnobfuscatedStringRep.unapply(_), Seq(textToRowIdFunc), Seq(textToRowIdExpr _)),
    Conversions.provenanced(SoQLVersion.FormattedButUnobfuscatedStringRep.unapply(_), Seq(textToRowVersionFunc), Seq(textToRowVersionExpr _)),
    Conversions.simple(isNumberLiteral, Seq(textToNumberFunc, textToMoneyFunc), Seq(textToNumberExpr, textToMoneyExpr)),
    Conversions(SoQLPoint.WktRep.unapply(_), Seq(textToPointFunc), Seq(textToPointExpr _)),
    Conversions(SoQLMultiPoint.WktRep.unapply(_), Seq(textToMultiPointFunc), Seq(textToMultiPointExpr _)),
    Conversions(SoQLLine.WktRep.unapply(_), Seq(textToLineFunc), Seq(textToLineExpr _)),
    Conversions(SoQLMultiLine.WktRep.unapply(_), Seq(textToMultiLineFunc), Seq(textToMultiLineExpr _)),
    Conversions(SoQLPolygon.WktRep.unapply(_), Seq(textToPolygonFunc), Seq(textToPolygonExpr _)),
    Conversions(SoQLMultiPolygon.WktRep.unapply(_), Seq(textToMultiPolygonFunc), Seq(textToMultiPolygonExpr _)),
    Conversions(SoQLPhone.parsePhone _, Seq(textToPhoneFunc), Seq(textToPhoneExpr _)),
    Conversions(SoQLUrl.parseUrl _, Seq(textToUrlFunc), Seq(textToUrlExpr _)),
    Conversions.simple(SoQLLocation.isPossibleLocation, Seq(textToLocationFunc), Seq(funcExpr(textToLocationFunc))),
    Conversions.simple(isBooleanLiteral, Seq(textToBooleanFunc), Seq(textToBooleanExpr _))
  )

  def stringLiteralExpr(s: String, pos: Position) = {
    val baseString = typed.StringLiteral(s, SoQLText.t)(pos)
    val results = Seq.newBuilder[typed.CoreExpr[Nothing, SoQLType]]
    results += baseString
    results ++= (for {
      conversion <- stringConversions
      if conversion.test(s).isDefined
      func <- conversion.functions
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
