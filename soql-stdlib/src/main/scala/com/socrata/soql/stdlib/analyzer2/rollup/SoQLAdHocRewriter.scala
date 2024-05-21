package com.socrata.soql.stdlib.analyzer2.rollup

import scala.util.parsing.input.NoPosition

import org.joda.time.LocalDateTime

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.AdHocRewriter
import com.socrata.soql.environment.Source
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.types._

class SoQLAdHocRewriter[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends AdHocRewriter[MT]
{
  import SoQLTypeInfo.hasType

  private val LtTime = MonomorphicFunction(SoQLFunctions.Lt, Map("a" -> SoQLFloatingTimestamp))
  private val LteTime = MonomorphicFunction(SoQLFunctions.Lte, Map("a" -> SoQLFloatingTimestamp))
  private val GteTime = MonomorphicFunction(SoQLFunctions.Gte, Map("a" -> SoQLFloatingTimestamp))
  private val GtTime = MonomorphicFunction(SoQLFunctions.Gt, Map("a" -> SoQLFloatingTimestamp))

  private val DateTruncYMD = SoQLFunctions.FloatingTimeStampTruncYmd.monomorphic.get
  private val DateTruncYM = SoQLFunctions.FloatingTimeStampTruncYm.monomorphic.get
  private val DateTruncY = SoQLFunctions.FloatingTimeStampTruncY.monomorphic.get

  private val FixedDateTruncAtYMD = SoQLFunctions.FixedTimeStampTruncYmdAtTimeZone.monomorphic.get
  private val FixedDateTruncAtYM = SoQLFunctions.FixedTimeStampTruncYmAtTimeZone.monomorphic.get
  private val FixedDateTruncAtY = SoQLFunctions.FixedTimeStampTruncYAtTimeZone.monomorphic.get
  private val ToFloatingTimestamp = SoQLFunctions.ToFloatingTimestamp.monomorphic.get

  private object FloatingAtZoneTruncEquiv {
    private val dateTruncMap = Map(
      FixedDateTruncAtYMD.function.identity -> DateTruncYMD,
      FixedDateTruncAtYM.function.identity -> DateTruncYM,
      FixedDateTruncAtY.function.identity -> DateTruncY
    )
    def unapply(f: MonomorphicFunction) = dateTruncMap.get(f.function.identity)
  }

  private object FixedTruncEquiv {
    private val dateTruncMap = Map(
      DateTruncYMD.function.identity -> FixedDateTruncAtYMD,
      DateTruncYM.function.identity -> FixedDateTruncAtYM,
      DateTruncY.function.identity -> FixedDateTruncAtY
    )
    def unapply(f: MonomorphicFunction) = dateTruncMap.get(f.function.identity)
  }

  override def apply(e: Expr): Seq[Expr] =
    e match {
      // some_timestamp_expr < pre-truncated literal timestamp
      // is the same as truncate(some_timestamp_expr) < that same literal
      // Ditto for >=
      // Because a single timestamp literal can be simultaneously
      // "truncated" to different degrees (ymd, ym, y), we'll generate
      // one such candidate for each such potential truncation
      case fc@FunctionCall(LtTime | GteTime, Seq(lhs, rhs@LiteralValue(SoQLFloatingTimestamp(timestamp)))) if timestamp.getMillisOfDay == 0 && !lhs.isInstanceOf[LiteralValue] =>
        val result = Vector.newBuilder[Expr]

        def addYear(expr: Expr): Unit = {
          if(timestamp.getDayOfMonth == 1 && timestamp.getMonthOfYear == 1) {
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncY, Seq(expr))(FuncallPositionInfo.Synthetic), rhs))(fc.position)
          }
        }

        def addMonth(expr: Expr): Unit = {
          if(timestamp.getDayOfMonth == 1) {
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncYM, Seq(expr))(FuncallPositionInfo.Synthetic), rhs))(fc.position)
          }
        }

        lhs match {
          case FunctionCall(DateTruncYMD, Seq(realLhs)) =>
            addMonth(realLhs)
            addYear(realLhs)

          case FunctionCall(DateTruncYM, Seq(realLhs)) =>
            addYear(realLhs)

          case FunctionCall(DateTruncY, _) =>
            // nothing; we can't truncate further

          case _ =>
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncYMD, Seq(lhs))(FuncallPositionInfo.Synthetic), rhs))(fc.position)
            addMonth(lhs)
            addYear(lhs)
        }

        result.result()

      // rewrite "'y-m-d' <= x" to "x >= 'y-m-d'" to reuse the above
      case fc@FunctionCall(LteTime, Seq(lhs@LiteralValue(_), rhs)) =>
        apply(FunctionCall[MT](GteTime, Seq(rhs, lhs))(fc.position))

      // rewrite "'y-m-d' > x" to "x < 'y-m-d'" to reuse the above
      case fc@FunctionCall(GtTime, Seq(lhs@LiteralValue(_), rhs)) =>
        apply(FunctionCall[MT](LtTime, Seq(rhs, lhs))(fc.position))

      // rewrite trunc_X_at_zone(ts, tz) to trunc_X(to_floating_timestamp(ts, tz))
      case fc@FunctionCall(FloatingAtZoneTruncEquiv(floatingTrunc), Seq(ts, tz)) =>
        Seq(
          FunctionCall[MT](
            floatingTrunc,
            Seq(
              FunctionCall[MT](
                ToFloatingTimestamp,
                Seq(ts, tz)
              )(new FuncallPositionInfo(Source.Synthetic, NoPosition))
            )
          )(fc.position)
        )

      // rewrite trunc_X(to_floating_timestamp(ts, tz)) to trunc_X_at_zone(ts, tz)
      case fc@FunctionCall(FixedTruncEquiv(fixedTrunc), Seq(FunctionCall(ToFloatingTimestamp, Seq(ts, tz)))) =>
        Seq(
          FunctionCall[MT](
            fixedTrunc,
            Seq(ts, tz)
          )(fc.position)
        )

      case _ =>
        Nil
    }
}
