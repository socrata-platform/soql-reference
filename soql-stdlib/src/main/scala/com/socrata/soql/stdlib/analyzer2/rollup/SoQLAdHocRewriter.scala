package com.socrata.soql.stdlib.analyzer2.rollup

import org.joda.time.LocalDateTime

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.AdHocRewriter
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions}
import com.socrata.soql.types._

class SoQLAdHocRewriter[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends AdHocRewriter[MT]
{
  private val LtTime = MonomorphicFunction(SoQLFunctions.Lt, Map("a" -> SoQLFloatingTimestamp))
  private val LteTime = MonomorphicFunction(SoQLFunctions.Lte, Map("a" -> SoQLFloatingTimestamp))
  private val GteTime = MonomorphicFunction(SoQLFunctions.Gte, Map("a" -> SoQLFloatingTimestamp))
  private val GtTime = MonomorphicFunction(SoQLFunctions.Gt, Map("a" -> SoQLFloatingTimestamp))

  private val DateTruncYMD = SoQLFunctions.FloatingTimeStampTruncYmd.monomorphic.get
  private val DateTruncYM = SoQLFunctions.FloatingTimeStampTruncYm.monomorphic.get
  private val DateTruncY = SoQLFunctions.FloatingTimeStampTruncY.monomorphic.get

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
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncY, Seq(expr))(FuncallPositionInfo.None), rhs))(fc.position)
          }
        }

        def addMonth(expr: Expr): Unit = {
          if(timestamp.getDayOfMonth == 1) {
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncYM, Seq(expr))(FuncallPositionInfo.None), rhs))(fc.position)
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
            result += FunctionCall(fc.function, Seq(FunctionCall[MT](DateTruncYMD, Seq(lhs))(FuncallPositionInfo.None), rhs))(fc.position)
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

      case _ =>
        Nil
    }
}
