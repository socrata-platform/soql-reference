package com.socrata.soql.stdlib.analyzer2.rollup

import java.math.{BigDecimal => JBigDecimal}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.SemigroupRewriter
import com.socrata.soql.functions.{SoQLFunctions, Function, MonomorphicFunction, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLNumber}

class SoQLSemigroupRewriter[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends SemigroupRewriter[MT]
{
  import SoQLTypeInfo.hasType

  private def countMerger(expr: Expr): Expr =
    FunctionCall[MT](
      MonomorphicFunction(
        SoQLFunctions.Coalesce,
        Map("a" -> SoQLNumber)
      ),
      Seq(
        AggregateFunctionCall[MT](
          MonomorphicFunction(
            SoQLFunctions.Sum,
            Map("a" -> SoQLNumber)
          ),
          Seq(expr),
          false,
          None
        )(FuncallPositionInfo.Synthetic),
        LiteralValue[MT](SoQLNumber(JBigDecimal.ZERO))(AtomicPositionInfo.Synthetic)
      )
    )(FuncallPositionInfo.Synthetic)

  private def simple(f: Function[CT], binding: String): (Function[CT], Expr => Expr) =
    f -> { expr =>
      AggregateFunctionCall[MT](
        MonomorphicFunction(
          f,
          Map(binding -> expr.typ)
        ),
        Seq(expr),
        false,
        None
      )(FuncallPositionInfo.Synthetic)
    }

  private val semigroupMap = Seq[(Function[CT], Expr => Expr)](
    simple(SoQLFunctions.Max, "a"),
    simple(SoQLFunctions.Min, "a"),
    simple(SoQLFunctions.Sum, "a"),
    SoQLFunctions.Count -> countMerger _,
    SoQLFunctions.CountStar -> countMerger _,
  ).map { case (func, rewriter) =>
      func.identity -> rewriter
  }.toMap

  private val semilattices = Seq[Function[CT]](
    SoQLFunctions.Max,
    SoQLFunctions.Min
  ).map(_.identity).toSet

  override def apply(f: MonomorphicFunction): Option[Expr => Expr] =
    semigroupMap.get(f.function.identity)

  override def isSemilattice(f: MonomorphicFunction): Boolean =
    semilattices(f.function.identity)
}
