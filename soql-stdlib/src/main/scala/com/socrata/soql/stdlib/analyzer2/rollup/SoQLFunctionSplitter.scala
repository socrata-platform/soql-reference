package com.socrata.soql.stdlib.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.FunctionSplitter
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLNumber}

class SoQLFunctionSplitter[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends FunctionSplitter[MT]
{
  private val numAvg =
    (
      SoQLFunctions.DivNumNum.monomorphic.get,
      Seq(
        MonomorphicFunction(SoQLFunctions.Sum, Map("a" -> SoQLNumber)),
        MonomorphicFunction(SoQLFunctions.Count, Map("a" -> SoQLNumber))
      )
    )

  override def apply(f: MonomorphicFunction): Option[(MonomorphicFunction, Seq[MonomorphicFunction])] =
    if(f.function.identity == SoQLFunctions.Avg.identity && f.bindings("a") == SoQLNumber) {
      Some(numAvg)
    } else {
      None
    }
}
