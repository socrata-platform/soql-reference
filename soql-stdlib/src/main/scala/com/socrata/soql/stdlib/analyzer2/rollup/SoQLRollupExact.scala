package com.socrata.soql.stdlib.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.RollupExact
import com.socrata.soql.types.{SoQLType, SoQLValue}

class SoQLRollupExact[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})](
  stringifier: Stringifier[MT]
) extends RollupExact[MT](new SoQLSemigroupRewriter[MT], new SoQLFunctionSubset[MT], new SoQLFunctionSplitter[MT], new SoQLSplitAnd[MT], stringifier)
