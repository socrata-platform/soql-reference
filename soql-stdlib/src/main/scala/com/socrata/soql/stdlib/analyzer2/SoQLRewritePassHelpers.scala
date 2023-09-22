package com.socrata.soql.stdlib.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLBoolean}
import com.socrata.soql.functions.{SoQLFunctions, SoQLTypeInfo}

class SoQLRewritePassHelpers[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })] extends rewrite.RewritePassHelpers[MT] {
  def isLiteralTrue(e: Expr): Boolean = {
    e match {
      case LiteralValue(SoQLBoolean(true)) => true
      case _ => false
    }
  }

  def isOrderable(e: SoQLType): Boolean = SoQLTypeInfo.isOrdered(e)

  val and = SoQLFunctions.And.monomorphic.get
}
